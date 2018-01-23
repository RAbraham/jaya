import zipfile
from jaya.lib import aws, util
import os
from jaya.config import config
from botocore.exceptions import ClientError

# from localstack.utils.aws import aws_stack
# from localstack.mock.apis import lambda_api
# from localstack.utils import testutil
import os.path
import base64

QUALIFIED_HANDLER_NAME = 'lambda.handler'

LATEST_VERSION_TAG = '$LATEST'

MOCK_ROLE = 'test-iam-role'

MAX_LAMBDA_MEMORY = 1536
MAX_LAMBDA_TIMEOUT = 300
JAYA_TMP_DIR = 'jaya-tmp'
MOCK_CREDENTIALS = {'aws_id': 'rajiv_id', 'aws_key': 'rajiv_key'}


def make_zipfile(output_filename, source_dirs_or_files):
    with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zip:
        for source_dir_or_file in source_dirs_or_files:
            relroot = os.path.abspath(os.path.join(source_dir_or_file, os.pardir))
            if os.path.isdir(source_dir_or_file):
                for root, dirs, files in os.walk(source_dir_or_file):
                    for file in files:
                        filename = os.path.join(root, file)
                        if os.path.isfile(filename):  # regular files only
                            arcname = os.path.join(os.path.relpath(root, relroot), file)
                            if '__pycache__' in arcname or '.pyc' in arcname:
                                continue
                            zip.write(filename, arcname)
            else:
                filename = source_dir_or_file.split('/')[-1]
                zip.write(source_dir_or_file, filename)


# def make_zipfile_new(output_filename, source_dirs_or_files):
#     from subprocess import call
#     import os.path
#
#     # call(["zip", "-rX", output_filename, ' '.join(source_dirs_or_files[-2:])])
#
#     a = source_dirs_or_files
#     a = a[:len(a) - 4] + a[-3:]
#     # a = a[:len(a) - 4] + a[-4:]
#     zip_cmd_str = "zip -rX {} {}".format(output_filename, ' '.join(a))
#     call(zip_cmd_str, shell=True)


def tmp_path():
    #     TODO: Return appropriate temp path based on OS Plaform e.g Windows, Mac
    return '/tmp'


def deploy_lambda_package(conf,
                          aws_lambda_function,
                          working_directory=tmp_path()
                          ):
    package_path = make_lambda_zip_package(aws_lambda_function, working_directory)
    deploy_lambda(conf, aws_lambda_function, package_path)


def make_lambda_zip_package(aws_lambda_function, working_directory):
    package_path = os.path.join(working_directory, aws_lambda_function.name + '.zip')
    all_files_and_dir = lambda_files_and_dirs(aws_lambda_function, working_directory) + jaya_files()
    make_zipfile(package_path, all_files_and_dir)
    return package_path


# def has_lambda_changed(environment, zip_package_path, a_lambda):
#     aws_conf = config.get_aws_config(environment)
#     remote_lambda_info = util.filter_keys_except(get_lambda_info(a_lambda, aws_conf),
#                                                  ['ResponseMetadata', 'Version', 'LastModified'])
#     local_lambda_info = make_lambda_conf_from_lambda(aws_conf, a_lambda, zip_package_path)
#
#     pprint('Local Lambda Info')
#     pprint(local_lambda_info)
#     pprint('------------------')
#     pprint(remote_lambda_info)
#
#     return remote_lambda_info != local_lambda_info


# def is_lambda_info_equal(local_lambda_info, remote_lambda_info):
#     pprint('Local Lambda Info')
#     pprint(local_lambda_info)
#     pprint('Remote Lambda Info')
#     pprint(remote_lambda_info)
#     pass
#

# def get_lambda_info(a_lambda, aws_conf):
#     if not a_lambda.alias:
#         return aws.get_lambda_info(aws_conf, a_lambda.name, region_name=a_lambda.region_name)
#     else:
#         return aws.get_lambda_info(aws_conf, a_lambda.name, qualifier=a_lambda.alias, region_name=a_lambda.region_name)
#     pass
#

def make_lambda_conf(base64_encoded_sha256,
                     code_size,
                     description,
                     a_function_arn,
                     function_name,
                     handler_name,
                     memory_size,
                     role,
                     runtime,
                     timeout,
                     tracing_config):
    return {'CodeSha256': base64_encoded_sha256,
            'CodeSize': code_size,
            'Description': description,
            'FunctionArn': a_function_arn,
            'FunctionName': function_name,
            'Handler': handler_name,
            'MemorySize': memory_size,
            'Role': role,
            'Runtime': runtime,
            'Timeout': timeout,
            'TracingConfig': tracing_config,
            # 'TracingConfig': {'Mode': 'PassThrough'},
            # 'Version': version
            }


def make_lambda_conf_from_lambda(conf, a_lambda, zip_package_path):
    # TODO: We should not use role parameter eventually
    sha256 = util.sha256_of_zipfile(zip_package_path)
    account_id = aws.get_account_id(conf)
    role_arn = make_role_arn(account_id, a_lambda.role_name)
    handler_name = QUALIFIED_HANDLER_NAME
    return make_lambda_conf(base64.b64encode(sha256).decode(),
                            os.path.getsize(zip_package_path),
                            a_lambda.description,
                            function_arn(a_lambda.region_name,
                                         account_id,
                                         a_lambda.name,
                                         a_lambda.alias
                                         ),
                            a_lambda.name,
                            handler_name,
                            a_lambda.memory,
                            role_arn,
                            a_lambda.runtime,
                            a_lambda.timeout,
                            a_lambda.tracing_config)


def make_role_arn(account_id, role_name):
    return 'arn:aws:iam::{account_id}:role/{role_name}'.format(account_id=account_id, role_name=role_name)


def function_arn(region_name, account_id, qualified_lambda_name, alias=None):
    main_arn = 'arn:aws:lambda:{region_name}:{account_id}:function:{qualified_lambda_name}'.format(
        region_name=region_name,
        account_id=account_id,
        qualified_lambda_name=qualified_lambda_name)
    if alias:
        return main_arn + ":" + alias
    else:
        return main_arn


def sns_arn(region_name, account_id, sns_name):
    return "arn:aws:sns:{}:{}:{}".format(region_name, account_id, sns_name)


def deploy_lambda(conf, a_lambda, zip_package_path):
    lambda_client = aws.client(conf, 'lambda', region_name=a_lambda.region_name)
    iam = aws.client(conf, 'iam')
    role = iam.get_role(RoleName=a_lambda.role_name)['Role']
    dlq_arn = None
    if a_lambda.dead_letter_queue:
        service = a_lambda.dead_letter_queue['service']
        if service == 'SNS':
            account_id = aws.get_account_id(conf)
            sns_name = a_lambda.dead_letter_queue['name']
            dlq_arn = sns_arn(a_lambda.region_name, account_id, sns_name)
        elif service == 'SQS':
            raise ValueError('SQS as Dead Letter Queue not supported yet')

    lambda_func = aws.create_lambda_simple(conf,
                                           a_lambda.name,
                                           zip_package_path,
                                           role,
                                           QUALIFIED_HANDLER_NAME,
                                           a_lambda.description,
                                           a_lambda.runtime,
                                           JAYA_TMP_DIR,
                                           a_lambda.name,
                                           lsize=a_lambda.memory,
                                           timeout=a_lambda.timeout,
                                           update=True,
                                           region_name=a_lambda.region_name,
                                           environment_variables=a_lambda.environment_variables,
                                           dead_letter_queue_arn=dlq_arn)

    if a_lambda.alias:
        lambda_client.delete_alias(
            FunctionName=a_lambda.name,
            Name=a_lambda.alias
        )
        lambda_client.create_alias(
            FunctionName=a_lambda.name,
            Name=a_lambda.alias,
            FunctionVersion=LATEST_VERSION_TAG,
            Description=''
        )
    return lambda_func


def jaya_files():
    lambda_starter_file = os.path.join(config.project_root(), 'core', 'template', 'lambda.py')
    return [lambda_starter_file]


def lambda_files_and_dirs(a_lambda, working_directory):
    return get_virtual_environment_packages(a_lambda.virtual_environment_path) \
           + a_lambda.dependency_paths \
           + [serialized_file(a_lambda, working_directory)]


def serialized_file(a_lambda, working_directory, serialized_file_name='handler.dill'):
    serialized_file_path = working_directory + '/' + serialized_file_name
    if os.path.isfile(serialized_file_path):
        os.remove(serialized_file_path)
    util.pickle_and_save_dill(a_lambda.handler, serialized_file_path)
    return serialized_file_path
    pass


# def deploy_lambda_package_local(aws_lambda_function,
#                                 working_directory='/tmp',
#                                 serialized_file_name='handler.dill'
#                                 ):
#     serialized_file_path = working_directory + '/' + serialized_file_name
#     import os.path
#     if os.path.isfile(serialized_file_path):
#         os.remove(serialized_file_path)
#     lambda_template_name = 'lambda'
#     lambda_template_file = config.project_root() + '/core/template/{0}.py'.format(lambda_template_name)
#     util.pickle_and_save_dill(aws_lambda_function.handler, serialized_file_path)
#
#     python_packages = get_virtual_environment_packages(aws_lambda_function.virtual_environment_path)
#     common_folders = [config.lib_folder(), config.config_folder()]
#     handler_code = [lambda_template_file, serialized_file_path]
#     # code_paths = python_packages + common_folders + aws_lambda_function.dependency_paths + handler_code
#     code_paths = handler_code
#
#     conn = aws.client(MOCK_CREDENTIALS, 'lambda')
#     zip_path = '/tmp/' + 'moto-rajiv' + '.zip'
#     make_local_zipfile(code_paths)
#     # with zipfile.ZipFile(zip_path, "r") as f:
#     #     for info in f.infolist():
#     #         print(info.filename, info.date_time, info.file_size, info.compress_size)
#     with zipfile.ZipFile(zip_path, "r", zipfile.ZIP_DEFLATED) as f:
#         pprint(f.namelist())
#
#     with open(zip_path, 'rb') as ziper:
#         conn.create_function(
#             FunctionName=aws_lambda_function.name,
#             Runtime=aws_lambda_function.runtime,
#             Role=MOCK_ROLE,
#             Handler=lambda_template_name + '.handler',
#             Code={
#                 'ZipFile': ziper.read()
#             },
#             Description=aws_lambda_function.description,
#             Timeout=aws_lambda_function.timeout,
#             MemorySize=aws_lambda_function.memory,
#             Publish=True,
#         )
#
#     if aws_lambda_function.alias:
#         conn.create_alias(
#             FunctionName=aws_lambda_function.name,
#             Name=aws_lambda_function.alias,
#             FunctionVersion=LATEST_VERSION_TAG
#         )


def get_virtual_environment_packages(venv_path: str):
    lib_path = os.path.join(venv_path, 'lib')

    site_packages_path = os.path.join(lib_path,
                                      util.get_immediate_subdirectories(lib_path)[0],
                                      'site-packages')

    return util.get_children(site_packages_path)

# def make_local_zipfile(paths):
#     zip_output = io.BytesIO()
#     make_zipfile(zip_output, paths)
#     zip_output.seek(0)
#
#     return zip_output.read()
#
#     pass

# def make_local_zipfile(paths):
#     zip_output = '/tmp/' + 'moto-rajiv' + '.zip'
#     make_zipfile(zip_output, paths)
#     return zip_output
