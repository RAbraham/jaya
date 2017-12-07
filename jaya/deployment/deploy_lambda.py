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


# def python_packages_for_env(virtual_env_path):
#     python_package_dir = virtual_env_path + '/' + 'lib/python2.7/site-packages'
#     return util.get_children(python_package_dir)


# def zip_project(lambda_file_path, virtual_env_path, dependency_paths, destination_path):
#     python_packages_path = python_packages_for_env(virtual_env_path)
#     code_paths = [lambda_file_path] + python_packages_path + dependency_paths
#     # make_zipfile(destination_path, code_paths)
#     make_zipfile_new(destination_path, code_paths)


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


# dependency_paths: list of paths either file or folder other than config and lib
# def deploy(environment, lambda_file, lambda_description, virtual_env_path, dependency_paths=None,
#            memory=MAX_LAMBDA_MEMORY, timeout=MAX_LAMBDA_TIMEOUT, update=False, lambda_name=None, region_name=None,
#            python_package_dir=None,
#            is_python3=False,
#            mock=False):
#     file_name = os.path.basename(lambda_file).split('.')[0]
#
#     if not dependency_paths:
#         dependency_paths = []
#
#     if not lambda_name:
#         lambda_name = file_name
#
#     if not python_package_dir:
#         python_package_dir = virtual_env_path + '/' + 'lib/python2.7/site-packages'
#     python_packages = util.get_children(python_package_dir)
#     common_folders = [config.lib_folder(), config.config_folder(), lambda_file]
#
#     code_paths = python_packages + common_folders + dependency_paths
#
#     return deploy_lambda(environment,
#                          lambda_name,
#                          code_paths,
#                          'lambda_s3_exec_role',
#                          memory,
#                          timeout,
#                          lambda_description=lambda_description,
#                          alias_description='Alias for ' + environment,
#                          update=update,
#                          handler_name=file_name,
#                          region_name=region_name,
#                          is_python3=is_python3,
#                          mock=mock)


def tmp_path():
    #     TODO: Return appropriate temp path based on OS Plaform e.g Windows, Mac
    return '/tmp'


# code_paths is a list of modules and packages that should be packaged in the lambda
# def deploy_lambda(environment,
#                   lambda_name,
#                   code_paths,
#                   role_name,
#                   memory,
#                   timeout,
#                   function_version=LATEST_VERSION_TAG,
#                   lambda_description='',
#                   alias_description='',
#                   update=True,
#                   zipfile_path=None,
#                   handler_name=None,
#                   region_name=None,
#                   is_python3=False,
#                   mock=False
#                   ):
#     conf = config.get_aws_config(environment)
#
#     if not handler_name:
#         handler_name = lambda_name
#
#     if not zipfile_path:
#         output_filename = '/tmp/' + lambda_name + '.zip'
#     else:
#         output_filename = zipfile_path
#
#     print("Saving zipped code at:" + output_filename)
#
#     make_zipfile(output_filename, code_paths)
#
#     # IAM
#     handler_name = handler_name + '.handler'
#     lambda_func = None
#     if not mock:
#         lambda_client = aws.client(conf, 'lambda', region_name=region_name)
#         iam = aws.client(conf, 'iam')
#         role = iam.get_role(RoleName=role_name)['Role']
#         lambda_func = aws.create_lambda(conf,
#                                         lambda_name,
#                                         output_filename,
#                                         role,
#                                         handler_name,
#                                         lambda_description,
#                                         lsize=memory,
#                                         timeout=timeout,
#                                         update=update,
#                                         region_name=region_name,
#                                         is_python3=is_python3)
#     else:
#         # print('Rajiv: In Mock AWS')
#         # lambda_client = aws_stack.connect_to_service('lambda')
#         # s3_client = aws_stack.connect_to_service('s3')
#         # bucket_name = 'test_bucket_lambda'
#         # bucket_key = 'test_lambda.zip'
#         # with open(output_filename, "rb") as file_obj:
#         #     zip_file = file_obj.read()
#         #
#         # s3_client.create_bucket(Bucket=bucket_name)
#         # s3_client.upload_fileobj(BytesIO(zip_file), bucket_name, bucket_key)
#         # lambda_func = lambda_client.create_function(
#         #     FunctionName=lambda_name,
#         #     Runtime='python3.6',
#         #     Role='r1',
#         #     Handler=handler_name,
#         #     Code={
#         #         'S3Bucket': bucket_name,
#         #         'S3Key': bucket_key
#         #     }
#         # )
#         pass
#     # Add Alias
#
#     lambda_client.delete_alias(
#         FunctionName=lambda_name,
#         Name=environment
#     )
#
#     lambda_client.create_alias(
#         FunctionName=lambda_name,
#         Name=environment,
#         FunctionVersion=function_version,
#         Description=alias_description
#     )
#
#     return lambda_func


# def lambda_path(lambda_name, app_folder_name):
#     file_name = lambda_name + '.py'
#     etl_path = config.project_root() + '/' + app_folder_name
#     lambda_file = etl_path + '/lambdas/' + file_name
#     return lambda_file


# def virtual_env():
#     return config.project_root() + '/venv'
#

# def project_paths(dependency_paths):
#     return [config.project_root() + '/' + path for path in dependency_paths]


def deploy_lambda_package_new_simple(conf,
                                     aws_lambda_function,
                                     working_directory=tmp_path()
                                     ):
    package_path = make_lambda_zip_package(aws_lambda_function, working_directory)
    deploy_lambda_simple(conf, aws_lambda_function, package_path)


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
            # 'FunctionArn': 'arn:aws:lambda:us-east-1:027995586716:function:dmp-trial_Echo1:development',
            'FunctionName': function_name,
            'Handler': handler_name,
            'MemorySize': memory_size,
            'Role': role,
            # 'Role': 'arn:aws:iam::027995586716:role/lambda_s3_exec_role',
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
    'arn:aws:lambda:us-east-1:027995586716:function:dmp-trial_Echo1:development'
    main_arn = 'arn:aws:lambda:{region_name}:{account_id}:function:{qualified_lambda_name}'.format(
        region_name=region_name,
        account_id=account_id,
        qualified_lambda_name=qualified_lambda_name)
    if alias:
        return main_arn + ":" + alias
    else:
        return main_arn


def deploy_lambda_simple(conf, a_lambda, zip_package_path):
    lambda_client = aws.client(conf, 'lambda', region_name=a_lambda.region_name)
    iam = aws.client(conf, 'iam')
    role = iam.get_role(RoleName=a_lambda.role_name)['Role']

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
                                           region_name=a_lambda.region_name)

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
