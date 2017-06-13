import zipfile
from jaya.lib import aws
import os
from jaya.lib import util
from jaya.config import config
from botocore.exceptions import ClientError
from jaya.lib import util
from localstack.utils.aws import aws_stack
from localstack.mock.apis import lambda_api
from localstack.utils import testutil
from io import BytesIO
import io

LATEST_VERSION_TAG = '$LATEST'

MOCK_ROLE = 'test-iam-role'

MAX_LAMBDA_MEMORY = 1536
MAX_LAMBDA_TIMEOUT = 300
MOCK_CREDENTIALS = {'aws_id': 'rajiv_id', 'aws_key': 'rajiv_key'}


def python_packages_for_env(virtual_env_path):
    python_package_dir = virtual_env_path + '/' + 'lib/python2.7/site-packages'
    return util.get_children(python_package_dir)


def zip_project(lambda_file_path, virtual_env_path, dependency_paths, destination_path):
    python_packages_path = python_packages_for_env(virtual_env_path)
    code_paths = [lambda_file_path] + python_packages_path + dependency_paths
    make_zipfile(destination_path, code_paths)


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
                            zip.write(filename, arcname)
            else:
                filename = source_dir_or_file.split('/')[-1]
                zip.write(source_dir_or_file, filename)


# dependency_paths: list of paths either file or folder other than config and lib
def deploy(environment, lambda_file, lambda_description, virtual_env_path, dependency_paths=None,
           memory=MAX_LAMBDA_MEMORY, timeout=MAX_LAMBDA_TIMEOUT, update=False, lambda_name=None, region_name=None,
           python_package_dir=None,
           is_python3=False,
           mock=False):
    file_name = os.path.basename(lambda_file).split('.')[0]

    if not dependency_paths:
        dependency_paths = []

    if not lambda_name:
        lambda_name = file_name

    if not python_package_dir:
        python_package_dir = virtual_env_path + '/' + 'lib/python2.7/site-packages'
    python_packages = util.get_children(python_package_dir)
    common_folders = [config.lib_folder(), config.config_folder(), lambda_file]

    code_paths = python_packages + common_folders + dependency_paths

    return deploy_lambda(environment,
                         lambda_name,
                         code_paths,
                         'lambda_s3_exec_role',
                         memory,
                         timeout,
                         lambda_description=lambda_description,
                         alias_description='Alias for ' + environment,
                         update=update,
                         handler_name=file_name,
                         region_name=region_name,
                         is_python3=is_python3,
                         mock=mock)


# code_paths is a list of modules and packages that should be packaged in the lambda
def deploy_lambda(environment,
                  lambda_name,
                  code_paths,
                  role_name,
                  memory,
                  timeout,
                  function_version=LATEST_VERSION_TAG,
                  lambda_description='',
                  alias_description='',
                  update=True,
                  zipfile_path=None,
                  handler_name=None,
                  region_name=None,
                  is_python3=False,
                  mock=False
                  ):
    conf = config.get_aws_config(environment)

    if not handler_name:
        handler_name = lambda_name

    if not zipfile_path:
        output_filename = '/tmp/' + lambda_name + '.zip'
    else:
        output_filename = zipfile_path

    print("Saving zipped code at:" + output_filename)

    make_zipfile(output_filename, code_paths)

    # IAM
    handler_name = handler_name + '.handler'
    lambda_func = None
    if not mock:
        lambda_client = aws.client(conf, 'lambda', region_name=region_name)
        iam = aws.client(conf, 'iam')
        role = iam.get_role(RoleName=role_name)['Role']
        lambda_func = aws.create_lambda(conf,
                                        lambda_name,
                                        output_filename,
                                        role,
                                        handler_name,
                                        lambda_description,
                                        lsize=memory,
                                        timeout=timeout,
                                        update=update,
                                        region_name=region_name,
                                        is_python3=is_python3)
    else:
        print('Rajiv: In Mock AWS')
        lambda_client = aws_stack.connect_to_service('lambda')
        s3_client = aws_stack.connect_to_service('s3')
        bucket_name = 'test_bucket_lambda'
        bucket_key = 'test_lambda.zip'
        with open(output_filename, "rb") as file_obj:
            zip_file = file_obj.read()

        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.upload_fileobj(BytesIO(zip_file), bucket_name, bucket_key)
        lambda_func = lambda_client.create_function(
            FunctionName=lambda_name,
            Runtime='python3.6',
            Role='r1',
            Handler=handler_name,
            Code={
                'S3Bucket': bucket_name,
                'S3Key': bucket_key
            }
        )

    # Add Alias

    lambda_client.delete_alias(
        FunctionName=lambda_name,
        Name=environment
    )

    lambda_client.create_alias(
        FunctionName=lambda_name,
        Name=environment,
        FunctionVersion=function_version,
        Description=alias_description
    )

    return lambda_func


def lambda_path(lambda_name, app_folder_name):
    file_name = lambda_name + '.py'
    etl_path = config.project_root() + '/' + app_folder_name
    lambda_file = etl_path + '/lambdas/' + file_name
    return lambda_file


def virtual_env():
    return config.project_root() + '/venv'


def project_paths(dependency_paths):
    return [config.project_root() + '/' + path for path in dependency_paths]


def _deploy_etl_lambda_only(environment, info, conf, region_name):
    path = lambda_path(info['file_name'], 'etl')
    return deploy_lambda_and_integrations(conf, environment, info, path, region_name)


def deploy_lambda_and_integrations(conf, environment, info, path, region_name):
    responses = []
    lambda_name = info.get('lambda_name', info['file_name'])
    deploy(environment,
           path,
           info['description'],
           virtual_env(),
           project_paths(info.get('dependency_paths', [])),
           memory=info.get('memory', MAX_LAMBDA_MEMORY),
           timeout=info.get('timeout', MAX_LAMBDA_TIMEOUT),
           update=True,
           lambda_name=lambda_name,
           region_name=region_name
           )
    if 'source_bucket' in info:
        source_bucket = conf[info['source_bucket']]

        prefix = None
        prefix_key = info.get('source_s3_prefix')

        if prefix_key:
            prefix = conf[prefix_key]

        responses.extend(
            aws.add_s3_notification_for_lambda(conf,
                                               source_bucket,
                                               lambda_name,
                                               environment,
                                               prefix=prefix,
                                               region_name=region_name))

    return responses


def deploy_lambda_package(aws_lambda_function,
                          working_directory='/tmp',
                          serialized_file_name='handler.dill'
                          ):
    serialized_file_path = working_directory + '/' + serialized_file_name
    import os
    os.remove(serialized_file_path)
    environment = 'staging'
    lambda_name = aws_lambda_function.name
    lambda_template_file = config.project_root() + '/core/template/lambda.py'
    util.pickle_and_save_dill(aws_lambda_function.handler, serialized_file_path)
    real_root = os.path.join(config.project_root(), '..')

    python3_package_dir = real_root + '/venv/' + 'lib/python3.6/site-packages'
    deploy(environment,
           lambda_template_file,
           'Jaya Lambda',
           real_root + '/venv',
           aws_lambda_function.dependency_paths + [serialized_file_path],
           memory=128,
           timeout=300,
           update=True,
           lambda_name=lambda_name,
           region_name='us-east-1',
           python_package_dir=python3_package_dir,
           is_python3=True
           )


def deploy_lambda_package_new(environment,
                              aws_lambda_function,
                              working_directory='/tmp',
                              serialized_file_name='handler.dill',
                              mock=False
                              ):
    serialized_file_path = working_directory + '/' + serialized_file_name
    import os.path
    if os.path.isfile(serialized_file_path):
        os.remove(serialized_file_path)
    lambda_name = aws_lambda_function.name
    lambda_template_file = config.project_root() + '/core/template/lambda.py'
    util.pickle_and_save_dill(aws_lambda_function.handler, serialized_file_path)
    real_root = os.path.join(config.project_root(), '..')

    python3_package_dir = real_root + '/venv/' + 'lib/python3.6/site-packages'
    deploy(environment,
           lambda_template_file,
           lambda_name,
           real_root + '/venv',
           aws_lambda_function.dependency_paths + [serialized_file_path],
           memory=aws_lambda_function.memory,
           timeout=aws_lambda_function.timeout,
           update=True,
           lambda_name=lambda_name,
           region_name=aws_lambda_function.region_name,
           python_package_dir=python3_package_dir,
           is_python3=True,
           mock=mock
           )


def deploy_lambda_package_local(aws_lambda_function,
                                working_directory='/tmp',
                                serialized_file_name='handler.dill'
                                ):
    serialized_file_path = working_directory + '/' + serialized_file_name
    import os.path
    if os.path.isfile(serialized_file_path):
        os.remove(serialized_file_path)
    lambda_template_name = 'lambda'
    lambda_template_file = config.project_root() + '/core/template/{0}.py'.format(lambda_template_name)
    util.pickle_and_save_dill(aws_lambda_function.handler, serialized_file_path)

    python_packages = get_virtual_environment_packages(aws_lambda_function.virtual_environment_path)
    common_folders = [config.lib_folder(), config.config_folder()]
    handler_code = [lambda_template_file, serialized_file_path]
    code_paths = python_packages + common_folders + aws_lambda_function.dependency_paths + handler_code
    conn = aws.client(MOCK_CREDENTIALS, 'lambda')
    conn.create_function(
        FunctionName=aws_lambda_function.name,
        Runtime=aws_lambda_function.runtime,
        Role=MOCK_ROLE,
        Handler=lambda_template_name + '.handler',
        Code={
            'ZipFile': make_local_zipfile(code_paths)
        },
        Description=aws_lambda_function.description,
        Timeout=aws_lambda_function.timeout,
        MemorySize=aws_lambda_function.memory,
        Publish=True,
    )

    if aws_lambda_function.alias:
        conn.create_alias(
            FunctionName=aws_lambda_function.name,
            Name=aws_lambda_function.alias,
            FunctionVersion=LATEST_VERSION_TAG
        )


def get_virtual_environment_packages(venv_path):
    lib_path = os.path.join(venv_path, 'lib')

    site_packages_path = os.path.join(lib_path,
                                      util.get_immediate_subdirectories(lib_path)[0],
                                      'site-packages')

    return util.get_children(site_packages_path)


def make_local_zipfile(paths):
    zip_output = io.BytesIO()
    make_zipfile(zip_output, paths)
    zip_output.seek(0)

    return zip_output.read()

    pass


def deploy_pipeline(pipeline):
    pipe = pipeline.pipes[0]


def create_firehose(application, environment, name, redshift_table, prefix=None):
    '''

    :param application: from enum app.application.APPLICATION
    :param environment: e.g. 'staging', 'production'
    :param raw_name: e.g. 'tracker_v2_events'
    :param redshift_table: e.g. 'tracker.v2_events'
    :param prefix: Optional. e.g. 'etl/tracker/v2_events/'
    :return: Response of AWS API
    '''

    conf = config.get_all_config(application, environment)
    try:
        aws.delete_firehose_stream(conf, name)
    except ClientError:
        print('Firehose:{} was not found'.format(name))

    role_arn = aws.resource(conf, 'iam').Role(conf['firehose_role']).arn
    response = aws.create_firehose_stream(conf,
                                          role_arn,
                                          conf['db-user'],
                                          conf['db-passwd'],
                                          name,
                                          conf['db-server'],
                                          conf['db-name'],
                                          redshift_table,
                                          prefix=prefix)

    return response


def delete_firehose(application, environment, name):
    '''
    :param application: from enum app.application.APPLICATION
    :param environment: e.g. 'staging', 'production'
    :param name: e.g. 'tracker_v2_events'
    :return: Response of AWS API
    '''

    conf = config.get_all_config(application, environment)

    try:
        response = aws.delete_firehose_stream(conf, name)
        return response
    except ClientError:
        print('Firehose:{} was not found'.format(name))
