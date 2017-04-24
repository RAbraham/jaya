import zipfile
from jaya.lib import aws
import os
from jaya.lib import util
from jaya.config import config
from botocore.exceptions import ClientError

MAX_LAMBDA_MEMORY = 1536
MAX_LAMBDA_TIMEOUT = 300


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
           is_python3=False):
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
                         is_python3=is_python3)


# code_paths is a list of modules and packages that should be packaged in the lambda
def deploy_lambda(environment,
                  lambda_name,
                  code_paths,
                  role_name,
                  memory,
                  timeout,
                  function_version='$LATEST',
                  lambda_description='',
                  alias_description='',
                  update=True,
                  zipfile_path=None,
                  handler_name=None,
                  region_name=None,
                  is_python3=False
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
    iam = aws.client(conf, 'iam')
    role = iam.get_role(RoleName=role_name)['Role']

    lambda_func = aws.create_lambda(conf,
                                    lambda_name,
                                    output_filename,
                                    role,
                                    handler_name + '.handler',
                                    lambda_description,
                                    lsize=memory,
                                    timeout=timeout,
                                    update=update,
                                    region_name=region_name,
                                    is_python3=is_python3)

    # Add Alias
    lambda_client = aws.client(conf, 'lambda', region_name=region_name)
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


if __name__ == '__main__':
    # deploy_do_etl_dev_lambda_only()
    # deploy_v3_firehoses('staging')
    # deploy_do_etl_staging_lambda_only()
    # deploy_do_etl_production_lambda_only()
    # deploy_etl_lambda('staging', 'copy_to_etl_inbound_staging')
    # deploy_etl_lambda(APPLICATION.sports.name, 'staging', 'move_to_etl_in_process_staging')

    # deploy_etl_firehose('esports', 'staging', 'v2_events')

    pass
