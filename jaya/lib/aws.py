import boto3
import json
from jaya.lib import util

# from localstack.utils.aws import aws_stack

FUNCTION_ARN = 'FunctionArn'
DEFAULT_REGION = 'us-east-1'
DEFAULT_COPY_OPTIONS = "format as json 'auto' gzip timeformat 'auto' truncatecolumns"
LAMBDA_ARN = 'LambdaFunctionArn'


def resource(conf, resource_name, region_name=DEFAULT_REGION):
    session = boto3.session.Session(aws_access_key_id=conf['aws_id'],
                                    aws_secret_access_key=conf['aws_key'],
                                    region_name=region_name)
    return session.resource(resource_name)


def client(conf, service_name, region_name=DEFAULT_REGION):
    return boto3.client(
        service_name,
        aws_access_key_id=conf['aws_id'],
        aws_secret_access_key=conf['aws_key'],
        region_name=region_name
    )


def move_s3_object(s3_resource, source_bucket, source_key, destination_bucket, destination_key):
    o = s3_resource.Object(destination_bucket, destination_key)
    o.copy_from(CopySource=source_bucket + '/' + source_key)
    s3_resource.Object(source_bucket, source_key).delete()


def upload_to_s3(config, path, bucket, key):
    s3 = resource(config, 's3')
    with open(path, 'rb') as data:
        s3.Bucket(bucket).put_object(Key=key, Body=data)


def get_keys(conf, bucket_name, prefix):
    s3 = resource(conf, 's3')
    bucket = s3.Bucket(bucket_name)
    return [obj.key for obj in bucket.objects.filter(Prefix=prefix)]


def upload_stream_to_s3(config, stream, bucket, key):
    s3 = resource(config, 's3')
    s3.Object(bucket, key).put(Body=stream)


# def download_from_s3(environment, s3_url, destination_path):
#     conf = config.get_aws_config(environment)
#     s3 = resource(conf, 's3')
#     bucket, key = split_s3_path(s3_url)
#     return s3.meta.client.download_file(bucket, key, destination_path)


def split_s3_path(s3_path):
    _, path = s3_path.split(":", 1)
    path = path.lstrip("/")
    bucket, key = path.split("/", 1)
    return bucket, key


def create_s3_bucket(conf, bucket_name, region_name):
    s3 = resource(conf, 's3', region_name=region_name)
    s3.create_bucket(Bucket=bucket_name)


def s3_delete_object(conf, bucket, key):
    s3 = resource(conf, 's3')
    s3.Bucket(bucket).delete_objects(Delete={
        'Objects': [
            {
                'Key': key,
            },
        ],
    })


def s3_delete_objects(config, bucket):
    s3 = resource(config, 's3')
    return s3.Bucket(bucket).objects.delete()


def s3_path(bucket, key):
    return "s3://" + bucket + "/" + key


# prefix should end with a slash. For e.g. 'images/'
# name: 'prefix' or 'suffix'
def s3_filter(name, value):
    assert name in ['prefix', 'suffix'], 'Invalid filter value for S3: {}'.format(name)
    return {'Filter': {
        'Key': {
            'FilterRules': [
                {'Name': name,
                 'Value': value
                 },
            ]
        }
    }
    }


def create_lambda_simple(config,
                         name,
                         zfile,
                         role,
                         handler_name,
                         description,
                         runtime,
                         bucket,
                         key,
                         lsize=512,
                         timeout=10,
                         update=True,
                         region_name=None,
                         environment_variables=None):
    create_s3_bucket(config, bucket, region_name)
    upload_to_s3(config, zfile, bucket, key)
    l = client(config, 'lambda', region_name)

    mandatory_params = dict(FunctionName=name,
                            Runtime=runtime,
                            Role=role['Arn'],
                            Handler=handler_name,
                            Code={'S3Bucket': bucket,
                                  'S3Key': key}, )
    if environment_variables:
        environment_var_dict = {'Variables': environment_variables}
    else:
        environment_var_dict = None

    optional_params = util.optional(
        Description=description,
        Timeout=timeout,
        MemorySize=lsize,
        Publish=True,
        Environment=environment_var_dict)

    param_dict = util.merge_dicts(optional_params, mandatory_params)

    """ Create, or update if exists, lambda function """

    with open(zfile, 'rb') as zipfile:
        funcs = l.list_functions()['Functions']
        if name in [f['FunctionName'] for f in funcs]:
            if update:
                print('Updating %s lambda function code' % (name))
                return l.update_function_code(FunctionName=name, S3Bucket=bucket, S3Key=key)
            else:
                print('Lambda function %s exists' % (name))
                for f in funcs:
                    if f['FunctionName'] == name:
                        lfunc = f
        else:
            print('Creating %s lambda function' % (name))
            lfunc = l.create_function(**param_dict)
        lfunc['Role'] = role
        return lfunc

# Template for Lambda Create Function
# client.create_function(
#     FunctionName='string',
#     Runtime='nodejs'|'nodejs4.3'|'nodejs6.10'|'java8'|'python2.7'|'python3.6'|'dotnetcore1.0'|'nodejs4.3-edge',
#     Role='string',
#     Handler='string',
#     Code={
#         'ZipFile': b'bytes',
#         'S3Bucket': 'string',
#         'S3Key': 'string',
#         'S3ObjectVersion': 'string'
#     },
#     Description='string',
#     Timeout=123,
#     MemorySize=123,
#     Publish=True|False,
#     VpcConfig={
#         'SubnetIds': [
#             'string',
#         ],
#         'SecurityGroupIds': [
#             'string',
#         ]
#     },
#     DeadLetterConfig={
#         'TargetArn': 'string'
#     },
#     Environment={
#         'Variables': {
#             'string': 'string'
#         }
#     },
#     KMSKeyArn='string',
#     TracingConfig={
#         'Mode': 'Active'|'PassThrough'
#     },
#     Tags={
#         'string': 'string'
#     }
# )
def delete_lambda(config, lambda_name, region_name):
    l = client(config, 'lambda', region_name)
    l.delete_function(
        FunctionName=lambda_name,
    )


def create_role(config, name, policies=None):
    """ Create a role with an optional inline policy """

    iam = client(config, 'iam')

    policydoc = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"Service": ["lambda.amazonaws.com"]}, "Action": ["sts:AssumeRole"]},
        ]
    }
    roles = [r['RoleName'] for r in iam.list_roles()['Roles']]
    if name in roles:
        print('IAM role %s exists' % (name))
        role = iam.get_role(RoleName=name)['Role']
    else:
        print('Creating IAM role %s' % (name))
        role = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(policydoc))['Role']

    # attach managed policy
    if policies is not None:
        for p in policies:
            iam.attach_role_policy(RoleName=role['RoleName'], PolicyArn=p)
    return role


def create_firehose_stream(conf,
                           role_arn,
                           redshift_user_name,
                           redshift_password,
                           delivery_stream_name,
                           redshift_server,
                           redshift_database_name,
                           redshift_table_name,
                           copy_options=DEFAULT_COPY_OPTIONS,
                           holding_bucket=None,
                           prefix=None,
                           buffering_size_mb=128,
                           buffering_interval_seconds=900,
                           log_group=None,
                           log_stream='RedshiftDelivery',
                           ):
    c = client(conf, 'firehose')

    s3_params = {
        'RoleARN': role_arn,
        'BucketARN': 'arn:aws:s3:::' + holding_bucket,
        'BufferingHints': {
            'SizeInMBs': buffering_size_mb,
            'IntervalInSeconds': buffering_interval_seconds
        },
        'CompressionFormat': 'GZIP',
        'EncryptionConfiguration': {
            'NoEncryptionConfig': 'NoEncryption'
        }
    }

    if prefix:
        s3_params = util.merge_dicts(s3_params, {'Prefix': prefix})

    jdbc_url = 'jdbc:redshift://' + redshift_server + ':' + conf['db-port'] + '/' + conf['db-name']
    redshift_config = {'RoleARN': role_arn,
                       'ClusterJDBCURL': jdbc_url,
                       'CopyCommand': {'DataTableName': redshift_table_name,
                                       'CopyOptions': copy_options},
                       'Username': redshift_user_name,
                       'Password': redshift_password,
                       'S3Configuration': s3_params}

    cloudwatch_params = {}
    if log_group:
        cloudwatch_params = {
            'CloudWatchLoggingOptions': {
                'Enabled': True,
                'LogGroupName': log_group,
                'LogStreamName': log_stream
            }
        }

    updated_redshift_config = util.merge_dicts(redshift_config, cloudwatch_params)
    response = c.create_delivery_stream(
        DeliveryStreamName=delivery_stream_name,
        RedshiftDestinationConfiguration=updated_redshift_config
    )

    return response


def add_s3_notification_for_lambda(conf, bucket_name, lambda_name, trigger, qualifier=None, prefix=None, suffix=None,
                                   region_name=None):
    assert conf is not None, 'Configuration Dict Required, wtf'
    assert bucket_name is not None, 'Bucket Name Mandatory, wtf'
    assert lambda_name is not None, 'Lambda Name Mandatory, wtf'

    responses = []

    add_responses = lambda_add_s3_permission(conf, lambda_name, bucket_name, prefix, qualifier, region_name)
    responses.extend(add_responses)

    notification_response = s3_add_lambda_notification(conf,
                                                       bucket_name,
                                                       qualifier,
                                                       lambda_name,
                                                       region_name,
                                                       trigger,
                                                       prefix,
                                                       suffix)

    responses.append(notification_response)
    return responses


def s3_add_lambda_notification(conf,
                               bucket_name,
                               qualifier,
                               lambda_name,
                               region_name,
                               trigger,
                               prefix=None,
                               suffix=None,
                               ):
    notification_response = None
    add = util.merge_dicts
    s3 = resource(conf, 's3', region_name=region_name)
    bucket_notification = s3.BucketNotification(bucket_name)
    lambda_arn = get_lambda_info(conf, lambda_name, qualifier, region_name)[FUNCTION_ARN]

    lambda_configuration = {
        LAMBDA_ARN: lambda_arn,
        'Events': [
            trigger
        ],

    }
    if prefix:
        lambda_configuration = add(lambda_configuration, s3_filter('prefix', prefix))
    # TODO: Duplication code for prefix and suffix
    if suffix:
        lambda_configuration = add(lambda_configuration, s3_filter('suffix', suffix))
    existing_configurations = get_lambda_notifications(bucket_notification)
    new_configurations = make_s3_notifications_for_upsertion(existing_configurations, lambda_configuration)

    notification_response = bucket_notification.put(
        NotificationConfiguration={'LambdaFunctionConfigurations': new_configurations})

    return notification_response


def make_s3_notifications_for_upsertion(existing_notifications, new_notification):
    other_notifications = [n for n in existing_notifications if n[LAMBDA_ARN] != new_notification[LAMBDA_ARN]]
    return other_notifications + [new_notification]


def get_lambda_notifications(bucket_notification_client):
    bucket_notification_client.load()
    result = bucket_notification_client.lambda_function_configurations

    if not result:
        return []
    else:
        return result


def lambda_add_s3_permission(conf, lambda_name, bucket_name, prefix, qualifier, region_name):
    add = util.merge_dicts
    responses = []
    lambda_client = client(conf, 'lambda', region_name=region_name)
    # Giving S3 permission to invoke the lambda
    action_name = 'InvokeFunction'
    action = 'lambda:' + action_name
    statement_id = action_name + '_' + bucket_name
    permission_main_kw = add({'StatementId': statement_id},
                             lambda_parameters(lambda_name, qualifier))
    # First, remove existing permissions if any
    # TODO: What if the qualifier does not exist
    try:
        remove_response = lambda_client.remove_permission(**permission_main_kw)
        responses.append(remove_response)
    except:
        print('Permission:{} for lambda:{} does not exist'.format(statement_id, lambda_name))
        # The statement did not exist. So let's move on. I don't know of a way of checking if permissions exist
        # for a lambda so this is the workaround
        pass

    # Add permission response,
    add_permission_others_params = {'Action': action,
                                    'Principal': 's3.amazonaws.com',
                                    'SourceArn': 'arn:aws:s3:::' + bucket_name,
                                    'SourceAccount': get_account_id(conf)
                                    }
    add_permission_params = add(permission_main_kw, add_permission_others_params)
    add_response = lambda_client.add_permission(**add_permission_params)
    responses.append(add_response)
    return responses


def get_lambda_info(conf, function_name, qualifier=None, region_name=None, lambda_client=None):
    if not lambda_client:
        lambda_client = client(conf, 'lambda', region_name=region_name)

    kw = lambda_parameters(function_name, qualifier)
    try:
        response = lambda_client.get_function_configuration(**kw)
    except lambda_client.exceptions.ResourceNotFoundException as ex:
        response = {}

    return response


def lambda_parameters(function_name, qualifier):
    mandatory_inp = {'FunctionName': function_name}
    optional_inp = optional(Qualifier=qualifier)
    kw = util.merge_dicts(mandatory_inp, optional_inp)
    return kw


def get_account_id(conf):
    return client(conf, 'iam').get_user()['User']['Arn'].split(':')[4]


def optional(**kwargs):
    return {key: kwargs[key] for key in kwargs.keys() if kwargs[key]}


def copy_from_s3_to_s3(conf, source_bucket, source_key, destination_bucket, destination_key):
    s3 = resource(conf, 's3')
    o = s3.Object(destination_bucket, destination_key)
    o.copy_from(CopySource=source_bucket + '/' + source_key)
