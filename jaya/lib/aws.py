import boto3
import json
from jaya.lib import util
from typing import Dict

# from localstack.utils.aws import aws_stack
SUBSCRIBE = "Subscribe"
RECEIVE = "Receive"

FUNCTION_ARN = 'FunctionArn'
DEFAULT_COPY_OPTIONS = "format as json 'auto' gzip timeformat 'auto' truncatecolumns"
LAMBDA_ARN = 'LambdaFunctionArn'


# def resource(conf, resource_name):
#     session = boto3.session.Session(aws_access_key_id=conf.get('aws_id'),
#                                     aws_secret_access_key=conf.get('aws_key'),
#                                     region_name=conf.get('region_name'))
#     return session.resource(resource_name)


def resource(conf, resource_name):
    session = boto3.session.Session(aws_access_key_id=conf.get('aws_id'),
                                    aws_secret_access_key=conf.get('aws_key'),
                                    region_name=conf.get('region_name'))
    return session.resource(resource_name)


def client(conf, service_name):
    return boto3.client(
        service_name,
        aws_access_key_id=conf.get('aws_id'),
        aws_secret_access_key=conf.get('aws_key'),
        region_name=conf.get('region_name')
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
    # TODO: Why pass region name this way? Search for all such ones
    conf['region_name'] = region_name
    s3 = resource(conf, 's3')
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
                         environment_variables=None,
                         dead_letter_queue_arn=None):
    create_s3_bucket(config, bucket, region_name)
    upload_to_s3(config, zfile, bucket, key)
    config['region_name'] = region_name
    l = client(config, 'lambda')

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

    if dead_letter_queue_arn:
        dead_letter_queue_dict = dict(TargetArn=dead_letter_queue_arn)
    else:
        dead_letter_queue_dict = None

    optional_params = util.optional(
        Description=description,
        Timeout=timeout,
        MemorySize=lsize,
        Publish=True,
        Environment=environment_var_dict,
        DeadLetterConfig=dead_letter_queue_dict)

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


def lambda_add_sns_permission(conf, account_id, lambda_name, sns_name, qualifier, region_name):
    add = util.merge_dicts
    responses = []
    conf['region_name'] = region_name

    lambda_client = client(conf, 'lambda')
    # Giving SNS permission to invoke the lambda
    action_name = 'InvokeFunction'
    action = 'lambda:' + action_name
    # statement_id = action_name + '_' + sns_name
    statement_id = 'lambda-03a99f95-f490-4b9c-8bf8-20ee85fb2bff'
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
                                    'Principal': 'sns.amazonaws.com',
                                    'SourceArn': sns_arn(region_name, account_id, sns_name)
                                    }
    add_permission_params = add(permission_main_kw, add_permission_others_params)
    add_response = lambda_client.add_permission(**add_permission_params)
    responses.append(add_response)
    from pprint import pprint
    pprint('Add Permission Input')
    pprint(add_permission_params)
    pprint('SNS Permission Response')
    pprint(add_response)
    return responses


def lambda_add_cloudwatch_event_permission(conf, account_id, lambda_name, event_name, qualifier, region_name):
    add = util.merge_dicts
    responses = []
    conf['region_name'] = region_name

    lambda_client = client(conf, 'lambda')
    # Giving SNS permission to invoke the lambda
    action_name = 'InvokeFunction'
    action = 'lambda:' + action_name
    statement_id = action_name + '_' + event_name
    # statement_id = 'lambda-03a99f95-f490-4b9c-8bf8-20ee85fb2bff'
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
                                    'Principal': 'events.amazonaws.com',
                                    'SourceArn': event_arn(region_name, account_id, event_name)
                                    }
    add_permission_params = add(permission_main_kw, add_permission_others_params)
    add_response = lambda_client.add_permission(**add_permission_params)
    responses.append(add_response)
    from pprint import pprint
    pprint('Add Permission Input')
    pprint(add_permission_params)
    pprint('CloudWatch Event Permission Response')
    pprint(add_response)
    return responses
    pass


# def add_sns_notification_for_lambda(conf: Dict, sns_name: str, lambda_name: str, qualifier: str, region_name: str):
#     sns_client = client(conf, 'sns')
#     account_id = get_account_id(conf)
#
#     lambda_add_sns_permission(conf, account_id, lambda_name, sns_name, qualifier, region_name)
#
#     response = sns_client.subscribe(
#         TopicArn=sns_arn(region_name, account_id, sns_name),
#         Protocol='lambda',
#         Endpoint=function_arn(region_name, account_id, lambda_name, qualifier)
#     )
#
#     from pprint import pprint
#     pprint('SNS Subscriber Response')
#     pprint(response)
#     return response
#

# def add_sns_notification_for_lambda(conf: Dict, sns_name: str, lambda_name: str, qualifier: str, region_name: str):
#     sns_client = client(conf, 'sns')
#     account_id = get_account_id(conf)
#
#     lambda_add_sns_permission(conf, account_id, lambda_name, sns_name, qualifier, region_name)
#
#     response = sns_client.subscribe(
#         TopicArn=sns_arn(region_name, account_id, sns_name),
#         Protocol='lambda',
#         Endpoint=function_arn(region_name, account_id, lambda_name, qualifier)
#     )
#
#     from pprint import pprint
#     pprint('SNS Subscriber Response')
#     pprint(response)
#     return response

def add_sns_notification_for_lambda(conf: Dict, sns_name: str, lambda_name: str, qualifier: str, region_name: str):
    sns_client = client(conf, 'sns')
    account_id = get_account_id(conf)

    lambda_add_sns_permission(conf, account_id, lambda_name, sns_name, qualifier, region_name)

    response = sns_client.subscribe(
        TopicArn=sns_arn(region_name, account_id, sns_name),
        Protocol='lambda',
        Endpoint=function_arn(region_name, account_id, lambda_name, qualifier)
    )

    from pprint import pprint
    pprint('SNS Subscriber Response')
    pprint(response)
    return response


def add_cloudwatch_event_notification_for_lambda(conf: Dict, event_name: str, lambda_name: str, qualifier: str,
                                                 region_name: str):
    event_client = client(conf, 'events')
    account_id = get_account_id(conf)

    lambda_add_cloudwatch_event_permission(conf, account_id, lambda_name, event_name, qualifier, region_name)

    # response = sns_client.subscribe(
    #     TopicArn=sns_arn(region_name, account_id, sns_name),
    #     Protocol='lambda',
    #     Endpoint=function_arn(region_name, account_id, lambda_name, qualifier)
    # )
    #
    # from pprint import pprint
    # pprint('SNS Subscriber Response')
    # pprint(response)
    # return response


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
    # s3 = resource(conf, 's3', region_name=region_name)
    s3 = resource(conf, 's3')
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
    conf['region_name'] = region_name
    lambda_client = client(conf, 'lambda')
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
        lambda_client = client(conf, 'lambda')

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


def get_sns_topic(conf, name: str, region_name: str):
    try:
        sns_client = client(conf, 'sns')
        response = sns_client.get_topic_attributes(
            TopicArn=sns_arn(region_name, get_account_id(conf), name)
        )
    except sns_client.exceptions.NotFoundException:
        response = None

    return response


def create_cloudwatch_target(rule_name, target):
    pass


def create_cloudwatch_rule(conf: Dict[str, str],
                           role: str,
                           cloudwatch_event_name: str,
                           schedule_expression: str,
                           description: str = None):
    arn = role_arn(get_account_id(conf), role)
    event_client = client(conf, 'events')
    response = event_client.put_rule(
        Name=cloudwatch_event_name,
        ScheduleExpression=schedule_expression,
        # EventPattern='string',
        State='ENABLED',
        Description=description,
        RoleArn=arn
    )
    return response


def create_sns_topic(conf, name: str, region_name):
    sns_client = client(conf, 'sns')
    response1 = sns_client.create_topic(
        Name=name
    )
    # account_id = get_account_id(conf)
    # TODO: get_account_id is called all over the place. Can we call it once and then pass it along?
    # response2 = sns_client.add_permission(
    #     TopicArn=sns_arn(region_name, account_id, name),
    #     Label='_'.join([RECEIVE, SUBSCRIBE]),
    #     AWSAccountId=[
    #         account_id
    #     ],
    #     ActionName=[
    #         RECEIVE,
    #         SUBSCRIBE
    #     ]
    # )
    # return [response1, response2]

    return [response1]


def sns_arn(region_name, account_id, sns_name):
    return "arn:aws:sns:{}:{}:{}".format(region_name, account_id, sns_name)


def event_arn(region_name, account_id, event_name):
    rule = f"rule/{event_name}"
    return "arn:aws:events:{}:{}:{}".format(region_name, account_id, rule)


def function_arn(region_name, account_id, qualified_lambda_name, alias=None):
    main_arn = 'arn:aws:lambda:{region_name}:{account_id}:function:{qualified_lambda_name}'.format(
        region_name=region_name,
        account_id=account_id,
        qualified_lambda_name=qualified_lambda_name)
    if alias:
        return main_arn + ":" + alias
    else:
        return main_arn


def role_arn(account_id, role_name):
    return 'arn:aws:iam::{account_id}:role/{role_name}'.format(account_id=account_id, role_name=role_name)


if __name__ == '__main__':
    from pprint import pprint

    lambda_client = client({}, 'lambda')
    policy = lambda_client.get_policy(
        FunctionName='jaya-sns-pp_EchoSNS')

    pprint(policy)
