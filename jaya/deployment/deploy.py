from jaya.core import aws
from collections import defaultdict
from jaya.lib import aws as aws_lib
from jaya.pipeline.pipe import Leaf, Composite
from pprint import pprint
from localstack.utils.aws import aws_stack
from localstack.mock import infra
from jaya.deployment import deploy_lambda

MOCK_CREDENTIALS = {'aws_id': 'rajiv_id', 'aws_key': 'rajiv_key'}

LAMBDA = 'lambda'
S3_SOURCE_BUCKET_NAME = 's3_source_bucket'
S3 = 's3'
S3_NOTIFICATION = 's3_notification'
EVENT_TRIGGERS = 'event_triggers'
LAMBDA_NAMES = 'lambda_names'
LAMBDA_INSTANCE = 'lambda_instance'
REGION_NAME = 'region_name'
TABLE = 'table'
FIREHOSE = 'firehose'


def create_deploy_stack_info(a_pipeline):
    # TODO: If the same service appears twice in the pipeline
    assert len(a_pipeline.pipes) == 1, 'We support only single node roots for now'
    pipe = a_pipeline.pipes[0]

    aggregator = defaultdict(dict)
    aggregator[LAMBDA] = defaultdict(dict)
    aggregator[S3_NOTIFICATION] = defaultdict(dict)
    aggregator[S3] = defaultdict(dict)
    aggregator[TABLE] = defaultdict(dict)
    aggregator[FIREHOSE] = defaultdict(dict)
    process_pipeline(aggregator, pipe)

    return aggregator


def process_pipeline(aggregator, pipe):
    starting_node = pipe
    visited_node = starting_node
    if isinstance(visited_node, Leaf):
        process_leaf(aggregator, visited_node)
    elif isinstance(visited_node, Composite):
        process_composite_node(aggregator, visited_node)
        for child in visited_node.children():
            process_pipeline(aggregator, child)


def process_leaf(aggregator, visited_node):
    if visited_node.service.name == aws.S3:
        bucket_name = visited_node.bucket
        aggregator[S3][bucket_name] = {REGION_NAME: visited_node.region_name}
    elif visited_node.service.name == aws.LAMBDA:
        aggregator[LAMBDA][visited_node.name][LAMBDA_INSTANCE] = visited_node
    elif visited_node.service.name == aws.TABLE:
        aggregator[TABLE][visited_node.table_name] = visited_node
    elif visited_node.service.name == aws.FIREHOSE:
        aggregator[FIREHOSE][visited_node.firehose_name] = visited_node


def process_composite_node(aggregator, visited_node):
    node_value = visited_node.value()
    if node_value.service.name == aws.S3:

        bucket_name = node_value.bucket
        # TODO: What happens if there is a cycle in the graph, then the following initialization will reset the earlier configs for the same s3 bucket
        aggregator[S3_NOTIFICATION][bucket_name] = []
        children = visited_node.children()
        for child in children:
            if isinstance(child, Leaf):
                child_value = child
            else:
                child_value = child.value()
            if child_value.service.name == aws.LAMBDA:
                lambda_name = child_value.name
                aggregator[LAMBDA][lambda_name][S3_SOURCE_BUCKET_NAME] = bucket_name
                aggregator[S3_NOTIFICATION][bucket_name].append(notification(lambda_name, node_value.on))

        aggregator[S3][bucket_name] = {REGION_NAME: node_value.region_name}
    elif node_value.service.name == aws.LAMBDA:
        aggregator[LAMBDA][node_value.name][LAMBDA_INSTANCE] = node_value


def notification(lambda_name, triggers):
    return {"lambda_name": lambda_name, 'triggers': triggers}


def deploy_stack_info(conf, environment, info):
    s3_buckets = info[S3]
    for bucket, bucket_info in s3_buckets.items():
        aws_lib.create_s3_bucket(conf, bucket, bucket_info[REGION_NAME])

    lambdas = info[LAMBDA]
    for lambda_name, lambda_info in lambdas.items():
        lambda_instance = lambda_info[LAMBDA_INSTANCE]
        deploy_lambda.deploy_lambda_package_new(environment, lambda_info[LAMBDA_INSTANCE])
        aws_lib.add_s3_notification_for_lambda(conf,
                                               lambda_info[S3_SOURCE_BUCKET_NAME],
                                               lambda_name,
                                               environment,
                                               prefix=lambda_info.get('prefix', None),
                                               region_name=lambda_instance.region_name)

    tables = info[TABLE]
    for table_name, table in tables.items():
        table.deploy()

    firehoses = info[FIREHOSE]
    for firehose_name, f in firehoses.items():
        role_arn = aws_lib.resource(conf, 'iam').Role(f.role_name).arn
        conf['db-port'] = '5439'
        conf['db-name'] = f.database_name
        aws_lib.create_firehose_stream(conf,
                                       role_arn,
                                       f.user_name,
                                       f.user_password,
                                       f.firehose_name,
                                       f.server_address,
                                       f.database_name,
                                       f.table_name,
                                       f.copy_options,
                                       f.holding_bucket,
                                       f.prefix,
                                       f.buffering_size_mb,
                                       f.buffering_interval_seconds,
                                       f.log_group,
                                       f.log_stream
                                       )

# def deploy_stack_info_local(info):
#     s3_buckets = info[S3]
#     for bucket, bucket_info in s3_buckets.items():
#         aws_lib.create_s3_bucket(MOCK_CREDENTIALS, bucket, bucket_info[REGION_NAME])
#
#     lambdas = info[LAMBDA]
#     for lambda_name, lambda_info in lambdas.items():
#         lambda_instance = lambda_info[LAMBDA_INSTANCE]
#         deploy_lambda.deploy_lambda_package_local(lambda_info[LAMBDA_INSTANCE])
#         aws_lib.add_s3_notification_for_lambda(conf,
#                                                lambda_info[S3_SOURCE_BUCKET_NAME],
#                                                lambda_name,
#                                                environment,
#                                                prefix=lambda_info.get('prefix', None),
#                                                region_name=lambda_instance.region_name)


# def deploy_stack_info_localstack(conf, environment, info):
#     s3_buckets = info[S3]
#     s3_resource = aws_stack.connect_to_resource('s3')
#     for bucket, bucket_info in s3_buckets.items():
#         s3_resource.create_bucket(Bucket=bucket)
#
#     lambdas = info[LAMBDA]
#     for lambda_name, lambda_info in lambdas.items():
#         lambda_instance = lambda_info[LAMBDA_INSTANCE]
#         deploy_lambda.deploy_lambda_package_new(environment, lambda_info[LAMBDA_INSTANCE], mock=True)
#         aws_lib.add_s3_notification_for_lambda(conf,
#                                                lambda_info[S3_SOURCE_BUCKET_NAME],
#                                                lambda_name,
#                                                environment,
#                                                prefix=lambda_info.get('prefix', None),
#                                                region_name=lambda_instance.region_name,
#                                                mock=True)
