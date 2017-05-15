from jaya.core import aws
from collections import defaultdict
from jaya.lib import aws as aws_lib
from jaya.pipeline.pipe import Leaf, Composite
from pprint import pprint

from jaya.deployment import deploy_lambda

LAMBDA = 'lambda'
S3_SOURCE_BUCKET_NAME = 's3_source_bucket'
S3 = 's3'
S3_NOTIFICATION = 's3_notification'
EVENT_TRIGGERS = 'event_triggers'
LAMBDA_NAMES = 'lambda_names'
LAMBDA_INSTANCE = 'lambda_instance'
REGION_NAME = 'region_name'


def create_deploy_stack_info(a_pipeline):
    # TODO: If the same service appears twice in the pipeline
    assert len(a_pipeline.pipes) == 1, 'We support only single node roots for now'
    pipe = a_pipeline.pipes[0]

    aggregator = defaultdict(dict)
    aggregator[LAMBDA] = defaultdict(dict)
    aggregator[S3_NOTIFICATION] = defaultdict(dict)
    aggregator[S3] = defaultdict(dict)

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
