from jaya.core import aws
from collections import defaultdict
from jaya.lib import aws as aws_lib
from jaya.pipeline.pipe import Leaf, Composite
from pprint import pprint
LAMBDA = 'lambda'
S3_SOURCE_BUCKET_NAME = 's3_source_bucket'
S3 = 's3'
S3_NOTIFICATION = 's3_notification'
EVENT_TRIGGERS = 'event_triggers'
LAMBDA_NAMES = 'lambda_names'
REGION_NAME = 'region_name'


def create_deploy_stack(a_pipeline):
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


def process_composite_node(aggregator, visited_node):
    pprint(visited_node)
    node_value = visited_node.value()

    if node_value.service.name == aws.S3:
        bucket_name = node_value.bucket
        children = visited_node.children()
        for child in children:
            if isinstance(child, Leaf):
                child_value = child
            else:
                child_value = child.value()
            if child_value.service.name == aws.LAMBDA:
                lambda_name = child_value.name
                aggregator[LAMBDA][lambda_name][S3_SOURCE_BUCKET_NAME] = bucket_name
                aggregator[S3_NOTIFICATION][bucket_name][LAMBDA_NAMES].append(lambda_name)
                aggregator[S3_NOTIFICATION][bucket_name][EVENT_TRIGGERS] = node_value.on

        aggregator[S3][bucket_name] = {REGION_NAME: node_value.region_name}


def deploy_stack_info(conf, info):
    s3_buckets = info[S3]
    for bucket, bucket_info in s3_buckets.items():
        aws_lib.create_s3_bucket(conf, bucket, bucket_info[REGION_NAME])
