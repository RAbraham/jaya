# from jaya.core import aws, Pipeline, AWSLambda
from sajan import Pipeline
from jaya.core import aws
from collections import defaultdict
from jaya.lib import aws as aws_lib
from sajan.pipeline.pipe import Leaf, Composite, Tree
# from pprint import pprint
# from localstack.utils.aws import aws_stack
# from localstack.mock import infra
from . import deploy_lambda
import os
import copy

MOCK_CREDENTIALS = {'aws_id': 'rajiv_id', 'aws_key': 'rajiv_key'}
HANDLER = 'lambda.handler'

LAMBDA = 'lambda'
S3_SOURCE_BUCKET_NAME = 's3_source_bucket'
S3 = 's3'
S3_NOTIFICATION = 's3_notification'
EVENT_TRIGGERS = 'event_triggers'
LAMBDA_NAMES = 'lambda_names'
LAMBDA_NAME = "lambda_name"
LAMBDA_INSTANCE = 'lambda_instance'
REGION_NAME = 'region_name'
TABLE = 'table'
TRIGGERS = 'triggers'
FIREHOSE = 'firehose'


def deploy_info(a_pipeline: Pipeline, test_mode: bool = False):
    # TODO: If the same service appears twice in the pipeline
    assert len(a_pipeline.pipes) == 1, 'We support only single node roots for now'
    pipe = a_pipeline.pipes[0]

    copy_func = copy.copy if test_mode else copy.deepcopy

    aggregator = init_aggregator()
    process_pipeline(aggregator, a_pipeline, pipe, copy_func)

    return aggregator


def init_aggregator():
    aggregator = defaultdict(dict)
    aggregator[LAMBDA] = defaultdict(dict)
    aggregator[S3_NOTIFICATION] = defaultdict(dict)
    aggregator[S3] = defaultdict(dict)
    aggregator[TABLE] = defaultdict(dict)
    aggregator[FIREHOSE] = defaultdict(dict)
    return aggregator


def process_pipeline(aggregator, a_pipeline: Pipeline, pipe: Tree, copy_func):
    starting_node = pipe
    visited_node = starting_node
    if isinstance(visited_node, Leaf):
        process_leaf(aggregator, a_pipeline, visited_node, copy_func)
    elif isinstance(visited_node, Composite):
        process_composite_node(aggregator, a_pipeline, visited_node, copy_func)
        for child in visited_node.children():
            process_pipeline(aggregator, a_pipeline, child, copy_func)


# def process_leaf(aggregator, a_pipeline, visited_node, copy_func):
#     if visited_node.service.name == aws.S3:
#         bucket_name = visited_node.bucket
#         aggregator[S3][bucket_name] = {REGION_NAME: visited_node.region_name}
#     elif visited_node.service.name == aws.LAMBDA:
#         add_lambda(a_pipeline, aggregator, visited_node, copy_func)
#     elif visited_node.service.name == aws.TABLE:
#         aggregator[TABLE][visited_node.table_name] = visited_node
#     elif visited_node.service.name == aws.FIREHOSE:
#         aggregator[FIREHOSE][visited_node.firehose_name] = visited_node

def process_leaf(aggregator, a_pipeline, visited_node, copy_func):
    if visited_node.service_name == aws.S3:
        bucket_name = visited_node.bucket
        aggregator[S3][bucket_name] = {REGION_NAME: visited_node.region_name}
    elif visited_node.service_name == aws.LAMBDA:
        add_lambda(a_pipeline, aggregator, visited_node, copy_func)
    elif visited_node.service_name == aws.TABLE:
        aggregator[TABLE][visited_node.table_name] = visited_node
    elif visited_node.service_name == aws.FIREHOSE:
        aggregator[FIREHOSE][visited_node.firehose_name] = visited_node


def add_lambda(a_pipeline, aggregator, visited_node, copy_func):
    name = lambda_name(a_pipeline.name, visited_node.name)
    assert_unique_lambda_names(aggregator, name, visited_node)

    copied_lambda = copy_lambda(name, visited_node, copy_func)
    aggregator[LAMBDA][name][LAMBDA_INSTANCE] = copied_lambda


def copy_lambda(name, visited_node, copy_func):
    copied_lambda = copy_func(visited_node)
    copied_lambda.name = name
    return copied_lambda


def assert_unique_lambda_names(aggregator, name, visited_node):
    lambda_instance = aggregator[LAMBDA].get(name, {}).get(LAMBDA_INSTANCE, {})
    if lambda_instance:
        if lambda_instance.name == name:
            raise Exception('There are multiple lambdas in the pipeline named {}. '
                            'Please change the names to be unique'.format(visited_node.name))


def lambda_name(pipeline_name: str, lambda_name: str) -> str:
    return '_'.join([pipeline_name, lambda_name])


# def process_composite_node(aggregator, a_pipeline, visited_node, copy_func):
#     node_value = visited_node.value()
#     if node_value.service.name == aws.S3:
#
#         bucket_name = node_value.bucket
#         # TODO: What happens if there is a cycle in the graph, then the following initialization will reset the earlier configs for the same s3 bucket
#         aggregator[S3_NOTIFICATION][bucket_name] = []
#         children = visited_node.children()
#         for child in children:
#             if isinstance(child, Leaf):
#                 child_value = child
#             else:
#                 child_value = child.value()
#             if child_value.service.name == aws.LAMBDA:
#                 name = lambda_name(a_pipeline.name, child_value.name)
#                 aggregator[LAMBDA][name][S3_SOURCE_BUCKET_NAME] = bucket_name
#                 aggregator[S3_NOTIFICATION][bucket_name].append(notification(name, node_value.on))
#
#         aggregator[S3][bucket_name] = {REGION_NAME: node_value.region_name}
#     elif node_value.service.name == aws.LAMBDA:
#         add_lambda(a_pipeline, aggregator, node_value, copy_func)
#     elif node_value.service.name == aws.TABLE:
#         aggregator[TABLE][node_value.table_name] = node_value
#     elif node_value.service.name == aws.FIREHOSE:
#         aggregator[FIREHOSE][node_value.firehose_name] = node_value


def process_composite_node(aggregator, a_pipeline, visited_node, copy_func):

    node_value = visited_node.value()
    if node_value.service_name == aws.S3:
        bucket_name = node_value.bucket
        # TODO: What happens if there is a cycle in the graph, then the following initialization will reset the earlier configs for the same s3 bucket
        aggregator[S3_NOTIFICATION][bucket_name] = []
        children = visited_node.children()
        for child in children:
            if isinstance(child, Leaf):
                child_value = child
            else:
                child_value = child.value()
            if child_value.service_name == aws.LAMBDA:
                name = lambda_name(a_pipeline.name, child_value.name)
                aggregator[LAMBDA][name][S3_SOURCE_BUCKET_NAME] = bucket_name
                aggregator[S3_NOTIFICATION][bucket_name].append(notification(name, node_value.on))

        aggregator[S3][bucket_name] = {REGION_NAME: node_value.region_name}
    elif node_value.service_name == aws.LAMBDA:
        add_lambda(a_pipeline, aggregator, node_value, copy_func)
    elif node_value.service_name == aws.TABLE:
        aggregator[TABLE][node_value.table_name] = node_value
    elif node_value.service_name == aws.FIREHOSE:
        aggregator[FIREHOSE][node_value.firehose_name] = node_value


def notification(lambda_name, triggers):
    return {LAMBDA_NAME: lambda_name, TRIGGERS: triggers}


def deploy_pipeline(aws_conf, pipeline):
    info = deploy_info(pipeline)
    deploy_stack_info(aws_conf, info)


def deploy_node(aws_conf, pipeline, a_lambda_name):
    subset_pipeline = subset(pipeline, a_lambda_name)
    if subset_pipeline:
        deploy_pipeline(aws_conf, subset_pipeline)
    else:
        print('No match found for ' +
              'Pipeline:' +
              pipeline.name +
              ',function:' +
              a_lambda_name +
              '. No Deployment made')


def subset(a_pipeline, a_lambda_name):
    assert len(a_pipeline.pipes) == 1, 'We support only single node roots for now'
    pipe = a_pipeline.pipes[0]
    subset_pipe = subset_tree(pipe, None, a_lambda_name)
    if subset_pipe:
        return Pipeline(a_pipeline.name, [subset_pipe])
    else:
        return None


def subset_tree(tree, upstream, a_lambda_name):
    if isinstance(tree, Leaf):
        return node_subset(tree, upstream, a_lambda_name)
    elif isinstance(tree, Composite):
        return subset_composite(tree, upstream, a_lambda_name)
    else:
        raise ValueError('Invalid Tree type')
    pass


# def node_subset(tree, upstream, a_lambda_name):
#     if tree.service.name == aws.LAMBDA and tree.name == a_lambda_name:
#         if upstream:
#             return upstream >> tree
#         else:
#             return tree
#     else:
#         return None

def node_subset(tree, upstream, a_lambda_name):
    if tree.service_name == aws.LAMBDA and tree.name == a_lambda_name:
        if upstream:
            return upstream >> tree
        else:
            return tree
    else:
        return None


def subset_composite(tree, upstream, a_lambda_name):
    node = tree.root()
    a_node_subset = node_subset(node, upstream, a_lambda_name)
    if a_node_subset:
        return a_node_subset
    else:
        for child in tree.children():
            s = subset_tree(child, node, a_lambda_name)
            if s:
                return s
        return None


def deploy_stack_info(conf, info):
    s3_buckets = info[S3]
    for bucket, bucket_info in s3_buckets.items():
        aws_lib.create_s3_bucket(conf, bucket, bucket_info[REGION_NAME])

    lambdas = info[LAMBDA]
    for lambda_name, lambda_info in lambdas.items():
        lambda_instance = lambda_info[LAMBDA_INSTANCE]
        deploy_lambda.deploy_lambda_package_new_simple(conf, lambda_instance)
        aws_lib.add_s3_notification_for_lambda(conf,
                                               lambda_info[S3_SOURCE_BUCKET_NAME],
                                               lambda_name,
                                               lambda_instance.alias,
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
