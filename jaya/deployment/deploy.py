# from jaya.core import aws, Pipeline, AWSLambda
from jaya.sajan import Pipeline
from jaya.core import aws
from collections import defaultdict
from jaya.services import AWSLambda
from jaya.lib import aws as aws_lib
from jaya.sajan.pipeline.pipe import Leaf, Composite, Tree
# from pprint import pprint
# from localstack.utils.aws import aws_stack
# from localstack.mock import infra
from . import deploy_lambda
import os
import copy
from functools import partial
from pyrsistent import pmap

SERVICE_NAME_KEY = 'service_name'

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


def deploy_info(a_pipeline: Pipeline, test_mode: bool = False, qualify_lambda_name: bool = True):
    # TODO: If the same service appears twice in the pipeline
    assert len(a_pipeline.pipes) == 1, 'We support only single node roots for now'

    lambda_name_func = partial(lambda_name, qualify_lambda_name=qualify_lambda_name)
    copy_func = copy.copy if test_mode else copy.deepcopy

    pipe = a_pipeline.pipes[0]

    aggregator = init_aggregator()
    process_pipeline(aggregator, a_pipeline, pipe, copy_func, lambda_name_func)

    return aggregator


def init_aggregator():
    aggregator = defaultdict(dict)
    aggregator[LAMBDA] = defaultdict(dict)
    # aggregator[S3_NOTIFICATION] = defaultdict(dict)
    aggregator[S3] = defaultdict(dict)
    aggregator[TABLE] = defaultdict(dict)
    aggregator[FIREHOSE] = defaultdict(dict)
    return aggregator


def process_pipeline(aggregator, a_pipeline: Pipeline, pipe: Tree, copy_func, lambda_name_func):
    starting_node = pipe
    visited_node = starting_node
    if isinstance(visited_node, Leaf):
        process_leaf(aggregator, a_pipeline, visited_node, copy_func, lambda_name_func)
    elif isinstance(visited_node, Composite):
        process_composite_node(aggregator, a_pipeline, visited_node, copy_func, lambda_name_func)
        for child in visited_node.children():
            process_pipeline(aggregator, a_pipeline, child, copy_func, lambda_name_func)


def process_leaf(aggregator, a_pipeline, visited_node, copy_func, lambda_name_func):
    if visited_node.service_name == aws.S3:
        bucket_name = visited_node.bucket_name
        aggregator[S3][bucket_name] = {REGION_NAME: visited_node.region_name}
    elif visited_node.service_name == aws.LAMBDA:
        add_lambda(a_pipeline, aggregator, visited_node, copy_func, lambda_name_func)
    elif visited_node.service_name == aws.TABLE:
        aggregator[TABLE][visited_node.table_name] = visited_node
    elif visited_node.service_name == aws.FIREHOSE:
        aggregator[FIREHOSE][visited_node.firehose_name] = visited_node


def add_lambda(a_pipeline, aggregator, visited_node, copy_func, lambda_name_func):
    name = lambda_name_func(a_pipeline.name, visited_node.name)
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
            raise ValueError('There are multiple lambdas in the pipeline named {}. '
                             'Please change the names to be unique'.format(visited_node.name))


def lambda_name(pipeline_name: str, lambda_name: str, qualify_lambda_name: bool) -> str:
    if qualify_lambda_name:
        return '_'.join([pipeline_name, lambda_name])
    else:
        return lambda_name


def process_composite_node(aggregator, a_pipeline, visited_node, copy_func, lambda_name_func):
    node_value = visited_node.value()
    if node_value.service_name == aws.S3:
        bucket_name = node_value.bucket_name
        # TODO: What happens if there is a cycle in the graph, then the following initialization will reset the earlier configs for the same s3 bucket
        children = visited_node.children()
        if len(children) == 1:
            single_child_value = get_child_value(children[0])
            if single_child_value.service_name == aws.LAMBDA:
                name = lambda_name_func(a_pipeline.name, single_child_value.name)
                if S3_NOTIFICATION not in aggregator[LAMBDA][name]:
                    aggregator[LAMBDA][name][S3_NOTIFICATION] = {}
                lambda_notif_info = aggregator[LAMBDA][name][S3_NOTIFICATION]
                add_to_single_lambda_notif_info(lambda_notif_info, bucket_name, node_value.events,
                                                single_child_value.name)
        else:

            services = [get_child_value(c) for c in children]
            check_valid_mappings(node_value.events, services)

            for child in children:
                child_value = get_child_value(child)
                if child_value.service_name == aws.LAMBDA:
                    name = lambda_name_func(a_pipeline.name, child_value.name)
                    if S3_NOTIFICATION not in aggregator[LAMBDA][name]:
                        aggregator[LAMBDA][name][S3_NOTIFICATION] = {}
                    lambda_notif_info = aggregator[LAMBDA][name][S3_NOTIFICATION]
                    add_to_lambda_notif_info(lambda_notif_info, bucket_name, node_value.events, child_value.name)

        aggregator[S3][bucket_name] = {REGION_NAME: node_value.region_name}
    elif node_value.service_name == aws.LAMBDA:
        add_lambda(a_pipeline, aggregator, node_value, copy_func, lambda_name_func)
    elif node_value.service_name == aws.TABLE:
        aggregator[TABLE][node_value.table_name] = node_value
    elif node_value.service_name == aws.FIREHOSE:
        aggregator[FIREHOSE][node_value.firehose_name] = node_value


def get_child_value(single_child):
    if isinstance(single_child, Leaf):
        single_child_value = single_child
    else:
        single_child_value = single_child.value()
    return single_child_value


def check_valid_mappings(events, services):
    service_names = [service.name for service in services]
    for e in events:
        service_name = e.get(SERVICE_NAME_KEY)
        if not service_name or service_name not in service_names:
            raise ValueError(
                'Service name: {} in event:{} is not present in downstream service names: {}'.format(service_name,
                                                                                                     e,
                                                                                                     service_names))


def add_to_single_lambda_notif_info(lambda_notif_info, bucket_name, notifications, original_lambda_name):
    # TODO: Ensure that new_info is unique set as it is possible to put multiple notifications of the same values
    old_info = lambda_notif_info.get(bucket_name, [])
    new_info = old_info + single_lambda_notifications(notifications, original_lambda_name, bucket_name)

    lambda_notif_info[bucket_name] = new_info


def add_to_lambda_notif_info(lambda_notif_info, bucket_name, notifications, original_lambda_name):
    # TODO: Ensure that new_info is unique set as it is possible to put multiple notifications of the same values
    old_info = lambda_notif_info.get(bucket_name, [])
    new_info = old_info + lambda_notifications(notifications, original_lambda_name)

    lambda_notif_info[bucket_name] = new_info


def single_lambda_notifications(notifications, original_lambda_name, bucket_name):
    results = []
    for a_notification in notifications:
        p_notification = pmap(a_notification)
        service_name = p_notification.get(SERVICE_NAME_KEY)
        if service_name and service_name != original_lambda_name:
            raise ValueError(
                'S3 Notification:{} for bucket: {} incorrectly '
                'has service_name={} instead of {}'.format(p_notification,
                                                           bucket_name,
                                                           p_notification[SERVICE_NAME_KEY],
                                                           original_lambda_name))
        mod_n = p_notification.set(SERVICE_NAME_KEY, original_lambda_name)
        results.append(mod_n)
    return results


def lambda_notifications(notifications, original_lambda_name):
    return [n for n in notifications if n['service_name'] == original_lambda_name]


def deploy_pipeline(aws_conf, pipeline, qualify_lambda_name: bool = True):
    info = deploy_info(pipeline, test_mode=False, qualify_lambda_name=qualify_lambda_name)
    deploy_stack(aws_conf, info)


def deploy_node(aws_conf, pipeline, a_lambda_name, qualify_lambda_name: bool = True):
    subset_pipeline = subset(pipeline, a_lambda_name)
    if subset_pipeline:
        deploy_pipeline(aws_conf, subset_pipeline, qualify_lambda_name=qualify_lambda_name)
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


def deploy_stack(conf, info):
    s3_buckets = info[S3]
    for bucket, bucket_info in s3_buckets.items():
        aws_lib.create_s3_bucket(conf, bucket, bucket_info[REGION_NAME])

    lambdas = info[LAMBDA]
    for lambda_name, lambda_info in lambdas.items():
        lambda_instance = lambda_info[LAMBDA_INSTANCE]
        deploy_lambda.deploy_lambda_package(conf, lambda_instance)
        for bucket_name, notification_infos in lambda_info[S3_NOTIFICATION].items():
            for notification_info in notification_infos:
                aws_lib.add_s3_notification_for_lambda(conf,
                                                       bucket_name,
                                                       lambda_name,
                                                       notification_info['trigger'],
                                                       lambda_instance.alias,
                                                       prefix=notification_info.get('prefix', None),
                                                       suffix=notification_info.get('suffix', None),
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
