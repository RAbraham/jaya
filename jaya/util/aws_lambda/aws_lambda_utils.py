from jaya.pipeline.pipe import Leaf, Composite

# TODO: Extract Jaya out of Aadith
from jaya.core import AWSLambda, module_path

from _functools import partial

from jaya.lib import util
from jaya.config import config

import jaya


class EchoLambda(AWSLambda):
    def __init__(self, region_name, alias):
        super(EchoLambda, self).__init__('EchoLambda', echo_handler, region_name, alias=alias, dependencies=[jaya])


def echo_handler(event, context):
    from pprint import pprint
    pprint('In Echo Handler')
    pprint(event)


class CopyS3Lambda(AWSLambda):
    def __init__(self, configuration, region_name, alias):
        self.configuration = configuration
        self.region_name = region_name
        self.alias = alias
        super(CopyS3Lambda, self).__init__('CopyS3Lambda', None, region_name, alias=alias, dependencies=[jaya])

    def __rshift__(self, node_or_nodes):
        print('In CopyS3Lambda Rshift')
        children = util.listify(node_or_nodes)
        dest_funcs = [self.make_dest_func(child) for child in children]
        handler_func = make_handler_func(dest_funcs)
        lambda_leaf = AWSLambda('CopyS3Lambda',
                                handler=handler_func,
                                region_name=self.region_name,
                                alias=self.alias,
                                dependencies=[jaya])
        return Composite(lambda_leaf, children)

    @staticmethod
    def make_dest_func(s3_child):
        return lambda b, k: tuple([s3_child.bucket, k])


def make_handler_func(dest_funcs):
    def handler(event, context):
        print('Using Module Path')
        environment = util.get_arn_environment(context.invoked_function_arn)

        conf = config.get_aws_config(environment)
        for dest_func in dest_funcs:
            bucket_key_pairs = util.get_bucket_key_pairs_from_event(event)
            copy_to_buckets(conf, bucket_key_pairs, dest_func)

    return handler


def copy_to_buckets(conf, bucket_key_pairs, dest_func):
    from jaya.lib import aws
    for bucket, key in bucket_key_pairs:
        dest_bucket, dest_key = dest_func(bucket, key)
        print('Rajiv: Dest Bucket:' + dest_bucket)
        print('Rajiv: Dest Key:' + dest_key)
        aws.copy_from_s3_to_s3(conf, bucket, key, dest_bucket, dest_key)


class CreateFileLambda(Leaf):
    def __init__(self):
        super(CreateFileLambda, self).__init__('Test CreateFileLambda Leaf Value Rajiv ')

    # def __rshift__(self, node_or_nodes):
    #     children = util.listify(node_or_nodes)
    #     child = children[0]
    #     handler_func = self.make_handler_func(child)
    #     lambda_leaf = AWSLambda('CreateFileLambda',
    #                             handler=handler_func)
    #
    #     # TODO: This could be delegated to a Leaf Class?
    #     return Composite(lambda_leaf, children)

    @staticmethod
    def make_handler_func(s3_node):
        bucket = s3_node.bucket
        dest_func = destination_func(bucket)
        h_p = partial(simple_handler, dest_func=dest_func)
        return h_p


def destination_func(actual_bucket):
    def dest_func(b, k):
        return actual_bucket, k

    return dest_func


def simple_handler(event, context, dest_func):
    b, k = dest_func('gg_x', 'gg_y')
    print('b:' + b)
    print('k:' + k)
