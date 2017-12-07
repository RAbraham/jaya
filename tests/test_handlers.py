from functools import partial
import jaya
from jaya.lib import util
from jaya.services import AWSLambda


def echo_handler(nodes, event, context):
    import sys
    import datetime
    print(sys.version)

    print('In New Echo Handler:' + str(datetime.datetime.utcnow().isoformat()))
    print(event)


def copy_to_buckets(conf, bucket_key_pairs, dest_func):
    from jaya.lib import aws
    for bucket, key in bucket_key_pairs:
        dest_bucket, dest_key = dest_func(bucket, key)
        print('Rajiv: Dest Bucket:' + dest_bucket)
        print('Rajiv: Dest Key:' + dest_key)
        aws.copy_from_s3_to_s3(conf, bucket, key, dest_bucket, dest_key)


def copy_handler(configuration, nodes, event, context):
    def make_dest_func(s3_child):
        return lambda b, k: tuple([s3_child.bucket, k])

    dest_funcs = [make_dest_func(child) for child in nodes]
    # environment = util.get_arn_environment(context.invoked_function_arn)

    conf = configuration
    for dest_func in dest_funcs:
        bucket_key_pairs = util.get_bucket_key_pairs_from_event(event)
        copy_to_buckets(conf, bucket_key_pairs, dest_func)


def copy_handler_service(name, region, configuration, **kwargs):
    kwargs['dependencies'] = kwargs.get('dependencies', []) + [jaya]
    copy_handler_func = partial(copy_handler, configuration)
    return AWSLambda(name,
                     copy_handler_func,
                     region,
                     **kwargs)
