import os
import hashlib

try:
    from ConfigParser import _Chainmap as ChainMap
except:
    from collections import ChainMap

import datetime
import gzip
import operator
import itertools
import pprint as pp
from pprint import pprint
import dill

TORONTO_TIMEZONE = "America/Toronto"


def file_relative_path(relative_file, file_name):
    return os.path.join(os.path.dirname(relative_file), file_name)


# level is how far up you want to go, level 1 is one level up.
def parent_dir(relative_file, level=1):
    assert level >= 1, "Only positive integers accepted for level. Don't mess with the boss"
    parent_dots = ['..' for i in range(level)]
    return os.path.abspath(os.path.join(os.path.dirname(relative_file), *parent_dots))


def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]


# Returns all immediate files and folders using full path
def get_children(a_dir):
    children = os.listdir(a_dir)
    return [os.path.join(a_dir, e) for e in children]


def filter_and_strip(file_lines):
    return [e.rstrip('\n') for e in file_lines if e is not None]


def flatten(ll):
    return [i for l in ll for i in l]


def etl_sources(bucket_prefix, regions):
    if regions:
        buckets = [bucket_prefix + '-' + region for region in regions]
    else:
        buckets = [bucket_prefix]

    return buckets


def get_arn_environment(invoked_function_arn):
    splits = invoked_function_arn.split(":")
    assert len(splits) == 8, "ARN:{0} does not have environment alias set".format(invoked_function_arn)
    return splits[7]


def get_bucket_key_pairs_from_event(event):
    return [(record['s3']['bucket']['name'],
             record['s3']['object']['key'])
            for record
            in event['Records']]


def text_chunks(file_object, chunk_size=1048576):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1MB."""
    while True:
        data = file_object.readlines(chunk_size)
        if not data:
            break
        yield data


def filter_keys(old_dict, keys):
    return {key: old_dict[key] for key in keys if key in old_dict.keys()}


def filter_keys_except(old_dict, keys):
    return {key: old_dict[key] for key in old_dict.keys() if key not in keys}


# Merges a list of dicts. Common keys in subsequent dicts override the ones in earlier dicts
def merge_dicts(*l):
    rev = list(reversed(l))
    return dict(ChainMap(*rev))


def dict_get(dct, *keys):
    '''

    :param dct:
    :param keys: Nested keys within the dict
    :return: value if present, None if any of the keys are absent
    '''
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


def group_by_key(rows, *keys):
    key_func = operator.itemgetter(*keys)
    sorted_rows = sorted(rows, key=key_func)
    grouped = itertools.groupby(sorted_rows, key_func)
    return {k: list(rows) for k, rows in grouped}


def utc_now():
    return datetime.datetime.utcnow()


# e.g. date_str('%a %b %d %X %Y'): 'Fri May 06 22:59:07 2016'
def str_to_datetime(date_str, str_format='%a %b %d %X %Y'):
    dt = datetime.datetime.strptime(date_str, str_format)
    return dt


# epoch_time: float
def unix_epoch_time_to_datetime(epoch_time):
    return datetime.datetime.utcfromtimestamp(epoch_time)


def epoch_to_utc_str(epoch_str):
    return str(unix_epoch_time_to_datetime(float(epoch_str)))


def error_dict(an_exception):
    # traceback.print_exc()
    return {'message': str(an_exception),
            'type': an_exception.__class__.__name__}


# For stress testing
def create_bigger_file(input_path, output_path, number_of_copies):
    lines = []
    with gzip.open(input_path) as ih:
        lines = ih.readlines()

    with gzip.open(output_path, 'w') as oh:
        for i in range(number_of_copies):
            for l in lines:
                oh.write(l)


def tee_filter(items, predicate=bool):
    a, b = itertools.tee((predicate(item), item) for item in items)
    return (list(item for pred, item in a if pred),
            list(item for pred, item in b if not pred))


def listify(item_or_items):
    if type(item_or_items) == list:
        children = item_or_items
    else:
        children = [item_or_items]

    return children


def pickle_and_save_dill(a_func, path):
    with open(path, "wb") as f:
        dill.dump(a_func, f)


def unpickle_handler_dill(path):
    with open(path, "rb") as f:
        r = dill.load(f)
    return r


def invoke_lambda_dill(event, context, a_lambda):
    h = a_lambda.handler
    path = '/tmp/what_a_handler.dill'
    pickle_and_save_dill(h, path)
    unpickled_handler = unpickle_handler_dill(path)
    unpickled_handler(event, context)


def debug(debug_str, value):
    pp.pprint('>>>>>>>>>>>>>>> dbg_start:{} >>>>>>>>>>>>>>>'.format(debug_str))
    pp.pprint(value)
    pp.pprint('<<<<<<<<<<<<<<< dbg_end:{} <<<<<<<<<<<<<<<<<'.format(debug_str))


def write_file(a_path, lines):
    with gzip.open(a_path, 'w') as f:
        for l in lines:
            f.write(l + '\n')


def debug_results(first, second):
    pprint(first)
    pprint('********************')
    pprint(second)


def md5_str(a_str):
    encoded_string = a_str.encode()
    m = hashlib.md5()
    m.update(encoded_string)
    return m.hexdigest()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def sha256_of_zipfile(file_path):
    import hashlib
    with open(file_path, 'rb') as f:
        code = f.read()
        m = hashlib.sha256()
        m.update(code)
        return m.digest()
