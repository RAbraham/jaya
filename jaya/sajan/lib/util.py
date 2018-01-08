import hashlib
import os
import functools

try:
    from ConfigParser import _Chainmap as ChainMap
except:
    from collections import ChainMap

import pprint as pp
from pprint import pprint


def filter_keys(old_dict, keys):
    return {key: old_dict[key] for key in keys if key in old_dict.keys()}


def filter_keys_except(old_dict, keys):
    return {key: old_dict[key] for key in old_dict.keys() if key not in keys}


# Merges a list of dicts. Common keys in subsequent dicts override the ones in earlier dicts
def merge_dicts(*l):
    rev = list(reversed(l))
    return dict(ChainMap(*rev))


def listify(item_or_items):
    if type(item_or_items) == list:
        children = item_or_items
    else:
        children = [item_or_items]

    return children


# def pickle_and_save_dill(a_func, path):
#     with open(path, "wb") as f:
#         dill.dump(a_func, f)


# def unpickle_handler_dill(path):
#     with open(path, "rb") as f:
#         r = dill.load(f)
#     return r
#

# def invoke_lambda_dill(event, context, a_lambda):
#     h = a_lambda.handler
#     path = '/tmp/what_a_handler.dill'
#     pickle_and_save_dill(h, path)
#     unpickled_handler = unpickle_handler_dill(path)
#     unpickled_handler(event, context)


def debug(debug_str, value):
    pp.pprint('>>>>>>>>>>>>>>> dbg_start:{} >>>>>>>>>>>>>>>'.format(debug_str))
    pp.pprint(value)
    pp.pprint('<<<<<<<<<<<<<<< dbg_end:{} <<<<<<<<<<<<<<<<<'.format(debug_str))


def debug_results(first, second):
    pprint(first)
    pprint('********************')
    pprint(second)


def md5_str(a_str):
    encoded_string = a_str.encode()
    m = hashlib.md5()
    m.update(encoded_string)
    return m.hexdigest()


def sha256_of_zipfile(file_path):
    import hashlib
    with open(file_path, 'rb') as f:
        code = f.read()
        m = hashlib.sha256()
        m.update(code)
        return m.digest()


def module_path(file_or_dir_module_or_python_object_or_file_path):
    try:
        if os.path.isfile(file_or_dir_module_or_python_object_or_file_path):
            return file_or_dir_module_or_python_object_or_file_path
    except:
        if '__path__' in file_or_dir_module_or_python_object_or_file_path.__dict__:
            return file_or_dir_module_or_python_object_or_file_path.__path__[0]
        elif '__file__' in file_or_dir_module_or_python_object_or_file_path.__dict__:
            return file_or_dir_module_or_python_object_or_file_path.__file__
        elif callable(file_or_dir_module_or_python_object_or_file_path):
            if type(file_or_dir_module_or_python_object_or_file_path) == functools.partial:
                return file_or_dir_module_or_python_object_or_file_path.func.__globals__['__file__']
            else:
                return file_or_dir_module_or_python_object_or_file_path.__globals__['__file__']
            # return file_or_dir_module_or_python_object_or_file_path.__globals__['__file__']
        else:
            raise ValueError('Is {} a module at all??'.format(str(file_or_dir_module_or_python_object_or_file_path)))
