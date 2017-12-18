import importlib.util
from jaya import Pipeline
from jaya import deploy_node, deploy_pipeline
import os
from jaya.config import config
import sys

def get_file_name(file_path):
    return os.path.basename(file_path).split('.')[0]
    pass


def load_module(file_path):

    spec = importlib.util.spec_from_file_location(get_file_name(file_path), file_path)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    return foo


def get_pipelines(module):
    pipelines = []
    vals = list(module.__dict__.values())
    for val in vals:
        if isinstance(val, Pipeline):
            pipelines.append(val)

    return pipelines


def deploy_file(config_file: str,
                file_path: str,
                pipeline_name: str,
                qualify_lambda_name: bool,
                lambda_name: str = None):
    aws_conf = config.get(config_file)
    aws_conf['aws_id'] = aws_conf['aws_access_key_id']
    aws_conf['aws_key'] = aws_conf['aws_secret_access_key']
    pipelines = []
    module = load_module(file_path)
    pipelines.extend(get_pipelines(module))

    the_pipelines = [p for p in pipelines if p.name == pipeline_name]
    assert len(the_pipelines) <= 1, "There can only be one pipeline with the name in the search space:" + pipeline_name
    if the_pipelines:
        the_pipeline = the_pipelines[0]
        # print('Fake Deploying:' + the_pipeline.name)
        if lambda_name:
            print('Deploying Pipeline:' + the_pipeline.name + ',function:' + lambda_name)
            deploy_node(aws_conf, the_pipeline, lambda_name, qualify_lambda_name=qualify_lambda_name)
        else:
            print('Deploying Pipeline:' + the_pipeline.name)
            deploy_pipeline(aws_conf, the_pipeline, qualify_lambda_name=qualify_lambda_name)
    else:
        print('No pipeline found with the name:' + pipeline_name)
