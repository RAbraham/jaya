import sys
import os

from jaya.util.aws_lambda.aws_lambda_utils import CreateFileLambda, CopyS3Lambda
from jaya.core import S3
from jaya.deployment.deploy_lambda import deploy_lambda_package, deploy_pipeline
from jaya.lib import util
import jaya.lib as jl

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from jaya.core import Pipeline


# from jaya.config import config
# from jaya.lib import util
# from jaya.deployment.deploy_lambda import deploy_lambda
# from jaya.lib import aws

# src_root = config.project_root()
# py2_handler = src_root + '/aws_lambda/handler.py'
# py3_handler = src_root + '/aws_lambda/handler_python3.py'
# venv3_path = '/Users/rabraham/Documents/dev/thescore/analytics/jaya/jaya/aws_lambda/linux_venv/'
# venv3_path_site_packages = venv3_path + 'lib/python3.6/site-packages'
# python3_packages = util.get_children(venv3_path_site_packages)
#
# lambda_name = 'test_py3_runner'
# conf = config.get_aws_config('production')
# aws.delete_lambda(conf, lambda_name, 'us-east-1')
# deploy_lambda('production',
#               lambda_name,
#               [py2_handler, py3_handler, venv3_path] + python3_packages,
#               'lambda_s3_exec_role',
#               128,
#               300,
#               lambda_description='Test for Python 3',
#               alias_description='Alias for ' + 'production',
#               update=True,
#               handler_name="handler",
#               region_name='us-east-1')



if __name__ == '__main__':
    region = 'us-east-1'
    # p = CreateFileLambda() >> S3(bucket='yahoo---dill-bucket', region_name='us-east-1')

    p = CopyS3Lambda({}) >> S3(bucket='thescore-demo-destination', region_name=region)
    # p = CopyS3Lambda({}) >> [S3(bucket='thescore-demo-destination', region_name=region),
    #                          S3(bucket='thescore-demo-destination1', region_name=region)]
    # p = CopyS3Lambda({}) >> S3(bucket='thescore-demo-destination', region_name=region)
    # p = CopyS3Lambda({}) >> S3(destination_func=lambda b, k: (b + '_new_yahoo', k + '_new_yahoo_key'))
    # p = CopyS3Lambda({}) >> [S3(destination_func=lambda b, k: (b + '_new_yahoo1', k + '_new_yahoo_key1')),
    #                          S3(destination_func=lambda b, k: (b + '_new_yahoo2', k + '_new_yahoo_key2'))]
    aws_lambda = p.value()
    # a_path = '/tmp/a_lambda_package.zip'
    # create_lambda_package(aws_lambda, a_path)
    # deploy_lambda_package(path)

    # p = S3(bucket='test-bucket', on=['s3:ObjectCreated:*']) >> CopyS3Lambda({})

    # p = S3(bucket='source-bucket', on=[S3.ALL_CREATED_OBJECTS]) >> \
    #     CopyS3Lambda({}) >> \
    #     S3(bucket='destination-bucket')

    # deploy(Pipeline(name='Test Pipeline', tree_list=[p]))

    deploy_lambda_package(aws_lambda)

    from pprint import pprint

    # pprint(jl.__file__)
    # pprint('__path__' in jl.__dict__)
    # pprint('__path__' in util.__dict__)
    # print(util.__file__)
    # p = S3(bucket='thescore-demo-source', region_name=region) \
    #     >> CopyS3Lambda({}) \
    #     >> S3(bucket='thescore-demo-destination', region_name=region)
    # piper = Pipeline('copy_alan_kay', [p])
    # deploy_pipeline(piper)
    pass
