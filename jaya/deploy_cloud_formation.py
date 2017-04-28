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
    import boto3

    #     AWSTemplateFormatVersion: '2010-09-09'
    #     Transform: AWS::Serverless - 2016 - 10 - 31
    #     Resources:
    #     CreateThumbnail:
    #     Type: AWS::Serverless::Function
    #     Properties:
    #     Handler: handler
    #     Runtime: runtime
    #     Timeout: 60
    #     Policies: AWSLambdaExecute
    #     Events:
    #     Type: S3
    #     Properties:
    #     Bucket: !Ref
    #     SrcBucket
    #     Events: s3:ObjectCreated: *
    #
    # SrcBucket:
    # Type: AWS::S3::Bucket
    test_lambda_template = {
        'AWSTemplateFormatVersion': '2010-09-09',
        'Transform': 'AWS::Serverless-2016-10-31',
        'Resources': {
            'CopyS3Rajiv': {
                'Type': 'AWS::Serverless::Function',

                'Properties': {
                    "CodeUri": 's3://thescore-tmp/CopyS3Lambda',
                    "Handler": 'lambda.handler',
                    "Runtime": 'python3.6',
                    "Timeout": 300,
                    'Policies': 'AWSLambdaExecute'
                },
                'Events': {
                    'Type': 'S3',
                    'Properties': {
                        "Bucket": '!Ref SrcBucket',
                        "Events": 's3:ObjectCreated:*'

                    }

                }
            },
            'SrcBucket': {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": 'thescore-cloudfront-trial',
                }
            }

        }

    }

    import json
    import jaya.lib.aws as aws
    from jaya.config import config
    from pprint import pprint

    conf = config.get_aws_config('development')
    client = aws.client(conf, 'cloudformation', region_name=region)
    # response = client.create_stack(
    #     StackName='RajivTestStack',
    #     TemplateBody=json.dumps(test_lambda_template),
    #
    #     ResourceTypes=[
    #         "AWS::Lambda::Function",
    #         "AWS::S3::Bucket"
    #     ],
    #
    # )

    # response = client.create_change_set(
    #     StackName='RajivTestStack',
    #     TemplateBody=json.dumps(test_lambda_template),
    #     # ResourceTypes=[
    #     #     "AWS::Lambda::Function",
    #     #     "AWS::S3::Bucket"
    #     # ],
    #     Capabilities=['CAPABILITY_IAM'],
    #     ChangeSetName='a',
    #     Description='Rajiv ChangeSet Description',
    #     ChangeSetType='CREATE'
    # )
    #
    # pprint(response)

    response = client.execute_change_set(
        ChangeSetName='a',
        StackName='RajivTestStack',
    )

    pprint(response)
