import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
stack_name = 'RajivTestStack'

if __name__ == '__main__':
    region = 'us-east-1'
    import boto3

    # test_lambda_template = {
    #     'AWSTemplateFormatVersion': '2010-09-09',
    #     'Transform': 'AWS::Serverless-2016-10-31',
    #     'Resources': {
    #         'CopyS3RajivCloudF': {
    #             'Type': 'AWS::Serverless::Function',
    #             'Properties': {
    #                 "CodeUri": 's3://thescore-tmp/CopyS3Lambda',
    #                 "Handler": 'lambda.handler',
    #                 "Runtime": 'python3.6',
    #                 "Timeout": 300,
    #                 "Role": 'arn:aws:iam::027995586716:role/lambda_s3_exec_role'
    #             },
    #
    #         },
    #         "AliasForMyApp": {
    #             "Type": "AWS::Lambda::Alias",
    #             "Properties": {
    #                 "FunctionName": {"Ref": "CopyS3RajivCloudF"},
    #                 "FunctionVersion": "$LATEST",
    #                 "Name": "staging"
    #             }
    #         }
    #
    #     }
    #
    # }

    test_lambda_template = {
        'AWSTemplateFormatVersion': '2010-09-09',
        'Transform': 'AWS::Serverless-2016-10-31',
        'Resources': {
            'CopyS3RajivCloudF': {
                'Type': 'AWS::Serverless::Function',

                'Properties': {
                    "CodeUri": 's3://thescore-tmp/CopyS3Lambda',
                    "Handler": 'lambda.handler',
                    "Runtime": 'python3.6',
                    "Timeout": 300,
                    "Role": 'arn:aws:iam::027995586716:role/lambda_s3_exec_role',

                    'Events': {
                        'RajivCopyEvent': {
                            'Type': 'S3',
                            'Properties': {
                                "Bucket": {"Ref": "SrcBucket"},
                                "Events": "s3:ObjectCreated:*"

                            }
                        }

                    }
                },

            },

            "AliasForMyApp": {
                "Type": "AWS::Lambda::Alias",
                "Properties": {
                    "FunctionName": {"Ref": "CopyS3RajivCloudF"},
                    "FunctionVersion": "$LATEST",
                    "Name": "staging"
                }
            }

            ,
            'SrcBucket': {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": 'thescore-cloudfront-trial',
                    # "NotificationConfiguration": {
                    #     "LambdaConfigurations": [{
                    #         "Function": {"Ref": "CopyS3RajivCloudF"},
                    #         "Event": "s3:ObjectCreated:*"
                    #
                    #     }]
                    # }
                }
            },

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

    try:
        response = client.delete_stack(
            StackName=stack_name
        )
        pprint('Change Set Deleted')
    except:
        pprint('ChangeSet did not exist')
        pass

    response = client.create_change_set(
        StackName=stack_name,
        TemplateBody=json.dumps(test_lambda_template),
        Capabilities=['CAPABILITY_IAM'],
        ChangeSetName='a',
        Description='Rajiv ChangeSet Description',
        ChangeSetType='CREATE'
    )

    pprint(response)
