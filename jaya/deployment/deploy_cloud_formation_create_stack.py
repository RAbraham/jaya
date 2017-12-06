import sys
import os

import time

stack_name = 'RajivTestStackLambda'

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


def cf_wait_until_not_in_stack_status(stack_resource, status, sleep_time_seconds=30):
    while stack_resource.stack_status == status:
        time.sleep(sleep_time_seconds)
        stack_resource.reload()
        print(stack_resource.stack_status)


if __name__ == '__main__':
    region = 'us-east-1'

    lambda_resource_name = "CopyRajiv"
    alias_resource_name = "AliasForMyApp"
    s3_resource_name = 'SrcBucket'
    lambda_name = 'CopyCloudFormation'
    test_lambda_template = {
        "AWSTemplateFormatVersion": "2010-09-09",

        "Resources": {
            lambda_resource_name: {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {
                        "S3Bucket": "thescore-tmp",
                        "S3Key": "CopyS3Lambda"
                    },
                    # "CodeUri": 's3://thescore-tmp/CopyS3Lambda',
                    "FunctionName": lambda_name,
                    "Handler": 'lambda.handler',
                    "Runtime": 'python3.6',
                    "Timeout": 300,
                    "Role": 'arn:aws:iam::027995586716:role/lambda_s3_exec_role',

                }
            },

            # alias_resource_name: {
            #     "Type": "AWS::Lambda::Alias",
            #     "Properties": {
            #         # "FunctionArn": {"Ref": lambda_resource_name},
            #         "FunctionName": lambda_name,
            #         "FunctionVersion": "$LATEST",
            #         "Name": "staging"
            #     }
            # },

            # s3_resource_name: {
            #     "Type": "AWS::S3::Bucket",
            #     "Properties": {
            #         "BucketName": 'thescore-cloudfront-trial',
            #         "NotificationConfiguration": {
            #             "LambdaConfigurations": [{
            #                 "Function": {"Ref": alias_resource_name},
            #                 "Event": "s3:ObjectCreated:*"
            #
            #             }]
            #         }
            #     }
            # },

            s3_resource_name: {
                "Type": "AWS::S3::Bucket",
                "DependsOn": lambda_resource_name,
                "Properties": {
                    "BucketName": 'thescore-cloudfront-trial',
                    # "NotificationConfiguration": {
                    #     "LambdaConfigurations": [{
                    #         "Function": {"Ref": lambda_resource_name},
                    #         "Event": "s3:ObjectCreated:*"
                    #
                    #     }]
                    # }
                }
            },

            # "LambdaInvokePermission": {
            #     "Type": "AWS::Lambda::Permission",
            #     "Properties": {
            #         "FunctionName": {"Fn::GetAtt": [alias_resource_name, "Arn"]},
            #         "Action": "lambda:InvokeFunction",
            #         "Principal": "s3.amazonaws.com",
            #         "SourceArn": {"Ref": s3_resource_name}
            #     }
            # }

        }

    }

    update_stack_template = {
        "AWSTemplateFormatVersion": "2010-09-09",

        "Resources": {
            lambda_resource_name: {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {
                        "S3Bucket": "thescore-tmp",
                        "S3Key": "CopyS3Lambda"
                    },
                    "FunctionName": lambda_name,
                    "Handler": 'lambda.handler',
                    "Runtime": 'python3.6',
                    "Timeout": 300,
                    "Role": 'arn:aws:iam::027995586716:role/lambda_s3_exec_role',

                }
            },

            s3_resource_name: {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": 'thescore-cloudfront-trial',
                    # "NotificationConfiguration": {
                    #     "LambdaConfigurations": [{
                    #         "Function": {"Ref": lambda_resource_name},
                    #         "Event": "s3:ObjectCreated:*"
                    #
                    #     }]
                    # }
                }
            },
            # alias_resource_name: {
            #     "Type": "AWS::Lambda::Alias",
            #     "Properties": {
            #         # "FunctionArn": {"Ref": lambda_resource_name},
            #         "FunctionName": lambda_name,
            #         "FunctionVersion": "$LATEST",
            #         "Name": "staging"
            #     }
            # },



            "LambdaInvokePermission": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "FunctionName": {"Fn::GetAtt": [lambda_resource_name, "Arn"]},
                    "Action": "lambda:InvokeFunction",
                    "Principal": "s3.amazonaws.com",
                    "SourceAccount": {"Ref": "AWS::AccountId"},
                    # "SourceArn": {"Ref": s3_resource_name}
                    # "SourceArn": {"Fn::GetAtt": [s3_resource_name, "Arn"]}
                    "SourceArn": {"Fn::Join": [":", [
                        "arn", "aws", "s3", "", "", {"Ref": s3_resource_name}]]
                                  }
                }
            },

        }

    }

    update_stack_template1 = {
        "AWSTemplateFormatVersion": "2010-09-09",

        "Resources": {
            # lambda_resource_name: {
            #     "Type": "AWS::Lambda::Function",
            #     "Properties": {
            #         "Code": {
            #             "S3Bucket": "thescore-tmp",
            #             "S3Key": "CopyS3Lambda"
            #         },
            #         "FunctionName": lambda_name,
            #         "Handler": 'lambda.handler',
            #         "Runtime": 'python3.6',
            #         "Timeout": 300,
            #         "Role": 'arn:aws:iam::027995586716:role/lambda_s3_exec_role',
            #
            #     }
            # },

            s3_resource_name: {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": 'thescore-cloudfront-trial',
                    "NotificationConfiguration": {
                        "LambdaConfigurations": [{
                            # "Function": {"Ref": lambda_resource_name},
                            "Function": {"Fn::GetAtt": [lambda_resource_name, "Arn"]},
                            # 'Function': {"Fn::GetAtt": [lambda_resource_name, "Arn"]},
                            # "Function": lambda_name,
                            "Event": "s3:ObjectCreated:*"

                        }]
                    }
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
    cloudformation = aws.resource(conf, 'cloudformation', region_name=region)
    stack = cloudformation.Stack(stack_name)
    try:
        response = client.delete_stack(
            StackName=stack_name
        )
        pprint('Stack Set Deleted')

        print('Just Relax')
        time.sleep(35)
    except:
        pprint('Stack Set did not exist')
    pass

    # cf_wait_until_not_in_stack_status(stack, 'DELETE_IN_PROGRESS')
    response = client.create_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(test_lambda_template),

        # ResourceTypes=[
        #     "AWS::Lambda::Function",
        #     "AWS::S3::Bucket"
        # ],

    )

    # pprint(response)

    import boto3
    #
    cf_wait_until_not_in_stack_status(stack, 'CREATE_IN_PROGRESS')
    #
    print('Outside Loop')
    print(stack.stack_status)

    # Update Stack
    response = client.update_stack(StackName=stack_name,
                                   TemplateBody=json.dumps(update_stack_template))

    print('After Update')
    stack.reload()
    print(stack.stack_status)

    pprint(response)