from typing import Dict
import os
import boto3
import argparse
import jaya  # Needed to give as dependencies below
from jaya import S3, Pipeline, AWSLambda, LambdaHandler
from jaya.deployment import deploy, jaya_deploy


class CopyHandler(LambdaHandler):
    def __init__(self, config):
        # Pass custom values (.e.g config) when the pipeline is specified and it will be
        # saved to be accessed during the actual lambda execution in the handler method.
        self.config = config
        super().__init__()

    def handler(self, event, context):
        # Get the bucket, key which triggered the event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = event['Records'][0]['s3']['key']['name']

        # jaya populates a self.jaya_context object with the downstream S3 bucket which was specified in the pipeline.
        # For simplicity, we just read the first child.
        destination_bucket = self.jaya_context.children()[0].bucket_name

        # Copy the file from source to destination. Note the use of self.config which was set at pipeline
        # specification
        copy(self.config,
             source_bucket,
             source_key,
             destination_bucket,
             source_key)


def copy(conf, source_bucket, source_key, destination_bucket, destination_key):
    s3 = resource(conf, 's3')
    o = s3.Object(destination_bucket, destination_key)
    o.copy_from(CopySource=source_bucket + '/' + source_key)


def resource(conf, resource_name, region_name='us-east-1'):
    session = boto3.session.Session(aws_access_key_id=conf['aws_access_key_id'],
                                    aws_secret_access_key=conf['aws_secret_access_key'],
                                    region_name=region_name)
    return session.resource(resource_name)


def pipeline(args: Dict[str, str], os_environ):
    region_name = args['region_name']

    handler = CopyHandler(args)
    copy_lambda = AWSLambda('jaya_copy_lambda',
                            handler,
                            region_name,
                            virtual_environment_path=os_environ['VIRTUAL_ENV'],
                            role_name=args['role'],
                            description="This project was inspired by a woman who codes",
                            dependencies=[jaya])

    s1 = S3(bucket_name=args['source_bucket'],
            region_name=region_name,
            events=[S3.event(S3.ALL_CREATED_OBJECTS)])

    s2 = S3(bucket_name=args['destination_bucket'], region_name=region_name)
    p = s1 >> copy_lambda >> s2

    piper = Pipeline('JayaCopyPipeline', [p])
    return piper


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--source_bucket",
                        required=True,
                        help="Bucket in which a file is placed to copy")
    parser.add_argument("--destination_bucket",
                        required=True,
                        help='Bucket to which file will be copied')
    parser.add_argument("--region_name",
                        required=True,
                        help="e.g. 'us-east-1'"
                        )
    parser.add_argument("--role",
                        required=True,
                        help="An existing role which has access to S3, Lambda etc'"
                        )
    parser.add_argument('--aws_access_key_id',
                        required=True,
                        help='Your aws_access_key_id')
    parser.add_argument('--aws_secret_access_key',
                        required=True,
                        help="Your aws_secret_access_key"
                        )

    return parser.parse_args().__dict__


if __name__ == '__main__':
    parsed_args = parse_args()
    p = pipeline(parsed_args, os.environ)

    aws_conf = dict(aws_id=parsed_args['aws_access_key_id'],
                    aws_key=parsed_args['aws_secret_access_key'])

    deploy.deploy_pipeline(aws_conf, p)
