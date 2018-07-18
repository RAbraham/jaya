from functools import partial
import jaya
from jaya.lib import util
from jaya.services import AWSLambda, LambdaHandler
from functools import partial
from typing import List

DEFAULT_REGION = 'us-east-1'


# def echo_handler_lambda(lambda_name, environment, echo_str):
#     echo_handler_with_str = partial(echo_handler, echo_str)
#     return AWSLambda(lambda_name,
#                      echo_handler_with_str,
#                      DEFAULT_REGION,
#                      alias=environment,
#                      virtual_environment_path=None,
#                      role_name=None,
#                      dependencies=[jaya])

class EchoHandler(LambdaHandler):
    def __init__(self, echo_str):
        self.echo_str = echo_str
        self.count = None
        super().__init__()

    def initialize(self):
        self.count = 100
        pass

    def handler(self, event, context):
        import sys
        import datetime
        print(sys.version)
        print('Count')
        print(self.count)
        self.count = self.count + 1
        print(self.echo_str + ':' + str(datetime.datetime.utcnow().isoformat()))
        print(event)
        pass


def echo_handler_lambda(lambda_name, environment, echo_str):
    return AWSLambda(lambda_name,
                     EchoHandler('Handler Class Echo'),
                     DEFAULT_REGION,
                     alias=environment,
                     virtual_environment_path=None,
                     role_name=None,
                     dependencies=[jaya])


class CopyHandler(LambdaHandler):
    def __init__(self, aws_config):
        self.aws_config = aws_config
        super().__init__()

    def handler(self, event, context):
        print('Self Configuration Size:')
        print(len(self.aws_config))  # or print any value

        # bucket_key_pairs = get_bucket_key_pairs_from_event(event)
        destination_buckets = [s3_child.bucket_name for s3_child in self.jaya_context.children()]


def copy_to_buckets(conf, bucket_key_pairs, dest_func):
    from jaya.lib import aws
    for bucket, key in bucket_key_pairs:
        dest_bucket, dest_key = dest_func(bucket, key)
        print('Rajiv: Dest Bucket:' + dest_bucket)
        print('Rajiv: Dest Key:' + dest_key)
        aws.copy_from_s3_to_s3(conf, bucket, key, dest_bucket, dest_key)


# def copy_handler(configuration, nodes, event, context):
#     def make_dest_func(s3_child):
#         return lambda b, k: tuple([s3_child.bucket, k])
#
#     dest_funcs = [make_dest_func(child) for child in nodes]
#     # environment = util.get_arn_environment(context.invoked_function_arn)
#
#     conf = configuration
#     for dest_func in dest_funcs:
#         bucket_key_pairs = util.get_bucket_key_pairs_from_event(event)
#         copy_to_buckets(conf, bucket_key_pairs, dest_func)


def email_handler(source_address: str, email_addresses: List[str], subject: str, smtp_server: str, event_transformer,
                  jaya_context, event, context):
    import smtplib, os
    from email.mime.multipart import MIMEMultipart
    from email.encoders import encode_base64
    from mimetypes import guess_type

    msg = MIMEMultipart()
    smtp_server = smtp_server or 'smtp.gmail.com:587'
    msg['Subject'] = subject
    msg['From'] = source_address

    msg['To'] = 'user@gmail.com'
    email_from = source_address
    email_to = "user@gmail.com"
    s = smtplib.SMTP(smtp_server)
    s.starttls()
    s.login('user@gmail.com', 'password')
    response = s.sendmail(email_from, email_to, msg.as_string())
    s.quit()
