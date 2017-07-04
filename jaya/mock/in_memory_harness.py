from jaya.deployment import deploy
from moto import mock_s3, mock_lambda
from jaya.deployment import deploy_lambda

def test(pipeline):
    return InMemoryHarness(pipeline)


# def deploy_stack_info_local(conf, environment, info):
#     s3_buckets = info[S3]
#     for bucket, bucket_info in s3_buckets.items():
#         aws_lib.create_s3_bucket(conf, bucket, bucket_info[REGION_NAME])
#
#     lambdas = info[LAMBDA]
#     for lambda_name, lambda_info in lambdas.items():
#         lambda_instance = lambda_info[LAMBDA_INSTANCE]
#         deploy_lambda.deploy_lambda_package_new(environment, lambda_info[LAMBDA_INSTANCE])
#         aws_lib.add_s3_notification_for_lambda(conf,
#                                                lambda_info[S3_SOURCE_BUCKET_NAME],
#                                                lambda_name,
#                                                environment,
#                                                prefix=lambda_info.get('prefix', None),
#                                                region_name=lambda_instance.region_name)

class InMemoryHarness(object):
    def __init__(self, pipeline):
        print('In Init')
        self.s3_mock = mock_s3()
        self.lambda_mock = mock_lambda()
        self.pipeline = pipeline

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    def start(self):
        print('In Start')
        self.s3_mock.start()
        self.lambda_mock.start()
        info = deploy.create_deploy_stack_info(self.pipeline)
        deploy.deploy_stack_info_local(info)

        pass

    def stop(self):
        print('In stop')
        self.s3_mock.stop()
        self.lambda_mock.stop()
        pass

    def s3(self):
        return S3Mock(aws.resource(conf, 's3'))


class S3Mock(object):
    def __init__(self, s3_service):
        self.s3_service = s3_service

        pass

    def hi(self):
        print('Hi')

    def __getattr__(self, operation):
        def method(*args, **kwargs):
            result = getattr(self.s3_service, operation)(*args, **kwargs)
            self.notify_observers(operation, args, kwargs)
            return result

        return method

    def notify_observers(self, operation, args, kwargs):
        for observer in self.observers_for(operation, args, kwargs):
            observer.notify()

    def observers_for(self, operation, args, kwargs):
        # TODO: build suffix filter support
        observers = []
        bucket = kwargs['Bucket']
        key = kwargs['Key']
        filters = self.filters[operation][bucket]

        for s3_filter in filters:
            prefix = s3_filter['prefix']
            _observers = s3_filter['observers']
            if prefix == 'all':
                # TODO: What if an actual prefix is 'all' :)?
                observers.extend(_observers)
            elif key.startswith(prefix):
                observers.extend(_observers)

        return observers


def handler_func(event, context):
    from datetime import datetime
    file = '/Users/rabraham/dev-thescore/analytics/jaya/tmp/rajiv_tries.txt'
    with open(file, 'a') as f:
        f.write('\nHello Rajiv:' + str(datetime.utcnow()))


if __name__ == '__main__':
    import io
    import boto3
    from jaya.lib import aws


    # ***********
    lambda_mock = mock_lambda()

    lambda_mock.start()
    from jaya.core import AWSLambda
    import json

    lambda_name = 'print-lambda'
    us_east = 'us-east-1'
    print_lambda = AWSLambda(lambda_name, handler_func, us_east)
    deploy_lambda.deploy_lambda_package_local(print_lambda)
    conn = boto3.client('lambda', us_east)
    # success_result = conn.invoke(FunctionName=lambda_name,
    #                              InvocationType='RequestResponse',
    #                              Payload=json.dumps({'hi': 'Rajiv'}))
    lambda_mock.stop()
    # ***********
    # conf = {'aws_key': None, "aws_id": None}
    # s3_jaya = S3Mock(aws.client(conf, 's3'))
    #
    # s3_mock = mock_s3()
    # s3_mock.start()
    #
    # file_content = io.BytesIO(b'Hi Rajiv')
    # source = 'tsa-rajiv-bucket1'
    # a_key = 'a_key'
    # s3_jaya.create_bucket(Bucket=source)
    # s3_jaya.put_object(Bucket=source, Key=a_key, Body=file_content)
    # response = s3_jaya.get_object(Bucket=source, Key=a_key)
    # data = response['Body'].read()
    # print('Data')
    # print(data)
    # s3_mock.stop()

    # ***************************************
    # conf = {'aws_key': None, "aws_id": None}
    # s3_jaya = S3Mock(aws.resource(conf, 's3'))
    #
    #
    # s3_mock = mock_s3()
    # s3_mock.start()
    #
    # file_content = io.BytesIO(b'Hi Rajiv')
    # source = 'tsa-rajiv-bucket1'
    # a_key = 'a_key'
    # aws.create_s3_bucket(conf, source, 'us-east-1')
    # s3_jaya.Bucket(source).put_object(Key=a_key, Body=file_content)
    # obj = s3_jaya.Object(bucket_name=source, key=a_key)
    # response = obj.get()
    # data = response['Body'].read()
    # print('Data')
    # print(data)
    # s3_mock.stop()

    # ******************************
    # s3_mock = mock_s3()
    # s3_mock.start()
    # s3 = aws.resource(conf, 's3')
    # file_content = io.BytesIO(b'Hi Rajiv')
    # source = 'tsa-rajiv-bucket1'
    # destination = 'tsa-rajiv-bucket2'
    # a_key = 'a_key'
    # aws.create_s3_bucket(conf, source, 'us-east-1')
    # s3.Bucket(source).put_object(Key=a_key, Body=file_content)
    # obj = s3.Object(bucket_name=source, key=a_key)
    # response = obj.get()
    # data = response['Body'].read()
    # print('Data')
    # print(data)
    # s3_mock.stop()
    # # ******************************
