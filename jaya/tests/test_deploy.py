import unittest
# from jaya.pipeline.pipe import Leaf, Composite
from jaya.util.aws_lambda.aws_lambda_utils import EchoLambda, CopyS3Lambda
from jaya.core import S3, Pipeline
from jaya.deployment import deploy
from jaya.config import config
from pprint import pprint
import json
import io
from localstack.mock import infra
from jaya.mock.in_memory_harness import test

class DeployTestCase(unittest.TestCase):
    # Test for duplicates in pipeline

    def setUp(self):
        # infra.start_infra(async=True, apis=['s3'])

        self.conf = config.get_aws_config('development')

    def tearDown(self):
        # infra.stop_infra()
        pass

    def test_single_s3_bucket(self):
        p = S3('tsa-test-bucket1', 'us-east-1', on=[S3.ALL_CREATED_OBJECTS])

        piper = Pipeline('my-first', [p])

        info = deploy.create_deploy_stack_info(piper)
        expected_output = {deploy.LAMBDA: {},
                           deploy.S3: {'tsa-test-bucket1': {deploy.REGION_NAME: 'us-east-1'}},
                           deploy.S3_NOTIFICATION: {}}

        # deploy.deploy_stack_info(conf, info)
        self.assertEqual(expected_output, info)

        # def test_s3_lambda(self):
        #     s1 = S3('tsa-rajiv-bucket', 'us-east-1', on=[S3.ALL_CREATED_OBJECTS])
        #     environment = 'development'
        #     l1 = EchoLambda('us-east-1', environment)
        #     p = s1 >> l1
        #     piper = Pipeline('two-node-pipe', [p])
        #     info = deploy.create_deploy_stack_info(piper)
        # pprint(info)

        # expected_output = {'lambda': {'EchoLambda': {'s3_source_bucket': 'tsa-lambda-bucket'}},
        #                    's3': {'tsa-lambda-bucket': {'region_name': 'us-east-1'}},
        #                    's3_notification': {'tsa-lambda-bucket': [{'lambda_name': 'EchoLambda',
        #                                                               'triggers': ['s3:ObjectCreated:*']}]}}
        # self.assertEqual(expected_output, info)

        # conf = config.get_aws_config(environment)
        # deploy.deploy_stack_info(conf, environment, info)


        # def test_s3_lambda(self):
        #     s1 = S3('tsa-lambda-bucket', 'us-east-1', on=[S3.ALL_CREATED_OBJECTS])
        #     s2 = S3('tsa-lambda-dest-bucket', 'us-east-1')
        #     l1 = CopyS3Lambda({})
        #     p = s1 >> l1 >> s2
        #     piper = Pipeline('two-node-pipe', [p])
        #     info = deploy.create_deploy_stack(piper)
        #
        #     print(dictify(info))

    # def test_s3_lambda_s3(self):
    #     region = 'us-east-1'
    #     environment = 'development'
    #     conf = config.get_aws_config(environment)
    #     s1 = S3('tsa-rajiv-bucket1', region, on=[S3.ALL_CREATED_OBJECTS])
    #     l1 = CopyS3Lambda({}, region, environment)
    #     s2 = S3('tsa-rajiv-bucket2', 'us-east-1')
    #     p = s1 >> l1 >> s2
    #     # pprint(p)
    #     piper = Pipeline('three-node-pipe', [p])
    #     info = deploy.create_deploy_stack_info(piper)
    #     # pprint(info)
    #
    #     # deploy.deploy_stack_info(conf, environment, info)

    # def test_multi_hop_node(self):
    #     region = 'us-east-1'
    #     environment = 'development'
    #     conf = config.get_aws_config(environment)
    #
    #     p = S3('tsa-rajiv-bucket1', region, on=[S3.ALL_CREATED_OBJECTS]) \
    #         >> CopyS3Lambda({}, region, environment) \
    #         >> S3('tsa-rajiv-bucket2', 'us-east-1')
    #
    #     piper = Pipeline('three-node-pipe', [p])
    #     info = deploy.create_deploy_stack_info(piper)
    #
    #     deploy.deploy_stack_info(conf, environment, info)

    # def test_multi_hop_node_localstack(self):
    #     region = 'us-east-1'
    #     environment = 'development'
    #     conf = config.get_aws_config(environment)
    #
    #     p = S3('tsa-rajiv-bucket1', region, on=[S3.ALL_CREATED_OBJECTS]) \
    #         >> CopyS3Lambda({}, region, environment) \
    #         >> S3('tsa-rajiv-bucket2', 'us-east-1')
    #
    #     piper = Pipeline('three-node-pipe', [p])
    #     info = deploy.create_deploy_stack_info(piper)
    #
    #     deploy.deploy_stack_info_localstack(conf, environment, info)

    def test_multi_hop_node_moto(self):
        region = 'us-east-1'
        environment = 'development'
        conf = config.get_aws_config(environment)

        source = 'tsa-rajiv-bucket1'
        destination = 'tsa-rajiv-bucket2'
        p = S3(source, region, on=[S3.ALL_CREATED_OBJECTS]) \
            >> CopyS3Lambda({}, region, environment) \
            >> S3(destination, 'us-east-1')

        piper = Pipeline('three-node-pipe', [p])

        # with test(piper) as test_harness:
        #     s3 = test_harness.s3()
        #     a_key = 'a_key'
        #     file_content = io.BytesIO(b'Hi Rajiv')
        #     s3.Bucket(source).put_object(Key=a_key, Body=file_content)
        #     obj = s3.Object(bucket_name=destination, key=a_key)
        #     response = obj.get()
        #     data = response['Body'].read()
        #     self.assertEqual(data, file_content.getvalue())

        with test(piper) as test_harness:
            s3 = test_harness.s3()
            a_key = 'a_key'
            file_content = io.BytesIO(b'Hi Rajiv')
            s3.Bucket(source).put_object(Key=a_key, Body=file_content)
            obj = s3.Object(bucket_name=source, key=a_key)
            response = obj.get()
            data = response['Body'].read()
            self.assertEqual(data, file_content.getvalue())

        # info = deploy.create_deploy_stack_info(piper)
        #
        # deploy.deploy_stack_info_localstack(conf, environment, info)

    # def test_localstack(self):
    #     from localstack.utils.aws import aws_stack
    #     s3 = aws_stack.connect_to_resource('s3')
    #     bucket = 'test_bucket_lambda'
    #     key = 'test_lambda.zip'
    #     s3.create_bucket(Bucket=bucket)
    #     file_content = io.BytesIO(b'Hi rajiv')
    #     s3.Bucket(bucket).put_object(Key=key, Body=file_content)
    #
    #     obj = s3.Object(bucket_name=bucket, key=key)
    #     response = obj.get()
    #     data = response['Body'].read()
    #     self.assertEqual(data, file_content.getvalue())




def dictify(a_dict):
    return json.loads(json.dumps(a_dict))


# TODO: Test empty tree

if __name__ == '__main__':
    unittest.main()
