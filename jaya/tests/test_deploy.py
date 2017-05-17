import unittest
# from jaya.pipeline.pipe import Leaf, Composite
from jaya.util.aws_lambda.aws_lambda_utils import EchoLambda, CopyS3Lambda
from jaya.core import S3, Pipeline
from jaya.deployment import deploy
from jaya.config import config
from pprint import pprint
import json


class DeployTestCase(unittest.TestCase):
    # Test for duplicates in pipeline

    def setUp(self):
        self.conf = config.get_aws_config('development')

    def test_single_s3_bucket(self):
        p = S3('tsa-test-bucket1', 'us-east-1', on=[S3.ALL_CREATED_OBJECTS])

        piper = Pipeline('my-first', [p])

        info = deploy.create_deploy_stack_info(piper)
        expected_output = {deploy.LAMBDA: {},
                           deploy.S3: {'tsa-test-bucket1': {deploy.REGION_NAME: 'us-east-1'}},
                           deploy.S3_NOTIFICATION: {}}

        # deploy.deploy_stack_info(conf, info)
        self.assertEqual(expected_output, info)

    def test_s3_lambda(self):
        s1 = S3('tsa-rajiv-bucket', 'us-east-1', on=[S3.ALL_CREATED_OBJECTS])
        environment = 'development'
        l1 = EchoLambda('us-east-1', environment)
        p = s1 >> l1
        piper = Pipeline('two-node-pipe', [p])
        info = deploy.create_deploy_stack_info(piper)
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

def dictify(a_dict):
    return json.loads(json.dumps(a_dict))


# TODO: Test empty tree

if __name__ == '__main__':
    unittest.main()
