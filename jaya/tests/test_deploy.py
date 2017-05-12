import unittest
# from jaya.pipeline.pipe import Leaf, Composite
from jaya.util.aws_lambda.aws_lambda_utils import CreateFileLambda
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

        info = deploy.create_deploy_stack(piper)
        expected_output = {deploy.LAMBDA: {},
                           deploy.S3: {'tsa-test-bucket1': {deploy.REGION_NAME: 'us-east-1'}},
                           deploy.S3_NOTIFICATION: {}}

        # deploy.deploy_stack_info(conf, info)
        self.assertEqual(expected_output, info)

    def test_s3_lambda(self):
        s1 = S3('tsa-lambda-bucket', 'us-east-1', on=[S3.ALL_CREATED_OBJECTS])
        l1 = CreateFileLambda()
        p = s1 >> l1
        piper = Pipeline('two-node-pipe', [p])
        info = deploy.create_deploy_stack(piper)

        print(dictify(info))

def dictify(a_dict):
    return json.loads(json.dumps(a_dict))


# TODO: Test empty tree

if __name__ == '__main__':
    unittest.main()
