import unittest
# from jaya.pipeline.pipe import Leaf, Composite
from jaya.util.aws_lambda.aws_lambda_utils import EchoLambda, CopyS3Lambda
from jaya.core import S3, Pipeline
from jaya.deployment import deploy
from jaya.config import config
from pprint import pprint
import json
import io


class LocalAws1TestCase(unittest.TestCase):
    # Test for duplicates in pipeline

    def setUp(self):
        self.conf = config.get_aws_config('development')

    def test_multi_hop_node(self):
        region = 'us-east-1'
        environment = 'development'
        conf = config.get_aws_config(environment)

        bucket1 = 'tsa-rajiv-bucket1'
        bucket2 = 'tsa-rajiv-bucket2'
        p = S3(bucket1, region, on=[S3.ALL_CREATED_OBJECTS]) \
            >> CopyS3Lambda({}, region, environment) \
            >> S3(bucket2, 'us-east-1')

        piper = Pipeline('three-node-pipe', [p])

        in_memory_aws = create_in_memory_aws(piper)

        # # Test
        key = 'a_key'

        s3 = in_memory_aws.s3()
        act_file = io.StringIO('Rajiv here')

        s3.put(bucket1, key, act_file)
        exp_file = s3.get(bucket2, key)

        self.assertEqual(act_file.getvalue(), exp_file.getvalue())


class InMemoryAws(object):
    def __init__(self):
        pass

    def s3(self):

        pass

def create_in_memory_aws(a_piper):

    return InMemoryAws(s3)
    pass


def dictify(a_dict):
    return json.loads(json.dumps(a_dict))


# TODO: Test empty tree

if __name__ == '__main__':
    unittest.main()
