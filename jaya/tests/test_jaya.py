import unittest
from jaya.aws import S3
from jaya.aws.lib.aws_lambda import CopyS3Lambda
from pprint import pprint

TEST_BUCKET1 = 'tmp-rajiv-bucket1'
TEST_BUCKET2 = 'tmp-rajiv-bucket2'


class TempS3(object):
    def bucket(self):
        return TEST_BUCKET1


def test_dest_func(source_bucket, source_key):
    return source_bucket + '_new', source_key + '_new'


class JayaTestCase(unittest.TestCase):
    def test_copy_to_bucket(self):
        p = CopyS3Lambda({}) >> S3(bucket=TEST_BUCKET1)
        aws_lambda = p.root()
        expected_value = ['from lib import util',
                          'from lib import aws',
                          'from config import config',
                          "destination_bucket = 'tmp-rajiv-bucket1'",
                          'def copy_to_buckets(conf, source_bucket_key_pairs, dest_func):\n'
                          "    s3 = aws.resource(conf, 's3')\n"
                          '    for bucket, key in source_bucket_key_pairs:\n'
                          '        dest_bucket, dest_key = dest_func(bucket, key)\n'
                          '        o = s3.Object(dest_bucket, dest_key)\n'
                          "        o.copy_from(CopySource=bucket + '/' + key)\n",
                          'def destination_func(source_bucket, source_key):\n'
                          '    return destination_bucket, source_key\n',
                          'def handler(event, context):\n'
                          '    environment = util.get_arn_environment(context.invoked_function_arn)\n'
                          '    bucket_key_pairs = util.get_bucket_key_pairs_from_event(event)\n'
                          '    conf = config.get_aws_config(environment)\n'
                          '    copy_to_buckets(conf, bucket_key_pairs, destination_func)\n']

        self.assertEqual(expected_value, aws_lambda.code_as_strs())

    def test_copy_to_bucket_func(self):
        p = CopyS3Lambda({}) >> S3(destination_func=test_dest_func)
        aws_lambda = p.root()
        pprint(aws_lambda.code_as_strs())

        # def test_copy_to_multiple_buckets(self):
        #     p = CopyS3Lambda({}) >> [S3(bucket=TEST_BUCKET1), S3(bucket=TEST_BUCKET2)]
        #     aws_lambda = p.root()
        #     pprint(aws_lambda.code_as_strs())


# TODO: Test giving lambda function
# TODO: Test to multiple destinations

# TODO: Test copy to bucket, prefix

if __name__ == '__main__':
    unittest.main()
