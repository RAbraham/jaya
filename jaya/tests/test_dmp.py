import unittest
# from jaya.pipeline.pipe import Leaf, Composite
from jaya.util.aws_lambda.aws_lambda_utils import MapS3ToFirehoseLambda, CopyS3Lambda
from jaya.core import S3, Pipeline, Firehose, Table
from jaya.deployment import deploy
from jaya.config import config
import gzip
import sqlalchemy as sa


def dmp_mapper(line, bucket, key):
    # TODO: environment + '_sports_' + 'etl_errors', has the value hardcoded for tracker errors firehose, change that.
    print('Jaya: In DMP Mapper!')
    return [{'firehose_name': 'dmp_firehose',
             'result': [{'name': 'Rajiv', 'age': '65'}, {'name': 'Harry', 'age': '72'}]}

            ]


class DmpTestCase(unittest.TestCase):
    # Test for duplicates in pipeline

    def setUp(self):
        self.conf = config.get_aws_config('development')

    def test_dmp(self):
        region = 'us-east-1'
        environment = 'staging'
        dmp_source_s3 = S3('tsa-rajiv-dmp-source', region, on=[S3.ALL_CREATED_OBJECTS])
        in_process_s3 = S3('tsa-rajiv-dmp-in-process', region, on=[S3.ALL_CREATED_OBJECTS])
        section_name = 'sports-staging'
        conf = config.get_db_conf(section_name)
        aws_conf = config.get_aws_config(environment)
        mapper = MapS3ToFirehoseLambda(open_function=gzip.open,
                                       map_function=dmp_mapper,
                                       batch_size=499,
                                       memory=1536,
                                       timeout=300,
                                       region_name='us-east-1',
                                       alias='staging')

        dmp_firehose = Firehose('dmp_firehose',
                                conf['db-name'],
                                conf['db-user'],
                                conf['db-passwd'],
                                conf['db-server'],
                                'dmp_table',
                                'thescore-firehose',
                                aws_conf['firehose_role'],
                                prefix='rajiv/test_jaya',
                                buffering_interval_seconds=60
                                )

        dmp_table = Table(
            conf,
            'dmp_table',
            sa.Column('name', sa.String),
            sa.Column('age', sa.String),
            schema='public',
            redshift_diststyle='KEY',
            redshift_distkey='name',

        )

        p = dmp_source_s3 \
            >> CopyS3Lambda({}, region, environment) \
            >> in_process_s3 \
            >> mapper \
            >> dmp_firehose \
            >> dmp_table

        piper = Pipeline('dmp-trial', [p])

        info = deploy.create_deploy_stack_info(piper)
        deploy.deploy_stack_info(aws_conf, environment, info)


# TODO: Test empty tree

if __name__ == '__main__':
    unittest.main()
