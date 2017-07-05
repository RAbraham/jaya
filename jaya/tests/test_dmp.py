import unittest
# from jaya.pipeline.pipe import Leaf, Composite
from jaya.util.aws_lambda.aws_lambda_utils import MapS3ToFirehoseLambda, CopyS3Lambda
from jaya.core import S3, Pipeline, Firehose, Table
from jaya.deployment import deploy
from jaya.config import config
import gzip
import sqlalchemy as sa


def dmp_mapper(bucket, key, line):
    return [{'firehose_name': 'dmp_firehose',
             'result': [{'name': 'Rajiv', 'age': '65'}, {'name': 'Harry', 'age': '72'}]},
            ]


class DmpTestCase(unittest.TestCase):
    # Test for duplicates in pipeline

    def setUp(self):
        self.conf = config.get_aws_config('development')

    # def test_dmp(self):
    #     region = 'us-east-1'
    #     environment = 'development'
    #     dmp_source_s3 = S3('tsa-rajiv-dmp-source', region, on=[S3.ALL_CREATED_OBJECTS])
    #     in_process_s3 = S3('tsa-rajiv-dmp-in-process', region, on=[S3.ALL_CREATED_OBJECTS])
    #     mapper = MapS3ToFirehoseLambda(open_func=gzip.open,
    #                                    batch_size=499,
    #                                    map_function=dmp_mapper,
    #                                    memory=1536,
    #                                    timeout=300)
    #
    #     dmp_firehose = Firehose('dmp_firehose')
    #     database_config = {}
    #     schema = 'dmp'
    #     table = 'dmp_table'
    #     dmp_firehose_name = 'dmp_firehose'
    #     dmp_table = Table(
    #         database_config,
    #         schema,
    #         table,
    #         sa.Column('name', sa.String),
    #         sa.Column('age', sa.String),
    #         redshift_diststyle='KEY',
    #         redshift_distkey = 'name',
    #
    #     )
    #     p = dmp_source_s3 \
    #         >> CopyS3Lambda({}, region, environment) \
    #         >> in_process_s3 \
    #         >> mapper \
    #         >> dmp_firehose \
    #         >> dmp_table
    #
    #
    #     piper = Pipeline('dmp-trial', [p])
    #
    #     info = deploy.create_deploy_stack_info(piper)

    # def test_dmp_table(self):
    #     from jaya.config import config
    #     environment = 'development_remote'
    #     section_name = 'sports-development_remote'
    #     conf = config.get_db_conf(section_name)
    #     dmp_table = Table(
    #         conf,
    #         'dmp_table',
    #         sa.Column('name', sa.String),
    #         sa.Column('age', sa.String),
    #         schema='dmp_schema',
    #         redshift_diststyle='KEY',
    #         redshift_distkey='name',
    #
    #     )
    #
    #     piper = Pipeline('dmp-trial', [dmp_table])
    #     info = deploy.create_deploy_stack_info(piper)
    #     deploy.deploy_stack_info(conf, environment, info)

    # def test_dmp_firehose(self):
    #     from jaya.config import config
    #     environment = 'development_remote'
    #     section_name = 'sports-development_remote'
    #     conf = config.get_db_conf(section_name)
    #     aws_conf = config.get_aws_config(environment)
    #
    #     dmp_firehose = Firehose('rajiv_firehose',
    #                             conf['db-name'],
    #                             conf['db-user'],
    #                             conf['db-passwd'],
    #                             conf['db-server'],
    #                             'copy_test',
    #                             'thescore-firehose',
    #                             aws_conf['firehose_role'],
    #                             prefix='rajiv/test_jaya'
    #                             )
    #     piper = Pipeline('dmp-trial', [dmp_firehose])
    #     info = deploy.create_deploy_stack_info(piper)
    #     deploy.deploy_stack_info(aws_conf, environment, info)

    def test_dmp_firehose_lambda(self):
        mapper = MapS3ToFirehoseLambda(open_function=gzip.open,
                                       batch_size=499,
                                       map_function=dmp_mapper,
                                       memory=1536,
                                       timeout=300,
                                       region_name='us-east-1',
                                       alias='production')
        # piper = Pipeline('dmp-trial', [dmp_firehose])
        # info = deploy.create_deploy_stack_info(piper)
        # deploy.deploy_stack_info(aws_conf, environment, info)


# TODO: Test empty tree

if __name__ == '__main__':
    unittest.main()
