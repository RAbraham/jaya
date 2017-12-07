import unittest
# from jaya.pipeline.pipe import Leaf, Composite
from jaya.util.aws_lambda.aws_lambda_utils import map_s3_to_firehose_lambda
from jaya.core import S3, Pipeline, Firehose, Table
from jaya.deployment import deploy
from jaya.config import config
import gzip
import sqlalchemy as sa

VIRTUAL_ENV_PATH = '/Users/rabraham/Documents/dev/thescore/analytics/jaya/venv/'
EXEC_ROLE = 'lambda_s3_exec_role'


def dmp_mapper(line, bucket, key):
    # TODO: environment + '_sports_' + 'etl_errors', has the value hardcoded for tracker errors firehose, change that.
    return [{'firehose_name': 'dmp_firehose',
             'result': [{'name': 'Rajiv', 'age': '65'}, {'name': 'Harry', 'age': '72'}]}

            ]


class DmpTestCase(unittest.TestCase):
    # Test for duplicates in pipeline

    def setUp(self):
        self.conf = config.get_aws_config('development')

    def test_dmp_simple(self):
        name = 'dmp-mapper'
        environment = 'staging'
        section_name = 'sports-staging'
        db_conf = config.get_db_conf(section_name)
        aws_conf = config.get_aws_config(environment)
        region = aws_conf['aws_default_region']
        in_process_bucket = 'tsa-rajiv-dmp-in-process'
        firehose_name = 'dmp_firehose'
        table = 'dmp_table'
        holding_bucket = 'thescore-firehose'
        schema = 'public'
        firehose_s3_prefix = 'rajiv/test_jaya'
        ingestion_pipeline = thescore_ingestion_pipeline_simple(name,
                                                                environment,
                                                                aws_conf,
                                                                db_conf,
                                                                region,
                                                                in_process_bucket,
                                                                dmp_mapper,
                                                                firehose_name,
                                                                holding_bucket,
                                                                firehose_s3_prefix,
                                                                schema,
                                                                table)

        # deploy.deploy_pipeline(aws_conf, environment, ingestion_pipeline)
        deploy.deploy_node(aws_conf, ingestion_pipeline, name)


def thescore_ingestion_pipeline_simple(name,
                                       environment,
                                       aws_conf,
                                       db_conf,
                                       region,
                                       in_process_bucket,
                                       mapping_function,
                                       firehose_name,
                                       holding_bucket,
                                       firehose_s3_prefix,
                                       schema,
                                       table):
    in_process_s3 = S3(in_process_bucket, region, on=[S3.ALL_CREATED_OBJECTS])

    mapper = map_s3_to_firehose_lambda(name,
                                       region,
                                       gzip.open,
                                       mapping_function,
                                       alias=environment,
                                       virtual_environment_path=VIRTUAL_ENV_PATH,
                                       role_name=EXEC_ROLE,
                                       )
    dmp_firehose = Firehose(firehose_name,
                            db_conf['db-name'],
                            db_conf['db-user'],
                            db_conf['db-passwd'],
                            db_conf['db-server'],
                            table,
                            holding_bucket,
                            aws_conf['firehose_role'],
                            prefix=firehose_s3_prefix,
                            buffering_interval_seconds=60
                            )
    dmp_table = Table(
        db_conf,
        table,
        sa.Column('name', sa.String),
        sa.Column('age', sa.String),
        schema=schema,
        redshift_diststyle='KEY',
        redshift_distkey='name',

    )
    p = in_process_s3 \
        >> mapper \
        >> dmp_firehose \
        >> dmp_table
    piper = Pipeline('dmp-trial', [p])
    return piper


if __name__ == '__main__':
    unittest.main()
