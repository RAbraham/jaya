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
        environment = 'staging'
        section_name = 'sports-staging'
        db_conf = config.get_db_conf(section_name)
        aws_conf = config.get_aws_config(environment)
        region = aws_conf['aws_default_region']
        source_bucket = 'tsa-rajiv-dmp-source'
        in_process_bucket = 'tsa-rajiv-dmp-in-process'
        firehose_name = 'dmp_firehose'
        table = 'dmp_table'
        holding_bucket = 'thescore-firehose'
        schema = 'public'

        firehose_s3_prefix = 'rajiv/test_jaya'
        ingestion_pipeline = thescore_ingestion_pipeline(environment,
                                                         aws_conf,
                                                         db_conf,
                                                         region,
                                                         source_bucket,
                                                         in_process_bucket,
                                                         dmp_mapper,
                                                         firehose_name,
                                                         holding_bucket,
                                                         firehose_s3_prefix,
                                                         schema,
                                                         table)

        # ingestion_pipeline = thescore_ingestion_pipeline(environment,
        #                                                  aws_conf,
        #                                                  db_conf,
        #                                                  region,
        #                                                  'bing_source_bucket',
        #                                                  'bing_in_process_bucket',
        #                                                  bing_complex_function,
        #                                                  'bing_firehose',
        #                                                  holding_bucket,
        #                                                  'bing/loves/tennis',
        #                                                  schema,
        #                                                  'bing_table')
        deploy.deploy_pipeline(aws_conf, environment, ingestion_pipeline)


def thescore_ingestion_pipeline(environment,
                                aws_conf,
                                db_conf,
                                region,
                                source_bucket,
                                in_process_bucket,
                                mapping_function,
                                firehose_name,
                                holding_bucket,
                                firehose_s3_prefix,
                                schema,
                                table):
    dmp_source_s3 = S3(source_bucket, region, on=[S3.ALL_CREATED_OBJECTS])
    in_process_s3 = S3(in_process_bucket, region, on=[S3.ALL_CREATED_OBJECTS])
    mapper = MapS3ToFirehoseLambda(open_function=gzip.open,
                                   map_function=mapping_function,
                                   batch_size=499,
                                   memory=1536,
                                   timeout=300,
                                   region_name=region,
                                   alias=environment)

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
    p = dmp_source_s3 \
        >> CopyS3Lambda({}, region, environment) \
        >> in_process_s3 \
        >> mapper \
        >> dmp_firehose \
        >> dmp_table
    piper = Pipeline('dmp-trial', [p])
    return piper


# TODO: Test empty tree

if __name__ == '__main__':
    unittest.main()
