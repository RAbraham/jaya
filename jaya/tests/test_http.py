import unittest
from jaya.util.aws_lambda.aws_lambda_utils import Zappa
from jaya.core import Pipeline
from jaya.deployment import deploy
from jaya.config import config


class DmpTestCase(unittest.TestCase):
    # Test for duplicates in pipeline

    def setUp(self):
        self.conf = config.get_aws_config('development')

    # def test_dmp(self):
    #     environment = 'staging'
    #     section_name = 'sports-staging'
    #     db_conf = config.get_db_conf(section_name)
    #     aws_conf = config.get_aws_config(environment)
    #     region = aws_conf['aws_default_region']
    #     source_bucket = 'tsa-rajiv-dmp-source'
    #     in_process_bucket = 'tsa-rajiv-dmp-in-process'
    #     firehose_name = 'dmp_firehose'
    #     table = 'dmp_table'
    #     holding_bucket = 'thescore-firehose'
    #     schema = 'public'
    #
    #     firehose_s3_prefix = 'rajiv/test_jaya'
    #     ingestion_pipeline = thescore_ingestion_pipeline(environment,
    #                                                      aws_conf,
    #                                                      db_conf,
    #                                                      region,
    #                                                      source_bucket,
    #                                                      in_process_bucket,
    #                                                      dmp_mapper,
    #                                                      firehose_name,
    #                                                      holding_bucket,
    #                                                      firehose_s3_prefix,
    #                                                      schema,
    #                                                      table)
    #
    #     deploy.deploy_pipeline(aws_conf, environment, ingestion_pipeline)

    def test_http(self):
        index_func = lambda app: "Hello, world!", 200
        environment = 'staging'
        aws_conf = config.get_aws_config(environment)
        # p = Zappa(application_name='my_app',
        #           route_handler_pairs=[({"route": '/'}, index_func)]
        #           )
        p = Zappa(application_name='my_app',
                  route_handler_pairs=[({"route": '/'}, index_func)]
                  )
        pipeline = Pipeline('trial-http-app', [p])
        deploy.deploy_pipeline(aws_conf, environment, pipeline)


if __name__ == '__main__':
    unittest.main()
