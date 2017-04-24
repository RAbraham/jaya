import sys
import os
import marol

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from jaya.config import config
from jaya.deployment.deploy_lambda import deploy_lambda
from jaya.lib import aws

src_root = config.project_root()
py3_handler = src_root + '/aws_lambda/test_do_etl.py'

lambda_name = 'test_py3_runner'
conf = config.get_aws_config('production')
# aws.delete_lambda(conf, lambda_name, 'us-east-1')
deploy_lambda('production',
              lambda_name,
              marol.get_lambda_files(py3_handler, '3.6.0'),
              'lambda_s3_exec_role',
              128,
              300,
              lambda_description='Test for Python 3',
              alias_description='Alias for ' + 'production',
              update=True,
              handler_name="handler",
              region_name='us-east-1')

# print('Zipping')
# zip_project(py3_handler, '/Users/rabraham/dev-thescore/analytics/jaya/venv', [], '~/tmp/zip-only.zip')