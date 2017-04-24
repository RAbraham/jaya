import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from jaya.config import config
from jaya.lib import util
from jaya.deployment.deploy_lambda import deploy_lambda
from jaya.lib import aws

src_root = config.project_root()
py2_handler = src_root + '/aws_lambda/handler.py'
py3_handler = src_root + '/aws_lambda/handler_python3.py'
venv3_path = '/Users/rabraham/Documents/dev/thescore/analytics/jaya/jaya/aws_lambda/linux_venv/'
venv3_path_site_packages = venv3_path + 'lib/python3.6/site-packages'
python3_packages = util.get_children(venv3_path_site_packages)

lambda_name = 'test_py3_runner'
conf = config.get_aws_config('production')
aws.delete_lambda(conf, lambda_name, 'us-east-1')
deploy_lambda('production',
              lambda_name,
              [py2_handler, py3_handler, venv3_path] + python3_packages,
              'lambda_s3_exec_role',
              128,
              300,
              lambda_description='Test for Python 3',
              alias_description='Alias for ' + 'production',
              update=True,
              handler_name="handler",
              region_name='us-east-1')
