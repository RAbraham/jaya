from jaya.pipeline.pipe import Leaf
from . import aws
import os
import sqlalchemy as sa

PYTHON36 = 'python3.6'
DEFAULT_COPY_OPTIONS = "format as json 'auto' gzip timeformat 'auto' truncatecolumns"


class S3(Leaf):
    ALL_CREATED_OBJECTS = 's3:ObjectCreated:*'

    def __init__(self, bucket, region_name, on=None):
        if not on:
            on = []
        assert type(on) == list, 'on should be of type list'

        # TODO: If region_name is made optional, then it should be US Standard or whatever the default is?
        self.service = Service(aws.S3)
        self.bucket = bucket
        self.region_name = region_name
        self.on = on
        super(S3, self).__init__([bucket, region_name, on])


class AWSLambda(Leaf):
    def __init__(self, name, handler, region_name, memory=1536, timeout=300, alias=None, dependencies=None,
                 runtime=PYTHON36, description='', virtual_environment_path=None):
        self.name = name
        self.handler = handler
        self.region_name = region_name
        self.memory = memory
        self.timeout = timeout
        self.service = Service(aws.LAMBDA)
        self.alias = alias
        self.runtime = runtime
        self.description = description
        # TODO: This should be mandatory. But we will later add a feature to take it from the Pipeline configuration anf fail only if that does not exist
        self.virtual_environment_path = virtual_environment_path
        if not dependencies:
            dependencies = []

        self.dependency_paths = [module_path(d) for d in dependencies]
        super(AWSLambda, self).__init__([self.name, self.handler, self.dependency_paths])


class Firehose(Leaf):
    # TODO: There are more parameters like Compression: Gzip etc. that we must consider.ƒ
    def __init__(self, firehose_name, database_name, user_name, user_password, server_address, table_name,
                 holding_bucket, role_name, copy_options=DEFAULT_COPY_OPTIONS, prefix=None, buffering_size_mb=128,
                 buffering_interval_seconds=900, log_group=None, log_stream='RedshiftDelivery'):
        self.service = Service(aws.FIREHOSE)
        self.firehose_name = firehose_name
        self.database_name = database_name
        self.user_name = user_name
        self.user_password = user_password
        self.server_address = server_address
        self.table_name = table_name
        self.holding_bucket = holding_bucket
        self.role_name = role_name
        self.copy_options = copy_options
        self.prefix = prefix
        self.buffering_size_mb = buffering_size_mb
        self.buffering_interval_seconds = buffering_interval_seconds
        self.log_group = log_group
        self.log_stream = log_stream
        pass


class Table(Leaf):
    def __init__(self, database_config, table_name, *columns, **keys):
        self.service = Service(aws.TABLE)
        self.database_config = database_config
        self.table_name = table_name

        self.metadata = sa.MetaData(bind=self.create_engine(database_config))

        self.table = sa.Table(table_name,
                              self.metadata,
                              *columns,
                              **keys)
        super(Table, self).__init__([database_config, table_name, columns, keys])

    @staticmethod
    def create_engine(config):
        config['db-dialect'] = config.get('db-dialect') or 'redshift+psycopg2'
        conn_str = "{db-dialect}://{db-user}:{db-passwd}@{db-server}:{db-port}/{db-name}"

        conn_str = conn_str.format(**config)

        return sa.create_engine(conn_str)

    def deploy(self):
        response = self.metadata.create_all(self.metadata.bind)
        print('Response')
        print(response)

    pass


class Service(Leaf):
    def __init__(self, service_name):
        self.name = service_name


class Pipeline(object):
    def __init__(self, name, pipes):
        self.name = name
        self.pipes = pipes


def module_path(file_or_dir_module_or_file_path):
    try:
        if os.path.isfile(file_or_dir_module_or_file_path):
            return file_or_dir_module_or_file_path
    except:
        if '__path__' in file_or_dir_module_or_file_path.__dict__:
            return file_or_dir_module_or_file_path.__path__[0]
        elif '__file__' in file_or_dir_module_or_file_path.__dict__:
            return file_or_dir_module_or_file_path.__file__
        else:
            raise ValueError('Is {} a module at all??'.format(str(file_or_dir_module_or_file_path)))
