import jaya
import sajan

from sajan import Leaf, Composite, Tree, Service
from sajan import util as sajan_util
from jaya.core import aws
import sqlalchemy as sa
from typing import Callable, List, Dict, TypeVar
from functools import partial

PYTHON36 = 'python3.6'
DEFAULT_COPY_OPTIONS = "format as json 'auto' gzip timeformat 'auto' truncatecolumns"
C = TypeVar('C')

HANDLER_SIGNATURE = Callable[[List[Leaf], Dict, C], None]


def event(trigger, prefix=None, suffix=None, service_name=None):
    assert trigger is not None, 'Trigger is mandatory'
    assert service_name is not None, "Named argument 'service_name' is mandatory"
    return {'service_name': service_name,
            'trigger': trigger,
            'prefix': prefix,
            'suffix': suffix
            }


class S3(Service):
    ALL_CREATED_OBJECTS = 's3:ObjectCreated:*'

    def __init__(self, bucket, region_name, on: List[Dict[str, str]] = None):
        if not on:
            on = []
        assert type(on) == list, 'on should be of type list'

        # TODO: If region_name is made optional, then it should be US Standard or whatever the default is?
        self.bucket = bucket
        self.region_name = region_name
        self.on = on
        super(S3, self).__init__(service_name=aws.S3,
                                 bucket=bucket,
                                 region_name=region_name,
                                 on=on)


class AWSLambda(Service):
    def __init__(self,
                 name,
                 handler_func: HANDLER_SIGNATURE,
                 region_name,
                 memory=1536,
                 timeout=300,
                 alias=None,
                 dependencies=None,
                 runtime=PYTHON36,
                 description='',
                 virtual_environment_path=None,
                 role_name=None,
                 tracing_config=None):
        self.name = name
        self.handler_func = handler_func
        self.handler = partial(self.handler_func, [])
        self.region_name = region_name
        self.memory = memory
        self.timeout = timeout
        # self.service = Service(aws.LAMBDA)
        self.alias = alias
        self.runtime = runtime
        self.description = description
        # TODO: This should be mandatory. But we will later add a feature to take it from the Pipeline configuration anf fail only if that does not exist
        self.virtual_environment_path = virtual_environment_path
        if not dependencies:
            dependencies = []
        dependencies.extend([sajan, jaya])

        self.dependency_paths = [sajan_util.module_path(d) for d in dependencies]
        self.role_name = role_name

        self.tracing_config = tracing_config or {'Mode': 'PassThrough'}
        # super(AWSLambda, self).__init__([self.name, self.handler_func, self.dependency_paths])
        # TODO: Pass all lambda values?
        super(AWSLambda, self).__init__(service_name=aws.LAMBDA,
                                        lambda_name=self.name,
                                        handler_func=self.handler_func,
                                        region_name=self.region_name,
                                        alias=self.alias,
                                        description=self.description,
                                        virtual_environment_path=self.virtual_environment_path,
                                        dependence_paths=self.dependency_paths,
                                        role_name=self.role_name)

    def __rshift__(self, node_or_nodes):
        children = sajan_util.listify(node_or_nodes)
        self.handler = partial(self.handler_func, children)

        return Composite(self, children)


class Firehose(Service):
    # TODO: There are more parameters like Compression: Gzip etc. that we must consider.ƒ
    def __init__(self, firehose_name, database_name, user_name, user_password, server_address, table_name,
                 holding_bucket, role_name, copy_options=DEFAULT_COPY_OPTIONS, prefix=None, buffering_size_mb=128,
                 buffering_interval_seconds=900, log_group=None, log_stream='RedshiftDelivery'):
        # self.service = Service(aws.FIREHOSE)
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
        # super(Firehose, self).__init__([self.firehose_name])
        super(Firehose, self).__init__(service_name=aws.FIREHOSE,
                                       firehose_name=firehose_name)
        pass


class Table(Service):
    def __init__(self, database_config, table_name, *columns, **keys):
        # self.service = Service(aws.TABLE)
        self.database_config = database_config
        self.table_name = table_name

        self.metadata = sa.MetaData(bind=self.create_engine(database_config))

        self.table = sa.Table(table_name,
                              self.metadata,
                              *columns,
                              **keys)
        # super(Table, self).__init__([database_config, table_name, columns, keys])
        super(Table, self).__init__(service_name=aws.TABLE,
                                    table_name=table_name,
                                    columns=columns,
                                    keys=keys)

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


if __name__ == '__main__':
    from pprint import pprint

    kwargs = dict(region_name='us', bucket='b1', on=[event(S3.ALL_CREATED_OBJECTS)])
    s1 = S3('b1', 'us', on=[event(S3.ALL_CREATED_OBJECTS, service_name='x')])

    s2 = S3(**kwargs)
    print(s2 == s1)

# def raise_(cls_exception, str):
#     raise cls_exception(str)
