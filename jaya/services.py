import jaya

from jaya.sajan import Composite, Service, SajanContext
from jaya.sajan import util as sajan_util
from jaya.core import aws
import sqlalchemy as sa
from typing import List, Dict, TypeVar

DLQ_SERVICES = ['SQS', 'SNS']

PYTHON36 = 'python3.6'
DEFAULT_COPY_OPTIONS = "format as json 'auto' gzip timeformat 'auto' truncatecolumns"
C = TypeVar('C')


def require(keyword_argument, name):
    assert keyword_argument is not None, "Argument '{name}' is mandatory".format(name=name)


class S3(Service):
    ALL_CREATED_OBJECTS = 's3:ObjectCreated:*'
    ALL_REMOVED_OBJECTS = 's3:ObjectRemoved:*'

    def __init__(self, bucket_name: str, region_name: str, events: List[Dict[str, str]] = None):
        if not events:
            events = []

        # TODO: If region_name is made optional, then it should be US Standard or whatever the default is?
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.events = events
        super(S3, self).__init__(service_name=aws.S3,
                                 bucket_name=bucket_name,
                                 region_name=region_name,
                                 events=events)

    @staticmethod
    def event(trigger: str, prefix: str = None, suffix: str = None, service_name: str = None) -> Dict[str, str]:
        require(trigger, 'trigger')
        return {'service_name': service_name,
                'trigger': trigger,
                'prefix': prefix,
                'suffix': suffix
                }


class LambdaHandler(object):
    def __init__(self):
        self.jaya_context = SajanContext()
        pass

    def initialize(self):
        pass

    def handler(self, event, context):
        pass


class AWSLambda(Service):
    def __init__(self,
                 name: str,
                 handler: LambdaHandler,
                 region_name: str,
                 memory: int = 1536,
                 timeout: int = 300,
                 alias=None,
                 dependencies=None,
                 runtime=PYTHON36,
                 description='',
                 virtual_environment_path=None,
                 role_name=None,
                 tracing_config=None,
                 environment_variables=None,
                 dead_letter_queue: Dict[str, str] = None):
        self.name = name
        self.handler_class = handler
        self.handler = handler
        self.region_name = region_name
        self.memory = memory
        self.timeout = timeout
        self.alias = alias
        self.runtime = runtime
        self.description = description
        # TODO: This should be mandatory. But we will later add a feature to take it from the Pipeline configuration anf fail only if that does not exist
        self.virtual_environment_path = virtual_environment_path
        if not dependencies:
            dependencies = []
        dependencies.extend([jaya.sajan, jaya])

        self.dependency_paths = [sajan_util.module_path(d) for d in dependencies]
        self.role_name = role_name

        self.tracing_config = tracing_config or {'Mode': 'PassThrough'}
        self.environment_variables = environment_variables
        self.dead_letter_queue = dead_letter_queue
        if self.dead_letter_queue:
            service = self.dead_letter_queue['service']
            assert service in DLQ_SERVICES, "Invalid {} not in {}".format(service, DLQ_SERVICES)

        # TODO: Pass all lambda values?
        super(AWSLambda, self).__init__(service_name=aws.LAMBDA,
                                        lambda_name=self.name,
                                        handler_class=self.handler_class,
                                        region_name=self.region_name,
                                        alias=self.alias,
                                        description=self.description,
                                        virtual_environment_path=self.virtual_environment_path,
                                        dependency_paths=self.dependency_paths,
                                        role_name=self.role_name,
                                        tracing_config=self.tracing_config,
                                        environment_variables=self.environment_variables,
                                        dead_letter_queue=self.dead_letter_queue)

    def __rshift__(self, node_or_nodes):
        children = sajan_util.listify(node_or_nodes)
        self.handler.jaya_context = SajanContext(children=children)

        return Composite(self, children)


class Firehose(Service):
    # TODO: There are more parameters like Compression: Gzip etc. that we must consider.ƒ
    def __init__(self, firehose_name, database_name, user_name, user_password, server_address, table_name,
                 holding_bucket, role_name, copy_options=DEFAULT_COPY_OPTIONS, prefix=None, buffering_size_mb=128,
                 buffering_interval_seconds=900, log_group=None, log_stream='RedshiftDelivery'):
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
        super(Firehose, self).__init__(service_name=aws.FIREHOSE,
                                       firehose_name=firehose_name)
        pass


# class SNS(Service):
#     def __init__(self, topic=None, region_name=None):
#         assert topic, 'topic is a required field'
#         self.name = topic
#         self.region_name = region_name
#         super(SNS, self).__init__(service_name=aws.SNS,
#                                   name=self.name,
#                                   region_name=self.region_name)
#
#         pass


# class CloudWatchEventRule(object):
#     pass
#
#
# class CloudWatchEvent(Service):
#     def __init__(self, name: str, role: str, rule: str, description: str = None):
#         self.name = name
#         self.role = role
#         self.rule = rule
#         self.description = description
#         super(CloudWatchEvent, self).__init__(service_name=aws.CLOUDWATCH_EVENT,
#                                               role=self.role,
#                                               name=self.name,
#                                               rule=self.rule,
#                                               description=self.description)
#
#     @staticmethod
#     def rule(**kwargs):
#         return kwargs


class Table(Service):
    def __init__(self, database_config, table_name, *columns, **keys):
        self.database_config = database_config
        self.table_name = table_name

        self.metadata = sa.MetaData(bind=self.create_engine(database_config))

        self.table = sa.Table(table_name,
                              self.metadata,
                              *columns,
                              **keys)
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
        return response
