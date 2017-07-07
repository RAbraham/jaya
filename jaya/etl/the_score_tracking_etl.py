import base64
import json
import functools
import copy
import jaya.etl.etl_util as etl_util
from jaya.lib import aws

ERROR_ATTRIBUTES = ["message", "validator_value", "schema", "validator", "instance"]
DATA_TYPE_KEY = 'event'
DATA_TYPE_KEYS = ['event', 'evt']
META_SCHEMA_KEY = 'meta'
V1_BASE = "/v1/event"

TRACKER_URL_VERSION = 'tracker_version'

REQUEST_URI = 'ru'
CLIENT_TIME = 't'
RAW_EVENT = 'raw_event'
RAW_EVENTS = 'raw_events'
URI_RAW_EVENTS = 'd'

PAYLOAD = 'payload'


class TheScoreTrackingEtl:
    def __init__(self, config, environment):
        self.config = config
        self.environment = environment

    def get_offload_func(self):
        if self.environment == 'development':
            return functools.partial(etl_util.offload_development,
                                     batch_size=self.config['offload_batch_size'])
        else:
            return functools.partial(offload_rows,
                                     firehose_client=aws.client(self.config, 'firehose'),
                                     environment=self.environment,
                                     batch_size=self.config['offload_batch_size'])


def offload_rows(firehose_name, rows, firehose_client, environment, batch_size):
    log_response_firehose = environment + etl_util.TRACKER_FIREHOSE_PREFIX + 'firehose_responses'
    etl_util.offload(firehose_client, log_response_firehose, firehose_name, batch_size, rows)
