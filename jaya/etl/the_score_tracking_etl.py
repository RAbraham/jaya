import base64
import json
import functools
import urllib2
from lib import util
from lib import aws
from lib import validate as validation_helper
import copy
import jaya.etl.etl_util as etl_util

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


def processor(application, schema_version):
    processing_funcs = {
        (APPLICATION.esports.name, 'v2'): process_v2,
        (APPLICATION.sports.name, 'v2'): process_v2,
        (APPLICATION.sports.name, 'v3'): process_v3,
        (APPLICATION.esports.name, 'v3'): process_v3,
        (APPLICATION.messenger_web.name, 'v3'): process_v3_messenger_web
    }
    return processing_funcs[(application, schema_version)]


class TheScoreTrackingEtl:
    def __init__(self, config, environment, now_time_func=util.utc_now):
        self.config = config
        self.environment = environment
        self.now_time_func = now_time_func

    def extract_and_transform(self, lines, bucket, key):
        return extract_and_transform_lines(lines, bucket, key, self.now_time_func)

    def get_offload_func(self):
        if self.environment == 'development':
            return functools.partial(etl_util.offload_development,
                                     batch_size=self.config['offload_batch_size'])
        else:
            return functools.partial(offload_rows_and_errors,
                                     firehose_client=aws.client(self.config, 'firehose'),
                                     environment=self.environment,
                                     batch_size=self.config['offload_batch_size'])


def parse(raw_line):
    def split_item(item):
        splits = item.split("=", 1)
        return splits[0], splits[1]

    items = raw_line.split("|")
    kv_pairs = [split_item(item) for item in items]
    kv_dict = dict(kv_pairs)
    request_uri = kv_dict[REQUEST_URI]

    url_version = validation_helper.validate_version(request_uri)
    kv_dict[TRACKER_URL_VERSION] = url_version
    url_data = validation_helper.validate_url(url_version, request_uri)
    query_params = url_data['query']

    time_str = query_params.get(CLIENT_TIME, [])
    if time_str:
        kv_dict[CLIENT_TIME] = time_str[0]

    kv_dict[RAW_EVENTS] = query_params.get(URI_RAW_EVENTS, [])

    return kv_dict


def decode_event(event_str):
    unquoted_str = urllib2.unquote(event_str)
    decoded = base64.urlsafe_b64decode(unquoted_str)
    result = json.loads(decoded)
    return result


def extract_and_transform_lines(lines, bucket, key, now_func=util.utc_now):
    all_errors = []
    all_transformed_rows = []
    for line in lines:
        try:
            if is_valid_log_entry(line):
                parsed_dict = parse(line)
                # No longer interested in V1 events and filter v3 events for now as it creates noise in the errors table
                tracker_url_version = parsed_dict[TRACKER_URL_VERSION]
                if tracker_url_version == 'v1':
                    continue
                processed_rows = extract_and_transform_line(parsed_dict, bucket, key, now_func)
                for raw_event, transformed_row, line_error in processed_rows:
                    if line_error:
                        all_errors.append(etl_util.error_line(line,
                                                              aws.s3_path(bucket, key),
                                                              line_error,
                                                              now_func,
                                                              raw_event))
                    else:
                        all_transformed_rows.append(transformed_row)

        except Exception as ex:
            all_errors.append(etl_util.error_line(line,
                                                  aws.s3_path(bucket, key),
                                                  util.error_dict(ex),
                                                  now_func))
    return all_transformed_rows, all_errors


# Returns 3-tuple (raw_event, transformed_record, error)
def extract_and_transform_line(parsed_dict, bucket, key, now_func):
    all_transformed = []

    try:
        line_dicts = flatten_parsed_dict(parsed_dict)

        for line_dict in line_dicts:
            transformed_record = transform(line_dict, bucket, key, now_func)
            all_transformed.append(transformed_record)

    except Exception as ex:
        all_transformed.append((None, None, util.error_dict(ex)))

    return all_transformed


def pre_process_event_set(url_version, decoded_event):
    result = copy.deepcopy(decoded_event)
    if 'application' not in result and url_version == 'v2':
        result['application'] = APPLICATION.sports.name
    return result


def transform(line_dict, bucket, key, now_func):
    raw_event = line_dict[RAW_EVENT]
    url_version = line_dict[TRACKER_URL_VERSION]

    try:
        decoded_event = validation_helper.decode_event(raw_event)

        decoded_event = pre_process_event_set(url_version, decoded_event)

        validation_helper.raise_error_if_invalid_event_dict(url_version, decoded_event, fail_quickly=True)

        schema_version = validation_helper.get_schema_version(url_version, decoded_event)

        application = decoded_event['application']
        result = (
            raw_event, processor(application, schema_version)(bucket, key, now_func, decoded_event, line_dict), None)
    except Exception as e:
        result = (raw_event, None, util.error_dict(e))
    return result


def meta_record_info(bucket, key, now_func):
    return {
        'created_at_utc': str(now_func()),
        's3_bucket': bucket,
        's3_key': key
    }


def process_v2(bucket, key, now_func, raw_event, line_dict):
    common_dict = util.filter_keys(raw_event, ['event_id', 'install_id', 'v_maj'])
    event = raw_event.get('event') or raw_event.get('evt')

    info_for_offload = {
        'table': 'v2_events',
        'application': raw_event['application']
    }

    processed = {
        'event': event,
        'raw_event': raw_event,

    }
    return util.merge_dicts(meta_record_info(bucket, key, now_func),
                            processed,
                            info_for_offload,
                            common_dict)


def server_info(line_dict):
    return {
        'server_timestamp_utc': util.epoch_to_utc_str(line_dict['ts']),
        'client_ip_address': line_dict['ip']
    }


def process_v3(bucket, key, now_func, decoded_event, line_dict):
    processed_event = util.merge_dicts(decoded_event,
                                       device_timestamp_columns(decoded_event))
    info_for_offload = {'table': decoded_event['event'],
                        'application': decoded_event['application']}

    return util.merge_dicts(server_info(line_dict),
                            meta_record_info(bucket, key, now_func),
                            info_for_offload,
                            processed_event)


def process_v3_messenger_web(bucket, key, now_func, decoded_event, line_dict):
    payload_dict = {}
    if PAYLOAD in decoded_event:
        payload = decoded_event[PAYLOAD]
        payload_str = json.dumps(payload)
        payload_dict = {PAYLOAD: payload_str}

    processed_event = util.merge_dicts(decoded_event,
                                       device_timestamp_columns(decoded_event),
                                       payload_dict)

    info_for_offload = {'table': decoded_event['event'],
                        'application': decoded_event['application']}

    return util.merge_dicts(server_info(line_dict),
                            meta_record_info(bucket, key, now_func),
                            info_for_offload,
                            processed_event)


def device_timestamp_columns(a_dict):
    ts = a_dict['device_timestamp']
    return {
        'device_timestamp_utc': util.to_utc_str(ts),
        'device_timestamp_local': ts
    }


# A parsed line may have multiple raw events
def flatten_parsed_dict(parsed_dict):
    result = []
    except_raw_events_dict = util.filter_keys_except(parsed_dict, ['raw_events'])

    raw_events = parsed_dict['raw_events']

    if raw_events:
        result = [util.merge_dicts(except_raw_events_dict,
                                   {'raw_event': raw_event})
                  for raw_event in raw_events]
    else:
        result.append(util.merge_dicts(except_raw_events_dict,
                                       {'raw_event': {}}))

    return result


def firehose_name(environment, application, table_name):
    return environment + '_' + application + '_' + table_name


def offload_rows_and_errors(rows, errors, firehose_client, environment, batch_size):
    # Normal Rows
    grouped_rows = util.group_by_key(rows, 'application', 'table')
    log_response_firehose = environment + etl_util.TRACKER_FIREHOSE_PREFIX + 'firehose_responses'
    for (application, table), table_rows in grouped_rows.items():
        table_firehose = firehose_name(environment, application, table)
        etl_util.offload(firehose_client, log_response_firehose, table_firehose, batch_size, table_rows)

    # Errors
    etl_errors_firehose = environment + etl_util.TRACKER_FIREHOSE_PREFIX + 'etl_errors'
    etl_util.offload(firehose_client, log_response_firehose, etl_errors_firehose, batch_size, errors)


def offload_rows(rows, firehose_client, table_firehose, batch_size):
    # Normal Rows
    grouped_rows = util.group_by_key(rows, 'application', 'table')
    for (application, table), table_rows in grouped_rows.items():
        etl_util.offload(firehose_client, log_response_firehose, table_firehose, batch_size, table_rows)


# Returns
# - error_dict for the first error
# - None if there are no errors
def validate(validator, data, filtered_error_attributes=ERROR_ATTRIBUTES):
    result = None
    try:
        validator.validate(data)
    except ValidationError as ex:
        error_dict = util.filter_keys(ex.__dict__, filtered_error_attributes)
        result = util.merge_dicts(error_dict, {'type': "ValidationError"})

    return result


def filter_and_strip(file_lines):
    return [e.rstrip('\n') for e in file_lines if e is not None]


# Returns
# - (processed_event, None) if successful validation
# - (None, error) if failed validation
def validate_and_process(validator, processing_func, raw_data):
    processed_event = None
    processing_error = None
    validation_error = validate(validator, raw_data)

    if validation_error:
        return None, validation_error
    else:
        try:
            processed_event = processing_func(raw_data)
        except Exception as e:
            processing_error = util.error_dict(e)

    if processing_error:
        return None, processing_error
    else:
        return processed_event, None


def is_valid_log_entry(log_entry):
    invalid_strs = [
        '/validate/v1/', 'favicon.ico'
    ]
    for s in invalid_strs:
        if s in log_entry:
            return False
    return True
