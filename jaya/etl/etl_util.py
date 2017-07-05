import glob
import json
import util
import os
import time
import aws
import botocore

TRACKER_FIREHOSE_PREFIX = '_sports_'

INBOUND = 'inbound'
IN_PROCESS = 'in-process'
DONE = 'done'
CHUNK_SPLITS = 6

def error_line(line, source, line_error, now_func, raw_event=None):
    if not raw_event:
        raw_event = ""
    return {'line': line,
            'created_at_utc': str(now_func()),
            'raw_event': raw_event,
            'source': source,
            'message': line_error['message'],
            'type': line_error['type']}


def _offload_rows_and_errors(rows, errors, firehose_client, environment, batch_size, grouping_keys, firehose_name_func):
    # Normal Rows
    grouped_rows = util.group_by_key(rows, *grouping_keys)
    log_response_firehose = environment + TRACKER_FIREHOSE_PREFIX + 'firehose_responses'

    for key, table_rows in grouped_rows.items():
        table_firehose = firehose_name_func(environment, key)
        offload(firehose_client, log_response_firehose, table_firehose, batch_size, table_rows)
    # Errors
    etl_errors_firehose = environment + TRACKER_FIREHOSE_PREFIX + 'etl_errors'
    offload(firehose_client, log_response_firehose, etl_errors_firehose, batch_size, errors)


def offload(firehose_client, firehose_responses_name, firehose_name, batch_size, rows):
    if rows:
        records = [make_record(json.dumps(row)) for row in rows]
        total_records = len(records)

        for ordinal, chunk in enumerate(util.chunks(records, batch_size)):
            send_chunk_to_firehose(firehose_client,
                                   firehose_responses_name,
                                   firehose_name,
                                   total_records,
                                   ordinal,
                                   chunk)


def send_chunk_to_firehose(firehose_client, firehose_responses_name, firehose_name, total_records, ordinal, chunk,
                           retry=0):
    try:
        response = firehose_client.put_record_batch(DeliveryStreamName=firehose_name, Records=chunk)
    except botocore.exceptions.ClientError:
        if len(chunk) >= 2 and retry < CHUNK_SPLITS:
            half_way = len(chunk)/2
            send_chunk_to_firehose(firehose_client, firehose_responses_name, firehose_name, total_records, ordinal, chunk[:half_way], retry+1)
            send_chunk_to_firehose(firehose_client, firehose_responses_name, firehose_name, total_records, ordinal, chunk[half_way:], retry+1)
            return
        raise
    except botocore.vendored.requests.exceptions.ConnectionError:
        time.sleep(retry + 1)
        if retry == 2 and len(chunk) >= 2:
            half_way = len(chunk)/2
            send_chunk_to_firehose(firehose_client, firehose_responses_name, firehose_name, total_records, ordinal, chunk[:half_way], retry+1)
            send_chunk_to_firehose(firehose_client, firehose_responses_name, firehose_name, total_records, ordinal, chunk[half_way:], retry+1)
            return
        else:
            send_chunk_to_firehose(firehose_client, firehose_responses_name, firehose_name, total_records, ordinal, chunk, retry+1)
            return

    log_firehose_response_record(firehose_client, firehose_responses_name, chunk, firehose_name, total_records,
                                 ordinal, response, retry)

    retry_records = [chunk[ordinal] for ordinal, error in enumerate(response.get('RequestResponses'))
                     if 'ErrorCode' in error]
    if retry_records:
        time.sleep(retry + 1)
        send_chunk_to_firehose(firehose_client, firehose_responses_name, firehose_name, total_records, ordinal,
                               retry_records, retry + 1)


def log_firehose_response_record(firehose_client, firehose_responses_name, chunk, target_firehose, total_records,
                                 ordinal, response, retry):
    data_dict = json.loads(chunk[0]['Data'])
    if data_dict and 's3_bucket' in data_dict and 's3_key' in data_dict:
        s3_bucket = data_dict['s3_bucket']
        s3_key = data_dict['s3_key']
    else:
        s3_bucket = None
        s3_key = None

    response['RequestResponses'] = [util.filter_keys_except(response, "RecordId")
                                    for response in response.get('RequestResponses', [])]
    log_response = {
        'created_at_utc': str(util.utc_now()),
        's3_bucket': s3_bucket,
        's3_key': s3_key,
        'target_firehose': target_firehose,
        'total_records': total_records,
        'ordinal': ordinal,
        'response': json.dumps(response),
        'retry': retry
    }
    log_record = make_record(json.dumps(log_response))
    firehose_client.put_record(DeliveryStreamName=firehose_responses_name, Record=log_record)


def make_record(row):
    return {"Data": row}


def offload_development(rows, errors, batch_size):
    def offload_to_file(file_name, lines):
        print('Offloading to file:' + file_name + ':lines:' + str(len(lines)))
        chunk_number = 1
        records = [make_record(json.dumps(row)) for row in lines]
        for chunk in util.chunks(records, batch_size):
            with open(file_name + str(chunk_number), 'a') as fh:
                json.dump(chunk, fh)
            chunk_number += 1

    offload_to_file('/tmp/transformed', rows)
    offload_to_file('/tmp/errors', errors)
