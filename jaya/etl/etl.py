from .the_score_tracking_etl import TheScoreTrackingEtl
import uuid
from jaya.lib import aws
from jaya.lib import util
import os
import collections
from functools import partial

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ONE_MB = 1048576
THESCORE_TRACKING_APP = 'thescore-tracking'
MESSENGER_BOT_APP = 'messenger-bot'
VALID_APPLICATIONS = [THESCORE_TRACKING_APP, MESSENGER_BOT_APP]
PRODUCTION_TRACKER_LOG_GROUP = '/aws/kinesisfirehose/tracker_production'
FIREHOSE_BUCKET = 'thescore-firehose'


def get_processor(conf, environment):
    return TheScoreTrackingEtl(conf, environment)


def do_etl(conf, environment, bucket_key_pairs, open_function, map_function):
    s3 = aws.resource(conf, 's3')

    for bucket, key in bucket_key_pairs:
        logger.info('Processing file: {}/{}'.format(bucket, key))
        etl_processor = get_processor(conf, environment)
        offload_func = etl_processor.get_offload_func()

        a_path = '/tmp/{}'.format(uuid.uuid4())
        s3.meta.client.download_file(bucket, key, a_path)
        etl_on_file(open_function(a_path), map_function, bucket, key, offload_func, conf['offload_batch_size'])
        os.remove(a_path)
        aws.s3_delete_object(conf, bucket, key)


def etl_on_file(file_handle, map_function, bucket, key, offload_rows_func, batch_size):
    transform_func = partial(map_function, bucket=bucket, key=key)
    with file_handle:
        _transform_and_load(file_handle, offload_rows_func, transform_func, batch_size,
                            ['firehose_name'])


def _transform_and_load(line_iterator, offload_rows_func, transform_func, batch_size, group_by_keys):
    offload_rows = collections.defaultdict(list)

    # all_rows is so that we can return data for test purposes
    all_rows = collections.defaultdict(list)

    for line in line_iterator:
        if not line:
            continue
        line = line.rstrip(b'\n')
        rows = transform_func(line)
        grouped_rows = util.group_by_key(rows, *group_by_keys)
        for firehose_name, mapping_results in grouped_rows.items():
            table_rows = util.flatten([r.get('result', []) for r in mapping_results])
            offload_rows[firehose_name] += table_rows
            all_rows[firehose_name] += table_rows

        # Send those bucketed rows which are greater than the batch size
        for firehose_name in offload_rows:
            if len(offload_rows[firehose_name]) >= batch_size:
                offload_rows_func(firehose_name, offload_rows[firehose_name][:batch_size])
                offload_rows[firehose_name] = offload_rows[firehose_name][batch_size:]

    # Send all remaining
    for firehose_name in offload_rows:
        offload_rows_func(firehose_name, offload_rows[firehose_name])

    return all_rows
