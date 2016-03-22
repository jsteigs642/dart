from datetime import datetime, timedelta
import logging
import boto3
import re
from dart.util.s3 import get_bucket_name, get_key_name
from dart.util.strings import substitute_date_tokens

_logger = logging.getLogger(__name__)


def data_check(s3_engine, datastore, action):
    """
    :type s3_engine: dart.engine.s3.s3.S3Engine
    :type datastore: dart.model.datastore.Datastore
    :type action: dart.model.action.Action
    """
    action = s3_engine.dart.patch_action(action, progress=.1)
    args = action.data.args
    now = datetime.utcnow()
    if args.get('date_offset_in_seconds'):
        now = now + timedelta(seconds=args['date_offset_in_seconds'])
    s3_path_prefix = substitute_date_tokens(args['s3_path_prefix'], now)
    bucket_name = get_bucket_name(s3_path_prefix)
    prefix = get_key_name(s3_path_prefix)

    s3_paginator = boto3.client('s3').get_paginator('list_objects')
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for element in (page.get('Contents') or []):
            path = 's3://' + bucket_name + '/' + element['Key']
            if args.get('s3_path_regex') and not re.match(substitute_date_tokens(args['s3_path_regex'], now), path):
                continue
            if args.get('min_file_size_in_bytes') and element['Size'] < args['min_file_size_in_bytes']:
                continue
            return

    raise Exception('Data check failed')
