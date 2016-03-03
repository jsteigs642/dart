import os
import boto
from boto.exception import S3ResponseError, S3DataError
from boto.s3.key import Key
from dart.util.s3 import get_bucket_name, get_key_name


class Secrets(object):
    def __init__(self, kms_key_arn, secrets_s3_path):
        self._kms_key_arn = kms_key_arn
        self._secrets_s3_path = secrets_s3_path.rstrip('/')
        self._bucket_name = get_bucket_name(self._secrets_s3_path)
        self._s3_prefix = get_key_name(self._secrets_s3_path)

    def get(self, key):
        try:
            os.environ['S3_USE_SIGV4'] = 'True'
            b = boto.connect_s3(host='s3.amazonaws.com').get_bucket(self._bucket_name)
            key_obj = Key(b)
            key_obj.key = self._s3_prefix + '/' + key.lstrip('/')
            return key_obj.get_contents_as_string()
        except S3ResponseError as e:
            if e.status == 404:
                return None
            raise e
        finally:
            del os.environ['S3_USE_SIGV4']

    def put(self, key, value):
        try:
            os.environ['S3_USE_SIGV4'] = 'True'
            b = boto.connect_s3(host='s3.amazonaws.com').get_bucket(self._bucket_name)
            key_obj = Key(b)
            key_obj.key = self._s3_prefix + '/' + key.lstrip('/')
            key_obj.set_contents_from_string(value, headers={
                'x-amz-server-side-encryption': 'aws:kms',
                'x-amz-ssekms-key-id': self._kms_key_arn
            })
        except S3DataError, e:
            # ignore this error until AWS fixes boto bug: https://github.com/boto/boto/issues/2921
            if 'ETag from S3 did not match computed MD5' not in e.message:
                raise e
        finally:
            del os.environ['S3_USE_SIGV4']

    def delete(self, key):
        try:
            os.environ['S3_USE_SIGV4'] = 'True'
            b = boto.connect_s3(host='s3.amazonaws.com').get_bucket(self._bucket_name)
            key_obj = Key(b)
            key_obj.key = self._s3_prefix + '/' + key.lstrip('/')
            key_obj.delete()
        finally:
            del os.environ['S3_USE_SIGV4']
