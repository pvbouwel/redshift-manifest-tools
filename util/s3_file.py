import re
import boto3
import click
import logging
import time
from util.s3_file_fragment import S3FileFragment


class InvalidS3PathException(Exception):
    def __init__(self, message):
        super(InvalidS3PathException, self).__init__(message)


class S3File:
    """
    This class will do all the S3 interactions
    S3file is identified by either:
      s3path like s3://bucketname/pathname given as 1st argument or as a kwarg
    kwargs or 2 positional arguments:
     - bucket_name
     - key

    """

    def __init__(self, *args, **kwargs):
        self.s3 = None
        self.region = kwargs.get('region', None)
        self.reset_transfer_attempts()
        self.has_meta = False
        self.head_object = None
        self.retries = 0   # default value in reset_transfer_attempts
        self.back_off = 2  # default value in reset_transfer_attempts
        self.x_amz_key = None
        self.x_amz_iv = None
        self.x_amz_matdesc = None
        s3path = None
        if len(args) == 1:
            # 1 arguments given should be s3path
            s3path = args[0]
        elif len(args) == 2:
            # 2 arguments given should be bucket and key
            self.bucket_name = args[0]
            self.key = args[1]
        elif len(args) == 0 and len(kwargs) != 0:
            if 'bucket_name' in kwargs:
                self.bucket_name = kwargs['bucket_name']
            if 'key' in kwargs:
                self.key = kwargs['key']
            if 's3path' in kwargs:
                s3path = kwargs['s3path']
        else:
            raise Exception('Unsupported initialization of S3File')

        if s3path is not None:
            if not s3path.startswith('s3://'):
                raise InvalidS3PathException(
                    message='S3 path did not start with \'s3://\': {path}'.format(path=s3path)
                )
            # Bucket restrictions according to AWS doc
            # 3-63 characters long
            # One or more labels separated by a .
            # lowercase, numbers hyphens (must start with number or lowercase
            # North virginia region can have Upper cases as wel
            s3path_structure = [
                {
                    'name': 'prefix',
                    'pattern': 's3://'
                },
                {
                    'name': 'bucket',
                    'pattern': '[a-zA-Z0-9.-]*'
                },
                {
                    'name': 'separator',
                    'pattern': '/'
                },
                {
                    'name': 'key',
                    'pattern': '.*'
                }
            ]
            pattern = ''
            for element in s3path_structure:
                if 'name' in element.keys():
                    pattern += '(?P<' + element['name'] + '>'
                pattern += element['pattern']
                if 'name' in element.keys():
                    pattern += ')'
            regex_s3_path = re.compile(pattern)
            match_result = regex_s3_path.match(s3path)
            if match_result is None:
                raise InvalidS3PathException(
                    'Could not parse S3 path. Is a valid s3 path given? {path}'.format(path=s3path)
                )
            self.bucket_name = match_result.groupdict().pop('bucket')
            self.key = match_result.groupdict().pop('key')

    def reset_transfer_attempts(self):
        self.retries = 0
        self.back_off = 2

    def failed_transfer_attempt(self):
        logging.debug('Failed transfer, awaiting for backoff {b} before retrying'.format(b=str(self.back_off)))
        time.sleep(self.back_off)
        self.retries += 1
        self.back_off *= 2

    def has_too_many_retries(self):
        return self.retries >= 10

    def get_key(self):
        return self.key

    def get_bucket(self):
        return self.bucket_name

    def get_s3_file_name(self, prefix=None):
        """
        Only return the part after the latest forward slash (/) or the part after the given prefix

        Args:
            prefix(str):

        Returns:
            str:
        """
        if prefix is None:
            s3_file_name_parts = self.key.split('/')
            return s3_file_name_parts[-1]
        else:
            if not str(self).startswith(prefix):
                raise(ValueError('S3 file {f} does not start with prefix {p}.'.format(f=str(self), p=prefix)))
            else:
                prefix_length = len(prefix)
                return str(self)[prefix_length:]

    def __eq__(self, other):
        return self.key == other.key and self.bucket_name == other.bucket_name

    def __str__(self):
        return 's3://{bucket}/{key}'.format(bucket=self.bucket_name, key=self.key)

    def get_s3_connection(self, force=False):
        if self.s3 is None or force:
            if self.region is not None and self.region != 'unknown':
                self.s3 = boto3.client('s3', region_name=self.region)
            else:
                self.s3 = boto3.client('s3')

        return self.s3

    def has_meta(self):
        return self.has_meta

    # noinspection PyBroadException
    def get_meta(self):
        """
        Get the S3 object its metadata

        Returns:
            :The head_object result from S3
        """
        if not self.has_meta:
            try:
                self.head_object = self.get_s3_connection().head_object(Bucket=self.get_bucket(), Key=self.get_key())
                self.has_meta = True
            except Exception as e:
                if self.region is None:
                    logging.debug('Could not get meta-data of S3Object. Region not set so assuming incorrect region.')
                    logging.debug('Try to determine bucket region, and try to reconfigure the connection.')
                    self.region = 'unknown'
                    self.connect_to_bucket_region()
                    return self.get_meta()
                else:
                    region_set_no_metadata = 'Could not get meta-data of S3Object and region was {r}.'
                    raise Exception(region_set_no_metadata.format(r=str(self.region))) from e
        return self.head_object

    # noinspection PyUnresolvedReferences
    def get_range(self, s3_byte_range):
        while not self.has_too_many_retries():
            try:
                response = self.get_s3_connection().get_object(
                    Bucket=self.get_bucket(),
                    Key=self.get_key(),
                    Range=str(s3_byte_range)
                )
                length = response['ContentLength']
                self.reset_transfer_attempts()
                return S3FileFragment(response['Body'], length, s3_byte_range)
            except Exception as e:
                logging.debug('Exception when getting byte range.')
                if hasattr(e, 'response') and 'Error' in e.response and 'Code' in e.response['Error'] \
                        and e.response['Error']['Code'] == 'InvalidRange':
                    logging.fatal('Requesting range that is not satisfiable.  Filesize = {fs}, byterange ={r}'
                                  'This should not happen.'.format(fs=str(self.get_size()), r=str(s3_byte_range)))
                    raise e
                else:
                    logging.debug('Exception encountered while retrieving byte range: {e}'.format(e=str(e)))
                    self.failed_transfer_attempt()
        raise(Exception("Too many S3 contact attempts to get byte range."))

    def get_file_content(self):
        s3_byte_range = self.get_size()
        s3_file_fragment = self.get_range(s3_byte_range)
        return s3_file_fragment.get_streaming_body().read()

    # noinspection PyUnresolvedReferences
    def download_file(self, destination_path):
        transfer = boto3.s3.transfer.S3Transfer(self.get_s3_connection())
        return transfer.download_file(self.get_bucket(), self.get_key(), destination_path)

    def get_size(self):
        """
        :return: The file size of the S3File
        """
        self.get_meta()
        if 'ContentLength' in self.head_object:
            return self.head_object['ContentLength']
        else:
            msg = 'ContentLength was not available in head_object; response={r}'
            raise(Exception(msg.format(r=str(self.head_object))))

    def set_region(self, region):
        self.region = region

    def get_region(self):
        return self.region

    def connect_to_bucket_region(self):
        """
        This method will determine the bucket region using the api.
        :return: 
        """
        bucket = self.get_s3_connection().get_bucket_location(Bucket=self.bucket_name)
        bucket_location = bucket.get('LocationConstraint', None)
        if bucket_location is not None:
            self.region = bucket_location
            self.get_s3_connection(force=True)
            return self.region
        else:
            raise(Exception('Could not get region for bucket {b}. Does region exist?'.format(b=self.bucket_name)))

    def get_encryption_metadata(self):
        """
        Encrypted objects have the following metadata:
         - x-amz-key
         - x-amz-iv
         - x-amz-matdesc
        :return: 
        """
        head_object = self.get_meta()
        if 'Metadata' in head_object:
            meta_data = head_object['Metadata']
            if 'x-amz-key' in meta_data:
                self.x_amz_key = meta_data.get('x-amz-key', None)
            else:
                raise(Exception('x-amz-key is not available in meta-data.'))

            if 'x-amz-iv' in meta_data:
                self.x_amz_iv = meta_data.get('x-amz-iv', None)
            else:
                raise(Exception('x-amz-iv is not available in meta-data.'))

            if 'x-amz-matdesc' in meta_data:
                self.x_amz_matdesc = meta_data.get('x-amz-matdesc', None)
            else:
                logging.debug('x-amz-matdesc is not available in meta-data but it is not mandatory.')

    def get_x_amz_iv(self):
        if hasattr(self, 'x_amz_iv'):
            return self.x_amz_iv
        else:
            self.get_encryption_metadata()
            return self.x_amz_iv

    def get_x_amz_key(self):
        if hasattr(self, 'x_amz_key'):
            return self.x_amz_key
        else:
            self.get_encryption_metadata()
            return self.x_amz_key


class S3PathParamType(click.ParamType):
    name = 's3-path'

    def convert(self, value, param, ctx):
        try:
            return S3File(value)
        except InvalidS3PathException as e:
            self.fail('{val} is not a valid S3 path: {err}'.format(val=value, err=str(e)), param, ctx)
