import boto3
from util.S3File import S3File
from util.Manifest import Manifest
from util.S3ByteRange import S3ByteRange
import logging
import os
import sys
import time

s3helper_out_handle = None
s3 = None


class S3Helper:
    """
    This class does all the S3 interactions
    """
    def __init__(self, region=None):
        self.make_s3_connection(region)

    @staticmethod
    def make_s3_connection(region=None):
        global s3
        if s3 is None:
            s3helper_out_handle = sys.stdout
            if region is not None:
                s3 = boto3.client('s3', region_name=region)
            else:
                s3 = boto3.client('s3')
        return s3

    @staticmethod
    def set_stdout(output_handle):
        global s3helper_out_handle
        s3helper_out_handle = output_handle

    @staticmethod
    def get_s3_connection():
        return S3Helper.make_s3_connection()

    @staticmethod
    def retrieve_manifest(s3file_manifest):
        """
        
        :param s3file_manifest: S3File that represents path to manifest in S3
        :return: Manifest object that represents the manifest content
        """
        if not isinstance(s3file_manifest, S3File):
            raise(Exception('retrieve_manifest can only be called with S3File parameter '+str(type(s3file_manifest))))
        client = S3Helper.get_s3_connection()
        response = client.get_object(Bucket=s3file_manifest.get_bucket(), Key=s3file_manifest.get_key())
        manifest_as_string = response['Body'].read()
        return Manifest(manifest_json_string=manifest_as_string)


    @staticmethod
    def retrieve_file(s3_file, target_path, **kwargs):
        """
        
        :param s3_file: S3File to retrieve 
        :param target_path: target directory to store the files if None then it will write the content straight to the
        global s3helper_out_handle
        :param kwargs: 
          - symmetric_key=None: if provided then client-side encryption is assumed to decrypt the files
          - overwrite=False: if destination file exists overwrite it otherwise raise error
          - target_file_name=None: name of target file. If None than last part of S3Path is used
        :return: 
        """
        if 'symmetric_key' in kwargs:
            symmetric_key = kwargs['symmetric_key']
        else:
            symmetric_key = None

        if 'overwrite' in kwargs:
            overwrite = kwargs['overwrite']
        else:
            overwrite = False

        if 'retain_manifest' in kwargs:
            retain_manifest = kwargs['retain_manifest']
        else:
            retain_manifest = False

        if 'target_file_name' in kwargs:
            target_file_name = kwargs['target_file_name']
        else:
            target_file_name = s3_file.get_s3_file_name()

        if 'bytes_per_fetch' in kwargs:
            bytes_per_fetch = S3ByteRange(int(kwargs['bytes_per_fetch']))
        else:
            bytes_per_fetch = S3ByteRange(10000000)

        if target_path is None:
            """
            
            """
            global s3helper_out_handle
            retries = 0
            back_off = 2
            content_length = bytes_per_fetch.size

            while content_length == bytes_per_fetch.size and retries < 10:
                try:
                    logging.debug('Retrieving range {r}'.format(r=str(bytes_per_fetch)))
                    response = S3Helper.get_s3_connection().get_object(Bucket=s3_file.get_bucket(),
                                                                   Key=s3_file.get_key(),
                                                                   Range=str(bytes_per_fetch))
                    if s3helper_out_handle is None:
                        sys.stdout.buffer.write(response['Body'].read())
                    else:
                        s3helper_out_handle.write(response['Body'].read())
                    content_length = response['ContentLength']
                    bytes_per_fetch.next()
                    retries = 0
                    back_off = 2
                except Exception as e:
                    if isinstance(e, AttributeError) or isinstance(e, TypeError):
                        logging.fatal('Seems like invalid streamer is given for output: {e}'.format(e=str(e)))
                        raise(e)
                    logging.debug('Exception when getting byte range.')
                    if hasattr(e,'response') \
                            and 'Error' in e.response \
                            and 'Code' in e.response['Error'] \
                            and e.response['Error']['Code'] == 'InvalidRange':
                        logging.debug('Requesting range that is not satisfiable.  Can happen when filesize can be split in chunks of bytes_per_fetch size.')
                        content_length = 1
                    else:
                        logging.debug('Exception encountered while retrieving byte range: {e}'.format(e=str(e)))
                        time.sleep(back_off)
                        back_off *= 2
                        retries += 1

        else:
            destination_path = os.path.join(target_path, target_file_name)

            transfer = boto3.s3.transfer.S3Transfer(s3)
            response = transfer.download_file(s3_file.get_bucket(),
                                   s3_file.get_key(), destination_path)
            logging.info('Downloaded file {src} to {dest}'.format(src=str(s3_file), dest=destination_path))

    @staticmethod
    def retrieve_files_from_manifest_file(s3file_manifest, target_path, **kwargs):
        """
        Retrieve files using a manifest file
        :param s3file_manifest: object of type util.S3File.S3File()
        :param target_path: 
        :param kwargs:
          - symmetric_key=None: if provided then client-side encryption is assumed to decrypt the files
          - overwrite=False: if destination file exists overwrite it otherwise raise error
          - retain_manifest=False: whether manifest file
        :return: 
        """
        symmetric_key = None
        overwrite = False
        retain_manifest = False

        logging.debug('Retrieve manifest file from S3 location={s3loc}.'.format(s3loc=str(s3file_manifest)))
        s3manifest = S3Helper.retrieve_manifest(s3file_manifest)

        for s3file in s3manifest.s3_files:
            logging.debug('Processing S3 file {file}'.format(file=str(s3file)))
            S3Helper.retrieve_file(s3file, target_path, symmetric_key=symmetric_key, overwrite=overwrite)
