import boto3
from util.S3File import S3File
from util.Manifest import Manifest
from util.S3ByteRange import S3ByteRange
from util.S3FileTransfer import S3FileTransfer
from util.S3FileCryptor import S3FileCryptor
from util.Exceptions import DuplicateLocalFileException, LocalFileExistsAndConflictsWithTargetFileAndNoOverWrite
import logging
import os
import sys
import gzip

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
        manifest_as_string = s3file_manifest.get_file_content()

        if isinstance(manifest_as_string, bytes):
            manifest_as_string = manifest_as_string.decode('utf-8')

        if isinstance(manifest_as_string, str):
            manifest = Manifest(manifest_json_string=manifest_as_string, region=s3file_manifest.get_region())
        else:
            raise (Exception('Invalid type for manifest string {t}'.format(t=type(manifest_as_string))))

        return manifest

    @staticmethod
    def return_data_as_is(data):
        return data

    @staticmethod
    def return_data_decompressed(data):
        return gzip.decompress(data)

    @staticmethod
    def retrieve_file(s3_transfer, **kwargs):
        """
        
        :param s3_transfer: S3FileTransfer that specifies S3File to retrieve and destination location 
        :param kwargs: 
          - symmetric_key=None: if provided then client-side encryption is assumed to decrypt the files
          - overwrite=False: if destination file exists overwrite it otherwise raise error
          - target_file_name=None: name of target file. If None than send content to stdout
        :return: 
        """
        symmetric_key = kwargs.get('symmetric_key', None)
        overwrite = kwargs.get('overwrite', False)
        s3_byte_range = S3ByteRange(int(kwargs.get('bytes_per_fetch', 10000000)))

        if s3_transfer.get_local_file() is None:
            # No destination file means the file content should be sent to stdout
            global s3helper_out_handle
            retries = 0
            back_off = 2

            file_size = s3_transfer.get_size()

            if s3_transfer.get_s3_file().get_key().endswith('.gz'):
                if file_size > s3_byte_range.size:
                    logging.fatal('Gzipped file is bigger than fetch file, not supported to stream {f}'.format(f=str(s3_transfer.get_s3_file())))
                else:
                    f_process = S3Helper.return_data_decompressed
            else:
                f_process = S3Helper.return_data_as_is

            received_bytes = 0
            while received_bytes < file_size:
                logging.debug('Retrieving range {r}'.format(r=str(s3_byte_range)))

                s3_file_fragment = s3_transfer.s3_file.get_range(s3_byte_range)
                received_bytes += s3_file_fragment.get_size()

                try:
                    if s3helper_out_handle is None:
                        sys.stdout.buffer.write(f_process(s3_file_fragment.get_streaming_body().read()))
                    else:
                        s3helper_out_handle.write(f_process(s3_file_fragment.get_streaming_body().read()))
                except Exception as e:
                    logging.fatal('Something went wrong writing data back.')
                    logging.fatal(str(e))
                    raise(e)

                s3_byte_range.next()

        else:
            s3_transfer.download()

            if symmetric_key is not None:
                logging.debug('Decryption is requested')
                cryptor = S3FileCryptor(symmetric_key=symmetric_key)
                try:
                    cryptor.decrypt(s3_transfer)
                except Exception as e:
                    logging.WARN('Exception {e} encountered when decrypting transfer.'.format(e=str(e)))
                    logging.WARN('No decryption performed.')

    @staticmethod
    def retrieve_files_from_manifest_file(s3file_manifest, target_path, **kwargs):
        """
        Retrieve files using a manifest file
        :param s3file_manifest: object of type util.S3File.S3File()
        :param target_path: 
        :param kwargs:
          - symmetric_key=None: if provided then client-side encryption is assumed to decrypt the files
          - overwrite=False: if destination file exists raise error by default if set to true then overwrite
          - region
          - flatten_paths = False: if paths in manifest have different paths only use part after latest forwards slash
          (/) as filename
        :return: 
        """
        symmetric_key = kwargs.get('symmetric_key', None)
        overwrite = kwargs.get('overwrite', False)
        region = kwargs.get('region', None)
        flatten_paths = kwargs.get('flatten_paths', False)

        if region is not None:
            s3file_manifest.set_region(region)

        logging.debug('Retrieve manifest file from S3 location={s3loc}.'.format(s3loc=str(s3file_manifest)))
        s3manifest = S3Helper.retrieve_manifest(s3file_manifest)

        if flatten_paths:
            prefix = None
        else:
            prefix = s3manifest.get_common_path_prefix()

        s3_transfers = []
        local_files = []

        for s3file in s3manifest.s3_files:
            if target_path is not None:
                file_path = os.path.join(target_path, s3file.get_s3_file_name(prefix=prefix))
                local_files.append(file_path)
            else:
                file_path = None

            s3_transfers.append(S3FileTransfer(s3file, file_path))

        if not overwrite:
            if len(local_files) != len(set(local_files)):
                raise(DuplicateLocalFileException('There is a duplicate collision in local_files {lf}'
                                                  .format(lf=str(local_files))))

            for local_file in local_files:
                if os.path.exists(local_file):
                    msg = 'Overwrite is disabled and local file {f} already exists.'.format(f=local_file)
                    raise(LocalFileExistsAndConflictsWithTargetFileAndNoOverWrite(msg))

        for s3_transfer in s3_transfers:
            logging.debug('Processing S3 file {file}'.format(file=str(s3file)))
            S3Helper.retrieve_file(s3_transfer, symmetric_key=symmetric_key, overwrite=overwrite)
