import logging
import hashlib
from time import strftime
import datetime
import os


class S3FileTransfer:
    def __init__(self, s3_file, local_file):
        self.s3_file = s3_file
        self.local_file = local_file
        self.temp_file = None
        self.has_temp_file = False
        self.is_downloaded = False

    def get_s3_file(self):
        return self.s3_file

    @staticmethod
    def get_unique_temp_name(file_name):
        md5_string = hashlib.md5()
        md5_string.update(file_name.encode('utf-8'))
        md5_string.update('redshift-manifest-tools'.encode('utf-8'))
        md5_string.update(str(datetime.datetime.now()).encode('utf-8'))
        temp_file_name = '{fn}.{suf}'.format(fn=file_name, suf=md5_string.hexdigest())
        if os.path.exists(temp_file_name):
            raise FileExistsError('Temp file {tf} exists this is a fatal error'.format(tf=temp_file_name))
        else:
            return temp_file_name

    def move_to_temp_location(self):
        """
        Move the destination file to a temporary location
        :return: 
        """
        self.download()
        self.temp_file = S3FileTransfer.get_unique_temp_name(self.local_file)
        os.rename(self.local_file, self.temp_file)
        self.has_temp_file = True
        return self.temp_file

    def get_local_file(self):
        return self.local_file

    def get_size(self):
        return self.s3_file.get_size()

    @staticmethod
    def get_parent_dir(file_path):
        path_elements = file_path.split(os.path.sep)
        return os.path.sep.join(path_elements[:-1])

    def make_sure_local_parent_dir_exists(self):
        parent_dir = S3FileTransfer.get_parent_dir(self.local_file)
        if os.path.isfile(parent_dir) or os.path.islink(parent_dir):
            if not os.path.islink(parent_dir):
                raise(FileExistsError('File {f} exists but it is no directory'.format(f=parent_dir)))
            elif os.path.islink(parent_dir):
                if not os.path.isdir(os.path.realpath(parent_dir)):
                    raise (FileExistsError('File {f} exists but it is link to no directory'.format(f=parent_dir)))
        elif not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)

    def download(self):
        if not self.is_downloaded:
            self.make_sure_local_parent_dir_exists()
            self.s3_file.download_file(self.local_file)
            self.is_downloaded = True
            msg = 'Downloaded file {src} to {dest}'
        else:
            msg = 'File {src} has previously been dowloaded to {dest}, skipping download command.'
        logging.debug(msg.format(src=str(self.s3_file), dest=self.local_file))

    def cleanup_temp_file(self):
        logging.debug('Cleanup temp file {tf}.'.format(tf=self.temp_file))
        os.remove(self.temp_file)
        self.has_temp_file = False

    def __del__(self):
        """
        Destructor if file is downloaded but moved to a temporary file then we clean it up
        :return: 
        """
        if self.has_temp_file:
            logging.warning('Temp file {tf} is still present, will be cleaned up as S3FileTransfer {sft} is destroyed'
                            .format(tf=self.temp_file, sft=str(self)))
            self.cleanup_temp_file()

