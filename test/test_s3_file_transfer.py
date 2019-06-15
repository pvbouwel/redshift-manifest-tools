from util.s3_file_transfer import S3FileTransfer
from sys import platform


def test_get_parent_dir():
    if platform == 'win32' or platform == 'cygwin':
        assert S3FileTransfer.get_parent_dir('C:\\path\to\file.txt') == 'C:\\path\to'
    else:
        assert S3FileTransfer.get_parent_dir('/path/to/file.txt') == '/path/to'
