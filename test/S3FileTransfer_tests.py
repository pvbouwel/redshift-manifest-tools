import unittest
from util.S3FileTransfer import S3FileTransfer
from sys import platform


class MyTestCase(unittest.TestCase):
    def test_get_parent_dir(self):
        if platform == 'win32' or platform == 'cygwin':
            self.assertEqual(S3FileTransfer.get_parent_dir('C:\\path\to\file.txt'),'C:\\path\to')
        else:
            self.assertEqual(S3FileTransfer.get_parent_dir('/path/to/file.txt'), '/path/to')


if __name__ == '__main__':
    unittest.main()
