import unittest
from util.S3File import S3File
from util.S3FileTransfer import S3FileTransfer
from util.S3FileCryptor import S3FileCryptor
from util.SymmetricKey import SymmetricKey
import tempfile
import os


class MyTestCase(unittest.TestCase):
    def test_simple_decrypt(self):
        s3file_encrypted = S3File('s3://manifest-tools/encrypted/encrypted.0000_part_00')
        temp_dir = tempfile.TemporaryDirectory()
        temp_file = os.path.join(temp_dir.name, s3file_encrypted.get_s3_file_name())
        s3filetransfer = S3FileTransfer(s3file_encrypted, temp_file)
        cryptor = S3FileCryptor(SymmetricKey('cibeQ6J5GwJ8hLrrAdAbb09HjObumZGC/LuzM1RBKRA='))
        cryptor.decrypt(s3filetransfer)
        with open(temp_file, 'r') as input_file:
            raw_content = ''.join(input_file.readlines())
        self.assertEqual(raw_content.strip(), '1')


if __name__ == '__main__':
    unittest.main()
