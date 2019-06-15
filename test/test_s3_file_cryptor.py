from test import symmetric_base64_aes256_key
from util.s3_file import S3File
from util.s3_file_transfer import S3FileTransfer
from util.file_cryptor import S3EnvelopeFileCryptor
from util.symmetric_key import SymmetricKey
import tempfile
import os


def test_simple_decrypt():
    s3file_encrypted = S3File('s3://manifest-tools/encrypted/encrypted.0000_part_00')
    temp_dir = tempfile.TemporaryDirectory()
    temp_file = os.path.join(temp_dir.name, s3file_encrypted.get_s3_file_name())
    s3filetransfer = S3FileTransfer(s3file_encrypted, temp_file)
    cryptor = S3EnvelopeFileCryptor(SymmetricKey(symmetric_base64_aes256_key), s3_transfer=s3filetransfer)
    cryptor.decrypt(s3filetransfer)
    with open(temp_file, 'r') as input_file:
        raw_content = ''.join(input_file.readlines())
    assert raw_content.strip() == '1|secret'
