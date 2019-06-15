from test import symmetric_base64_aes256_key
import pkg_resources
from util.file_cryptor import EnvelopeFileCryptor
from util.symmetric_key import SymmetricKey
from string import Template


class EncryptionTest(object):
    IV = 'iv'
    IV_SUFFIX = '.{iv}'.format(iv=IV)
    KEY = 'key'
    KEY_SUFFIX = '.{key}'.format(key=KEY)
    ENCRYPTED = 'encrypted'
    PLAINTEXT = 'plaintext'
    RESOURCE_PATH_TEMPLATE = 'resources/${crypto_state}.${id}${suffix}'

    def __init__(self, test_id):
        """

        Args:
            test_id(str): the identifier of the test e.g. 0000_part_00
        """
        self.id = test_id

    def get_resource_path_template(self):
        return Template(Template(self.RESOURCE_PATH_TEMPLATE).safe_substitute(id=self.id))

    def get_input_file_text(self, crypto_state, suffix=''):
        file_name = pkg_resources.resource_filename(
            'test',
            self.get_resource_path_template().substitute(crypto_state=crypto_state, suffix=suffix)
        )
        file_mode = 'r' if crypto_state == self.PLAINTEXT else 'rb'
        with open(file_name, file_mode) as input_file:
            return input_file.read()

    def get_cipher_text(self):
        """

        Returns:
            bytes:
        """
        return self.get_input_file_text(crypto_state=self.ENCRYPTED)

    def get_iv_text(self):
        return self.get_input_file_text(crypto_state=self.ENCRYPTED, suffix=self.IV_SUFFIX)

    def get_key_text(self):
        return self.get_input_file_text(crypto_state=self.ENCRYPTED, suffix=self.KEY_SUFFIX)

    def get_plain_text(self):
        return self.get_input_file_text(crypto_state=self.PLAINTEXT)

    def get_decrypted_crypto_text(self):
        cryptor = EnvelopeFileCryptor(
            symmetric_key=SymmetricKey(symmetric_base64_aes256_key),
            iv=self.get_iv_text(),
            data_key=self.get_key_text()
        )
        return cryptor.decrypt_bytes(self.get_cipher_text())

    def assert_decrypted_is_expected(self):
        decrypted_crypto_test = self.get_decrypted_crypto_text()
        expected_text = self.get_plain_text()
        assert expected_text == decrypted_crypto_test.decode('utf-8')


def test_unittest_decrypt():
    EncryptionTest(test_id='0000_part_00').assert_decrypted_is_expected()
    EncryptionTest(test_id='0001_part_00').assert_decrypted_is_expected()
    EncryptionTest(test_id='0002_part_00').assert_decrypted_is_expected()
    EncryptionTest(test_id='0003_part_00').assert_decrypted_is_expected()
