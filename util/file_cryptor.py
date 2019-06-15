from util.symmetric_key import SymmetricKey
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import base64
import logging
import binascii

backend = default_backend()


class EnvelopeFileCryptor(object):
    def __init__(self, symmetric_key, iv, data_key):
        """

        Args:
            symmetric_key: The key used for Envelope encryption
            iv: The base64 encoded initialization vector for decrypting the encrypted data blocks.
            data_key: The base64 encoded encryption key used to encrypt the data blocks
        """
        if not isinstance(symmetric_key, SymmetricKey):
            raise(Exception('FileCryptor can only be constructed using a valid SymmetricKey object'))
        self.iv = base64.b64decode(iv)
        self.data_key = base64.b64decode(data_key)
        self.symmetric_key = symmetric_key.get_key_data()
        self.block_size = 32
        logging.debug('FileCryptor initialized with blocksize={bs}'.format(bs=self.block_size))

    def encrypt(self, s3file):
        raise(Exception('Not implemented at the moment '))

    def get_decrypted_data_key(self):
        """
        For envelop encryption the data key is encrypted with a generated key. This generated key itself will be
        encrypted with the provided symmetric key. In case of S3 ECB mode of AES256 is used. This method will return
        the decrypted key.
        Returns:
            bytes: Decrypted data key bytes.
        """
        cipher = Cipher(algorithms.AES(self.symmetric_key), modes.ECB(), backend=backend)
        decryptor = cipher.decryptor()
        padded_key = decryptor.update(self.data_key) + decryptor.finalize()
        key = S3EnvelopeFileCryptor.un_pad(padded_key)
        return key

    @staticmethod
    def un_pad(padded_bytes):
        """
        According to the documentation in the AWS Java SDK the content encryption key is AES/CBC/PKCS5Padding
        However that works only for block sizes of 64-bit which does not make sense as AES uses a block-size
        of 16 Bytes.
        Assuming that they actually use PKCS7 (https://en.wikipedia.org/wiki/Padding_(cryptography)#PKCS7)

        Args:
            padded_bytes:

        Returns:

        """
        logging.debug('Unpadding padded bytes {b}'.format(b=binascii.hexlify(padded_bytes)))
        logging.debug('Number of bytes: {bl}'.format(bl=len(padded_bytes)))
        logging.debug('Type of padded bytes: {bt}'.format(bt=type(padded_bytes)))

        pad_value = padded_bytes[len(padded_bytes) - 1]
        if isinstance(pad_value, int):
            logging.debug('Type of pad_value is int, value {v}'.format(v=str(pad_value)))
            return padded_bytes[:-pad_value]
        elif isinstance(pad_value, str):
            logging.debug(
                'Type of pad_value is str, length {len}, value {v}'.format(len=len(pad_value), v=ord(pad_value))
            )
            return padded_bytes[:-ord(pad_value)]
        else:
            logging.fatal(
                'Invalid type for pad_value: {t} with length {len}'.format(t=type(pad_value), len=len(pad_value))
            )
            return padded_bytes[:-pad_value]

    def pad(self, input_str):
        """
        Do PKCS7 padding (https://en.wikipedia.org/wiki/Padding_(cryptography)#PKCS7
        Args:
            input_str:

        Returns:

        """
        pad_value = self.block_size - len(input_str) % self.block_size
        return input_str + pad_value * chr(pad_value)

    def decrypt_file(self, input_file, output_file):
        # aes_crypt_handle = AES.new(self.get_decrypted_data_key(), AES.MODE_CBC, self.iv)
        cipher = Cipher(algorithms.AES(self.get_decrypted_data_key()), modes.CBC(self.iv), backend=backend)
        decryptor = cipher.decryptor()

        logging.debug('Starting decrypting loop')
        with open(input_file, 'rb') as in_file:
            with open(output_file, 'wb') as out_file:
                next_chunk = b''
                finished = False
                while not finished:
                    # chunk, next_chunk = next_chunk, aes_crypt_handle.decrypt(in_file.read(1024 * self.block_size))
                    chunk, next_chunk = next_chunk, decryptor.update(in_file.read(1024 * self.block_size))
                    if len(next_chunk) == 0:
                        chunk = S3EnvelopeFileCryptor.un_pad(chunk)
                        finished = True
                    out_file.write(chunk)
                out_file.write(decryptor.finalize())

    def decrypt_bytes(self, crypto_text):
        """
        
        Args:
            crypto_text(bytes): 

        Returns:
            bytes: plaintext
        """
        cipher = Cipher(algorithms.AES(self.get_decrypted_data_key()), modes.CBC(self.iv), backend=backend)
        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(crypto_text) + decryptor.finalize()
        return S3EnvelopeFileCryptor.un_pad(padded_plaintext)


class S3EnvelopeFileCryptor(EnvelopeFileCryptor):
    def __init__(self, symmetric_key, s3_transfer):
        """

        Args:
            symmetric_key:
            s3_transfer(S3FileTransfer):
        """
        s3file = s3_transfer.get_s3_file()
        s3file.get_encryption_metadata()
        logging.debug('Retrieved envelop_encryption_key.')
        super(S3EnvelopeFileCryptor, self).__init__(
            symmetric_key,
            iv=s3file.get_x_amz_iv(),
            data_key=s3file.get_x_amz_key()
        )

    def decrypt(self, s3_file_transfer):
        """
        Decrypt an S3FileTransfer target file

        Args:
            s3_file_transfer(S3FileTransfer): Contains the S3File to be decrypted and the target location

        Returns:

        """
        target_file = s3_file_transfer.get_local_file()
        logging.debug('Will decrypt to {tf}'.format(tf=target_file))

        input_file = s3_file_transfer.move_to_temp_location()
        logging.debug('Will use temp location to {tl}'.format(tl=input_file))

        self.decrypt_file(input_file, target_file)
        s3_file_transfer.cleanup_temp_file()
