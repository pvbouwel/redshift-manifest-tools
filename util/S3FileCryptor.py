from util.SymmetricKey import SymmetricKey
from Crypto.Cipher import AES
import base64
import logging
import binascii


class S3FileCryptor:
    def __init__(self, symmetric_key=None):
        if not isinstance(symmetric_key, SymmetricKey):
            raise(Exception('S3FileCryptor can only be constructed using a valid SymmetricKey object'))
        self.symmetric_key = symmetric_key
        self.block_size = 32
        logging.debug('S3FileCryptor initialized with blocksize={bs}'.format(bs=self.block_size))

    def encrypt(self, s3file):
        raise(Exception('Not implemented at the moment '))

    def get_decryption_materials(self, s3file):
        """
        Given an S3File object get materials to decrypt the file content
        :param s3file: 
        :return: 
        """
        s3file.get_encryption_metadata()
        logging.debug('Get x-amz-iv')
        iv = base64.b64decode(s3file.get_x_amz_iv())
        logging.debug('Get x-amz-iv = {iv}'.format(iv=iv))

        envelop_encryption_key = self.symmetric_key.get_key_data()
        logging.debug('Retrieved envelop_encryption_key.')
        aes_envelope = AES.new(envelop_encryption_key, mode=AES.MODE_ECB)
        x_amz_key = base64.b64decode(s3file.get_x_amz_key())
        padded_key = aes_envelope.decrypt(x_amz_key)
        key = S3FileCryptor.un_pad(padded_key)
        logging.debug('Returning IV and Key to decrypt the data')
        return [iv, key]

    def decrypt(self, s3_file_transfer):
        """
        Decrypt an S3FileTransfer target file
        :param s3_file_transfer: S3FileTransfer object for the file to be decrypted
        :return: None
        """
        s3file = s3_file_transfer.get_s3_file()
        logging.debug('Retrieve decryption materials')
        [iv, key] = self.get_decryption_materials(s3file)

        target_file = s3_file_transfer.get_local_file()
        logging.debug('Will decrypt to {tf}'.format(tf=target_file))

        input_file = s3_file_transfer.move_to_temp_location()
        logging.debug('Will use temp location to {tl}'.format(tl=input_file))

        aes_crypt_handle = AES.new(key, AES.MODE_CBC, iv)

        logging.debug('Starting decrypting loop')
        with open(input_file, 'rb') as in_file:
            with open(target_file, 'wb') as out_file:
                next_chunk = b''
                finished = False
                while not finished:
                    chunk, next_chunk = next_chunk, aes_crypt_handle.decrypt(in_file.read(1024 * self.block_size))
                    if len(next_chunk) == 0:
                        chunk = S3FileCryptor.un_pad(chunk)
                        finished = True
                    out_file.write(chunk)
        s3_file_transfer.cleanup_temp_file()

    @staticmethod
    def un_pad(padded_bytes):
        """
        According to the documentation in the AWS Java SDK the content encryption key is AES/CBC/PKCS5Padding
        However that works only for block sizes of 64-bit which does not make sense as AES uses a block-size 
        of 16 Bytes.
        Assuming that they actually use PKCS7 (https://en.wikipedia.org/wiki/Padding_(cryptography)#PKCS7)
        :param padded_bytes : 
        :return: 
        """
        logging.debug('Unpadding padded bytes {b}'.format(b=binascii.hexlify(padded_bytes)))
        logging.debug('Number of bytes: {bl}'.format(bl=len(padded_bytes)))
        logging.debug('Type of padded bytes: {bt}'.format(bt=type(padded_bytes)))

        pad_value = padded_bytes[len(padded_bytes)-1]
        if isinstance(pad_value, int):
            logging.debug('Type of pad_value is int, value {v}'.format(v=str(pad_value)))
            return padded_bytes[:-pad_value]
        elif isinstance(pad_value, str):
            logging.debug('Type of pad_value is str, length {l}, value {v}'.format(l=len(pad_value),v=ord(pad_value)))
            return padded_bytes[:-ord(pad_value)]
        else:
            logging.fatal('Invalid type for pad_value: {t} with length {l}'.format(t=type(pad_value), l=len(pad_value)))
            return padded_bytes[:-pad_value]

    def pad(self, input_str):
        """
        Do PKCS7 padding (https://en.wikipedia.org/wiki/Padding_(cryptography)#PKCS7
        :param input_str: 
        :param block_size: 
        :return: 
        """
        pad_value = self.block_size - len(input_str) % self.block_size
        return input_str + pad_value * chr(pad_value)
