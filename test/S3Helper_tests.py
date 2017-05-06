import unittest
from util.S3Helper import S3Helper
from util.S3File import S3File
import tempfile
import sys
import os
import hashlib

class MyTestCase(unittest.TestCase):
    def test_retrieve_manifest(self):
        s3helper = S3Helper('eu-west-1')
        s3file_manifest = S3File('s3://redshift-manifest-tools/gzip-unload/stv_blocklist_unloadmanifest')
        manifest = S3Helper.retrieve_manifest(s3file_manifest)
        self.assertEqual(len(manifest), 4)
        self.assertIn(S3File('s3://redshift-manifest-tools/gzip-unload/stv_blocklist_unload0000_part_00.gz'), manifest)
        self.assertIn(S3File('s3://redshift-manifest-tools/gzip-unload/stv_blocklist_unload0001_part_00.gz'), manifest)
        self.assertIn(S3File('s3://redshift-manifest-tools/gzip-unload/stv_blocklist_unload0002_part_00.gz'), manifest)
        self.assertIn(S3File('s3://redshift-manifest-tools/gzip-unload/stv_blocklist_unload0003_part_00.gz'), manifest)

    def test_retrieve_file_with_boto3_transfer_logic(self):
        test_description = 'Check if file gets retrieved correctly'
        s3helper = S3Helper('eu-west-1')
        s3file = S3File('s3://redshift-manifest-tools/multi-chunk/502bytesfile')
        temp_dir = tempfile.TemporaryDirectory()
        S3Helper.retrieve_file(s3file, temp_dir.name)
        temp_file =os.path.join(temp_dir.name, s3file.get_s3_file_name())

        buffer_size = 1024
        md5 = hashlib.md5()

        with open(temp_file, 'rb') as tf:
            while True:
                data = tf.read(buffer_size)
                if not data:
                    break
                md5.update(data)
        os.remove(temp_file)
        self.assertEqual(str(md5.hexdigest()),'efdd302407789f38cf07811fcece6fbd', test_description)

    def test_retrieve_file_with_cat_functionality_multiple_fetches(self):
        test_description = 'Check if file gets catted correctly'
        s3helper = S3Helper('eu-west-1')
        s3file = S3File('s3://redshift-manifest-tools/multi-chunk/502bytesfile')
        temp_dir = tempfile.TemporaryDirectory()
        temp_file = os.path.join(temp_dir.name, s3file.get_s3_file_name())
        temp_file_handle = open(temp_file, 'wb')
        S3Helper.set_stdout(temp_file_handle)
        S3Helper.retrieve_file(s3file, None, bytes_per_fetch=100)
        temp_file_handle.close()
        S3Helper.set_stdout(sys.stdout)

        buffer_size = 1024
        md5 = hashlib.md5()

        with open(temp_file, 'rb') as tf:
            while True:
                data = tf.read(buffer_size)
                if not data:
                    break
                md5.update(data)
        os.remove(temp_file)
        self.assertEqual(str(md5.hexdigest()), 'efdd302407789f38cf07811fcece6fbd', test_description)

    def test_retrieve_file_with_exactly_1_fetch(self):
        test_description = 'Check if file gets catted correctly'
        s3helper = S3Helper('eu-west-1')
        s3file = S3File('s3://redshift-manifest-tools/multi-chunk/502bytesfile')
        temp_dir = tempfile.TemporaryDirectory()
        temp_file = os.path.join(temp_dir.name, s3file.get_s3_file_name())
        temp_file_handle = open(temp_file, 'wb')
        S3Helper.set_stdout(temp_file_handle)
        S3Helper.retrieve_file(s3file, None, bytes_per_fetch=502)
        temp_file_handle.close()
        S3Helper.set_stdout(sys.stdout)

        buffer_size = 1024
        md5 = hashlib.md5()

        with open(temp_file, 'rb') as tf:
            while True:
                data = tf.read(buffer_size)
                if not data:
                    break
                md5.update(data)
        os.remove(temp_file)
        self.assertEqual(str(md5.hexdigest()), 'efdd302407789f38cf07811fcece6fbd', test_description)


if __name__ == '__main__':
    unittest.main()
