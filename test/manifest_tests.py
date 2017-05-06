import unittest
from util.Manifest import Manifest
from util.S3File import S3File


class MyTestCase(unittest.TestCase):
    def test_something(self):
        manifest = Manifest('./resources/files.manifest')
        file0 = S3File('support-peter-ie', 'redshift/test/files0000_part_00')
        file1 = S3File('support-peter-ie', 'redshift/test/files0001_part_00')
        file2 = S3File('support-peter-ie', 'redshift/test/files0002_part_00')
        file3 = S3File('support-peter-ie', 'redshift/test/files0003_part_00')

        self.assertIn(file0, manifest.s3_files, 'First file must be present in manifest')
        self.assertIn(file1, manifest.s3_files, 'Second file must be present in manifest')
        self.assertIn(file2, manifest.s3_files, 'Third file must be present in manifest')
        self.assertIn(file3, manifest.s3_files, 'Fourth file must be present in manifest')
        self.assertEqual(True, len(manifest.s3_files) == 4, 'There must be 4 files in the resulting manifest file.')


if __name__ == '__main__':
    unittest.main()
