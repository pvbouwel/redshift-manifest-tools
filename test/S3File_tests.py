import unittest
from util.S3File import S3File


class MyTestCase(unittest.TestCase):
    def test_parsing_of_s3_paths(self):
        s3File1 = S3File('s3://mytest.bucket/path/to/key')
        s3File2 = S3File('mytest.bucket', 'path/to/key')
        self.assertEqual(s3File1, s3File2)

    def test_get_file_name_of_s3_file(self):
        test_file = S3File('s3://mytest.bucket/path/to/key.csv')
        self.assertEqual('key.csv', test_file.get_s3_file_name())


if __name__ == '__main__':
    unittest.main()
