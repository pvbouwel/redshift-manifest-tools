from util.s3_file import S3File


def test_parsing_of_s3_paths():
    s3_file1 = S3File('s3://mytest.bucket/path/to/key')
    s3_file2 = S3File('mytest.bucket', 'path/to/key')
    assert s3_file1 == s3_file2


def test_get_file_name_of_s3_file():
    test_file = S3File('s3://mytest.bucket/path/to/key.csv')
    assert 'key.csv' == test_file.get_s3_file_name()
