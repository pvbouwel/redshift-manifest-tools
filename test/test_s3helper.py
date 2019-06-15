from test import symmetric_base64_aes256_key
from util.s3_helper import S3Helper
from util.s3_file import S3File
from util.s3_file_transfer import S3FileTransfer
from util.symmetric_key import SymmetricKey
from util.exceptions import DuplicateLocalFileException, LocalFileExistsAndConflictsWithTargetFileAndNoOverWrite
import tempfile
import sys
import os
import hashlib


def test_retrieve_manifest():
    s3file_manifest = S3File('s3://manifest-tools/gzip-unload/stv_blocklist_unloadmanifest')
    manifest = S3Helper.retrieve_manifest(s3file_manifest)
    assert len(manifest) == 4
    assert S3File('s3://manifest-tools/gzip-unload/stv_blocklist_unload0000_part_00.gz') in manifest
    assert S3File('s3://manifest-tools/gzip-unload/stv_blocklist_unload0001_part_00.gz') in manifest
    assert S3File('s3://manifest-tools/gzip-unload/stv_blocklist_unload0002_part_00.gz') in manifest
    assert S3File('s3://manifest-tools/gzip-unload/stv_blocklist_unload0003_part_00.gz') in manifest


def test_retrieve_manifest_euw1():
    s3file_manifest = S3File('s3://manifest-tools-euw1/test_manifest/test-euw1.manifest')
    manifest = S3Helper.retrieve_manifest(s3file_manifest)
    assert len(manifest) == 2
    assert S3File('s3://manifest-tools-euw1/test_manifest/file1') in manifest
    assert S3File('s3://manifest-tools-euw1/test_manifest/file2') in manifest


def test_retrieve_file_with_boto3_transfer_logic():
    test_description = 'Check if file gets retrieved correctly'
    s3file = S3File('s3://manifest-tools/multi-chunk/502bytesfile')
    temp_dir = tempfile.TemporaryDirectory()
    temp_file = os.path.join(temp_dir.name, s3file.get_s3_file_name())

    s3_file_transfer = S3FileTransfer(s3file, temp_file)
    S3Helper.retrieve_file(s3_file_transfer)

    buffer_size = 1024
    md5 = hashlib.md5()

    with open(temp_file, 'rb') as tf:
        while True:
            data = tf.read(buffer_size)
            if not data:
                break
            md5.update(data)
    os.remove(temp_file)
    assert str(md5.hexdigest()) == 'efdd302407789f38cf07811fcece6fbd', test_description


def test_retrieve_file_with_cat_functionality_multiple_fetches():
    test_description = 'Check if file gets catted correctly'
    s3file = S3File('s3://manifest-tools/multi-chunk/502bytesfile')
    temp_dir = tempfile.TemporaryDirectory()
    temp_file = os.path.join(temp_dir.name, s3file.get_s3_file_name())
    temp_file_handle = open(temp_file, 'wb')
    S3Helper.set_stdout(temp_file_handle)
    s3_file_transfer = S3FileTransfer(s3file, None)
    S3Helper.retrieve_file(s3_file_transfer, bytes_per_fetch=100)
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
    assert str(md5.hexdigest()) == 'efdd302407789f38cf07811fcece6fbd', test_description


def test_cat_should_be_gzip_agnostic_if_file_not_too_big():
    test_description = 'If a file can be retrieved in a single round trip than we will automatically gunzip it if ' \
                       'it has the .gz extension'
    s3file_manifest = S3File('s3://manifest-tools/simple_unload_gzipped/simple_unload.manifest')
    temp_dir = tempfile.TemporaryDirectory()
    temp_file = os.path.join(temp_dir.name, 'gzip_test')
    temp_file_handle = open(temp_file, 'wb')
    S3Helper.set_stdout(temp_file_handle)
    S3Helper.retrieve_files_from_manifest_file(s3file_manifest, None)
    temp_file_handle.close()

    buffer_size = 1024
    md5 = hashlib.md5()

    with open(temp_file, 'rb') as tf:
        while True:
            data = tf.read(buffer_size)
            if not data:
                break
            md5.update(data)
    os.remove(temp_file)
    gzipped_md5_hash = md5.hexdigest()

    s3file_manifest = S3File('s3://manifest-tools/simple_unload_raw/simple_unload.manifest')
    temp_dir = tempfile.TemporaryDirectory()
    temp_file = os.path.join(temp_dir.name, 'raw_test')
    temp_file_handle = open(temp_file, 'wb')
    S3Helper.set_stdout(temp_file_handle)
    S3Helper.retrieve_files_from_manifest_file(s3file_manifest, None)
    temp_file_handle.close()

    buffer_size = 1024
    md5 = hashlib.md5()

    with open(temp_file, 'rb') as tf:
        while True:
            data = tf.read(buffer_size)
            if not data:
                break
            md5.update(data)
    os.remove(temp_file)
    raw_md5_hash = md5.hexdigest()
    S3Helper.set_stdout(sys.stdout)

    assert str(gzipped_md5_hash) == str(raw_md5_hash), test_description


def test_retrieve_file_with_exactly_1_fetch():
    test_description = 'Check if file gets catted correctly'
    s3file = S3File('s3://manifest-tools/multi-chunk/502bytesfile')
    temp_dir = tempfile.TemporaryDirectory()
    temp_file = os.path.join(temp_dir.name, s3file.get_s3_file_name())
    temp_file_handle = open(temp_file, 'wb')
    S3Helper.set_stdout(temp_file_handle)
    s3_file_transfer = S3FileTransfer(s3file, None)
    S3Helper.retrieve_file(s3_file_transfer, bytes_per_fetch=502)
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
    assert str(md5.hexdigest()) == 'efdd302407789f38cf07811fcece6fbd', test_description


def test_encrypted_file_should_have_encryption_metadata():
    s3file_encrypted = S3File('s3://manifest-tools/encrypted/encrypted.0000_part_00')
    s3file_encrypted.get_encryption_metadata()
    iv = 'neQfEbChkTO40WbwReDYlQ=='
    x_amz_key = '1/6I/Hck8wBmICcL+/4a681K2oabZ/rYnHhIbxVqZcEwdhQZrG9aIvkXto/NyKzf'
    assert iv == s3file_encrypted.x_amz_iv
    assert x_amz_key == s3file_encrypted.x_amz_key


def test_same_dataset_encrypted_plaintext_should_give_same_results():
    """
    Same table is unloaded twice, once encrypted, once in plaintext:
     - s3://manifest-tools/encrypted/encrypted.manifest
     - s3://manifest-tools/encrypted/plaintext.manifest
    The resulting files should be the same
    """
    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_enc = os.path.join(temp_dir.name, 'encrypted')
    temp_dir_plain = os.path.join(temp_dir.name, 'plaintext')
    os.mkdir(os.path.join(temp_dir_enc))
    os.mkdir(os.path.join(temp_dir_plain))

    s3manifest_encrypted = S3File('s3://manifest-tools/encrypted/encrypted.manifest')
    s3manifest_plaintext = S3File('s3://manifest-tools/encrypted/plaintext.manifest')
    sk = SymmetricKey(symmetric_base64_aes256_key)
    S3Helper.retrieve_files_from_manifest_file(s3manifest_encrypted, temp_dir_enc, symmetric_key=sk)
    S3Helper.retrieve_files_from_manifest_file(s3manifest_plaintext, temp_dir_plain)

    for file in os.listdir(temp_dir_enc):
        enc_file = os.path.join(temp_dir_enc, file)
        plain_file = enc_file.replace('encrypted', 'plaintext')
        with open(enc_file, 'rb') as enc_file_handle:
            with open(plain_file, 'rb') as plain_file_handle:
                assert enc_file_handle.readlines() == plain_file_handle.readlines()
        os.remove(enc_file)
        os.remove(plain_file)
    os.rmdir(temp_dir_enc)
    os.rmdir(temp_dir_plain)


def test_get_same_file_with_different_paths_from_manifest_no_overwrite_no_flattening():
    """
    Use manifest that has S3 files:
     - s3://manifest-tools/multi-path/path1/file
     - s3://manifest-tools/multi-path/path2/file

    no overwrite is allowed
    no flattening means we expect the different paths to be kept relative with respect to their common prefix:
     - path1/file
     - path2/file
    :return:
    """
    s3manifest = S3File('s3://manifest-tools/multi-path/multi-path.manifest')
    temp_dir = tempfile.TemporaryDirectory()
    S3Helper.retrieve_files_from_manifest_file(s3manifest, temp_dir.name)
    file1 = os.path.join(temp_dir.name, 'path1', 'file')
    file2 = os.path.join(temp_dir.name, 'path2', 'file')
    assert os.path.isfile(file1) and os.path.isfile(file2), 'no_overwrite_and_no_flattening_path_check'
    os.remove(file1)
    os.remove(file2)


def test_get_same_file_with_different_paths_from_manifest_no_overwrite_with_flatten_paths():
    """
    Use manifest that has S3 files:
     - s3://manifest-tools/multi-path/path1/file
     - s3://manifest-tools/multi-path/path2/file

    no overwrite is allowed
    flattening means we expect to only use only the last part of the key (no '/' allowed):
     - file
     - file
     This should raise an exception
    :return:
    """
    s3manifest = S3File('s3://manifest-tools/multi-path/multi-path.manifest')
    temp_dir = tempfile.TemporaryDirectory()
    try:
        S3Helper.retrieve_files_from_manifest_file(s3manifest, temp_dir.name, flatten_paths=True)
        assert False
    except DuplicateLocalFileException:
        assert True

    file1 = os.path.join(temp_dir.name, 'path1', 'file')
    file2 = os.path.join(temp_dir.name, 'path2', 'file')
    if os.path.isfile(file1):
        os.remove(file1)
    if os.path.isfile(file2):
        os.remove(file2)


def test_get_same_file_with_different_paths_from_manifest_overwrite_no_flattening():
    """
    Use manifest that has S3 files:
     - s3://manifest-tools/multi-path/path1/file
     - s3://manifest-tools/multi-path/path2/file

    overwrite is allowed
    no flattening means we expect the different paths to be kept relative with respect to their common prefix:
     - path1/file
     - path2/file

     Here we create a file with content test and verify that it is overwritten as expected
    :return:
    """
    s3manifest = S3File('s3://manifest-tools/multi-path/multi-path.manifest')
    temp_dir = tempfile.TemporaryDirectory()
    file1 = os.path.join(temp_dir.name, 'path1', 'file')
    file2 = os.path.join(temp_dir.name, 'path2', 'file')
    parent_dir = os.path.join(temp_dir.name, 'path2')
    os.mkdir(parent_dir)

    with open(file2, 'w') as test_file:
        test_file.write('test')

    S3Helper.retrieve_files_from_manifest_file(s3manifest, temp_dir.name, overwrite=True)

    assert os.path.isfile(file1) and os.path.isfile(file2), 'overwrite_and_no_flattening_path_check'
    with open(file2, 'r') as test_file:
        content = test_file.readline().strip()

    assert content != 'test', \
        'Test that content is changed after fetching file and overwriting existing files'
    os.remove(file1)
    os.remove(file2)


def test_get_file_to_path_that_has_that_file_with_no_overwrite():
    """
    Use manifest that has S3 files:
     - s3://manifest-tools/multi-path/path1/file
     - s3://manifest-tools/multi-path/path2/file

    overwrite is not allowed
    no flattening means we expect the different paths to be kept relative with respect to their common prefix:
     - path1/file
     - path2/file

     Here we create a file with content test and verify that it is overwritten as expected
    :return:
    """
    s3manifest = S3File('s3://manifest-tools/multi-path/multi-path.manifest')
    temp_dir = tempfile.TemporaryDirectory()
    parent_dir = os.path.join(temp_dir.name, 'path2')
    os.mkdir(parent_dir)
    file1 = os.path.join(temp_dir.name, 'path1', 'file')
    file2 = os.path.join(temp_dir.name, 'path2', 'file')

    with open(file2, 'w') as test_file:
        test_file.write('test')

    try:
        S3Helper.retrieve_files_from_manifest_file(s3manifest, temp_dir.name, overwrite=False)
    except LocalFileExistsAndConflictsWithTargetFileAndNoOverWrite:
        assert True

    if os.path.isfile(file1):
        os.remove(file1)
    if os.path.isfile(file2):
        os.remove(file2)
