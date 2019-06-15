from util.manifest import Manifest
from util.s3_file import S3File
import os


def test_manifest_in_logic_and_lengths():
    old_cwd = os.getcwd()
    cwd_changed = False
    if not old_cwd.endswith('test'):
        print('Current working directory = {cwd}'.format(cwd=old_cwd))
        os.chdir('{old_cwd}/test'.format(old_cwd=old_cwd))
        cwd_changed = True

    manifest = Manifest('./resources/files.manifest')
    file0 = S3File('manifest-tools', 'redshift/test/files0000_part_00')
    file1 = S3File('manifest-tools', 'redshift/test/files0001_part_00')
    file2 = S3File('manifest-tools', 'redshift/test/files0002_part_00')
    file3 = S3File('manifest-tools', 'redshift/test/files0003_part_00')

    assert file0 in manifest.s3_files, 'First file must be present in manifest'
    assert file1 in manifest.s3_files, 'Second file must be present in manifest'
    assert file2 in manifest.s3_files, 'Third file must be present in manifest'
    assert file3 in manifest.s3_files, 'Fourth file must be present in manifest'
    assert len(manifest.s3_files) == 4, 'There must be 4 files in the resulting manifest file.'
    if cwd_changed:
        os.chdir('{old_cwd}'.format(old_cwd=old_cwd))


def test_common_prefix_in_manifest():
    manifest = Manifest()
    first_path = 's3://manifest-tools/path/to/a/file'
    manifest.add_s3file(S3File(first_path))
    assert first_path == manifest.get_common_prefix(), 'Single entry results in common prefix=entry'
    manifest.add_s3file(S3File(first_path))
    assert first_path == manifest.get_common_prefix(), 'Duplicate entry results in common prefix=entry'
    manifest.add_s3file(S3File('s3://manifest-tools/path/to/another/file'))
    assert 's3://manifest-tools/path/to/a' == manifest.get_common_prefix(), \
        'Adding files with different prefix should only return common prefix'
    manifest.add_s3file(S3File('s3://manifest-tools2/path/to/another/file'))
    assert 's3://manifest-tools' == manifest.get_common_prefix(),\
        'Adding files with different bucket should only return common prefix'


def test_common_path_prefix_in_manifest():
    manifest = Manifest()
    first_path = 's3://manifest-tools/path/to/a/file'
    manifest.add_s3file(S3File(first_path))
    assert 's3://manifest-tools/path/to/a/' == manifest.get_common_path_prefix(), \
        'Single entry results in common prefix=entry'
    manifest.add_s3file(S3File(first_path))
    assert 's3://manifest-tools/path/to/a/' == manifest.get_common_path_prefix(), \
        'Duplicate entry results in common prefix=entry'
    manifest.add_s3file(S3File('s3://manifest-tools/path/to/another/file'))
    assert 's3://manifest-tools/path/to/' == manifest.get_common_path_prefix(), \
        'Adding files with different prefix should only return common prefix up untill path separator'
    manifest.add_s3file(S3File('s3://manifest-tools2/path/to/another/file'))
    assert 's3://' == manifest.get_common_path_prefix(),\
        'Adding files with different bucket should only return common path prefix (s3://)'
