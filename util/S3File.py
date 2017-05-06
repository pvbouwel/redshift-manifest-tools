import re
s3 = None
import click


class InvalidS3PathException(Exception):
    def __init__(self, message, line):
        super(AssertionError, self).__init__(message)
        self.line = line


class S3File:
    def __init__(self, *args, **kwargs):
        s3path = None
        if len(args) == 1:
            # 1 arguments given should be s3path
            s3path = args[0]
        elif len(args) == 2:
            # 2 arguments given should be bucket and key
            self.bucket_name = args[0]
            self.key = args[1]
        elif len(args) == 0 and len(kwargs) != 0:
            if 'bucket_name' in kwargs:
                self.bucket_name = kwargs['bucket_name']
            if 'key' in kwargs:
                self.key = kwargs['key']
            if 's3path' in kwargs:
                s3path = kwargs['s3path']
        else:
            raise Exception('Unsupported initialization of S3File')

        if s3path is not None:
            if not s3path.startswith('s3://'):
                raise InvalidS3PathException('S3 path did not start with \'s3://\': {path}'.format(path=s3path))
            #Bucket restrictions according to AWS doc
            # 3-63 characters long
            # One or more labels separated by a .
            # lowercase, numbers hyphens (must start with number or lowercase
            # North virginia region can have Upper cases as wel
            s3path_structure = [
                {
                    'name': 'prefix',
                    'pattern': 's3://'
                },
                {
                    'name': 'bucket',
                    'pattern': '[a-zA-Z0-9.-]*'
                },
                {
                    'name': 'separator',
                    'pattern': '/'
                },
                {
                    'name': 'key',
                    'pattern': '.*'
                }
            ]
            pattern = ''
            for element in s3path_structure:
                if 'name' in element.keys():
                    pattern += '(?P<' + element['name'] + '>'
                pattern += element['pattern']
                if 'name' in element.keys():
                    pattern += ')'
            regex_s3_path = re.compile(pattern)
            match_result = regex_s3_path.match(s3path)
            if match_result is None:
                raise InvalidS3PathException('Could not parse S3 path. Is a valid s3 path given? {path}'.format(path=s3path))
            self.bucket_name = match_result.groupdict().pop('bucket')
            self.key = match_result.groupdict().pop('key')

    def get_key(self):
        return self.key

    def get_bucket(self):
        return self.bucket_name

    def get_s3_file_name(self):
        s3_file_name_parts = self.key.split('/')
        return s3_file_name_parts[-1]

    def __eq__(self, other):
        return self.key == other.key and self.bucket_name == other.bucket_name

    def __str__(self):
        return 's3://{bucket}/{key}'.format(bucket=self.bucket_name, key=self.key)


class S3PathParamType(click.ParamType):
    name = 's3-path'

    def convert(self, value, param, ctx):
        try:
            return S3File(value)
        except InvalidS3PathException as e:
            self.fail('{val} is not a valid S3 path: {err}'.format(val=value,err=str(e)), param, ctx)