from util.S3File import S3File
import json


class Manifest:
    s3_files = []

    def __init__(self, *args, **kwargs):
        """
        
        :param args: can be manifest_path
        :param kwargsmanifest_path: 
          - manifest_path = path to manifest
          - manifest_json_string
        """
        if len(args) == 1:
            # should be manifest_path
            self.manifest_path = args[0]

        if 'manifest_path' in kwargs:
            self.manifest_path = kwargs['manifest_path']

        manifest_json = {'entries': []}

        if hasattr(self, 'manifest_path'):
            with open(self.manifest_path, 'r') as manifest_file:
                manifest_json = json.load(manifest_file)
        elif 'manifest_json_string' in kwargs:
            manifest_json = json.loads(kwargs['manifest_json_string'])

        self.region = kwargs.get('region', None)

        self.s3_files = []
        for entry in manifest_json['entries']:
            self.s3_files.append(S3File(entry['url'], region=self.region))

    def add_s3file(self, s3file):
        if isinstance(s3file, S3File):
            self.s3_files.append(s3file)
        else:
            raise(TypeError('Cannot add {o} to manifest since not of type S3File.'.format(o=str(s3file))))

    def set_region(self, region):
        for s3file in self.s3_files:
            s3file.set_region(region)
        self.region = region

    @staticmethod
    def _get_common_start_strings(str1, str2):
        common_prefix = ''
        for index in range(0, len(str1)):
            if str1[index]==str2[index]:
                common_prefix += str1[index]
            else:
                return common_prefix
        return common_prefix

    def get_common_prefix(self):
        """
        If in the manifest there are multiple files get the part is common for all entries
        E.g. given the entries:
          s3://bucket1/path/to/a/file
          s3://bucket1/path/to/another/file
         
         this would return s3://bucket1/path/to/
        :return: 
        """
        if len(self.s3_files) == 0:
            return ''
        else:
            common_prefix = str(self.s3_files[0])
            for file in self.s3_files[1:]:
                common_prefix = Manifest._get_common_start_strings(common_prefix, str(file))
        return common_prefix

    def get_common_path_prefix(self):
        common_prefix = self.get_common_prefix()
        if common_prefix.endswith('/'):
            return common_prefix
        else:
            index_last_slash = common_prefix.rfind('/')
            return common_prefix[0:index_last_slash+1]

    def __contains__(self, item):
        for s3file in self.s3_files:
            if s3file == item:
                return True
        return False

    def __len__(self):
        return len(self.s3_files)