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

        if 'manifest_path' in self:
            with open(self.manifest_path, 'r') as manifest_file:
                manifest_json = json.load(manifest_file)
        elif 'manifest_json_string' in kwargs:
            manifest_json = json.loads(kwargs['manifest_json_string'])

        self.s3_files = []
        for entry in manifest_json['entries']:
            self.s3_files.append(S3File(entry['url']))

    def __contains__(self, item):
        for s3file in self.s3_files:
            if s3file == item:
                return True
        return False

    def __len__(self):
        return len(self.s3_files)