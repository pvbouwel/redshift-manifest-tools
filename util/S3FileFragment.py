class S3FileFragment:
    def __init__(self, data, length, requested_byte_range):
        self.data = data
        self.length = length
        self.requested_byte_range = requested_byte_range

    def get_streaming_body(self):
        return self.data

    def get_size(self):
        return self.length
