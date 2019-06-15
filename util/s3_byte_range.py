class S3ByteRange:
    def __init__(self, size=10000000):
        self.size = size
        self.lower_bound = 0
        self.upper_bound = size - 1

    def __str__(self):
        return 'bytes={lb}-{ub}'.format(lb=str(self.lower_bound), ub=str(self.upper_bound))

    def next(self):
        self.lower_bound += self.size
        self.upper_bound += self.size
        return str(self)
