from jaya.pipeline.pipe import Leaf


class S3(Leaf):
    ALL_CREATED_OBJECTS = 's3:ObjectCreated:*'

    def __init__(self, bucket, aws_region, on=None):
        self.bucket = bucket
        self.aws_region = aws_region
        self.on = on
        super(S3, self).__init__([bucket, aws_region, on])


class AWSLambda(object):
    def __init__(self, name, handler, dependencies):
        self.handler = handler
        self.name = name
        self.dependency_paths = [module_path(d) for d in dependencies]


class Pipeline(object):
    def __init__(self, name, pipes):
        self.name = name
        self.pipes = pipes


def module_path(file_or_dir_module):
    if '__path__' in file_or_dir_module.__dict__:
        return file_or_dir_module.__path__[0]
    elif '__file__' in file_or_dir_module.__dict__:
        return file_or_dir_module.__file__
    else:
        raise ValueError('Is {} a module at all??'.format(str(file_or_dir_module)))
