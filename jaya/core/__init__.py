from jaya.pipeline.pipe import Leaf
from . import aws


class S3(Leaf):
    ALL_CREATED_OBJECTS = 's3:ObjectCreated:*'

    def __init__(self, bucket, region_name, on=None):
        if not on:
            on = []
        assert type(on) == list, 'on should be of type list'

        # TODO: If region_name is made optional, then it should be US Standard or whatever the default is?
        self.service = Service(aws.S3)
        self.bucket = bucket
        self.region_name = region_name
        self.on = on
        super(S3, self).__init__([bucket, region_name, on])


class AWSLambda(Leaf):
    def __init__(self, name, handler, dependencies):
        self.handler = handler
        self.name = name
        self.dependency_paths = [module_path(d) for d in dependencies]
        super(AWSLambda, self).__init__([self.name, self.handler, self.dependency_paths])


class Service(Leaf):
    def __init__(self, service_name):
        self.name = service_name


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
