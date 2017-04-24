from jaya.pipeline.pipe import Leaf


class S3(Leaf):
    ALL_CREATED_OBJECTS = 's3:ObjectCreated:*'

    def __init__(self, bucket, aws_region, on=None):
        self.bucket = bucket
        self.aws_region = aws_region
        self.on = on
        super(S3, self).__init__([bucket, aws_region, on])


class Dummy(Leaf):
    pass


# class AWSLambda(object):
#     def __init__(self, name, handler_func, import_statements=[], variables=[], functions=[], classes=[]):
#         self.handler_func = handler_func
#         self.name = name
#         self.import_statements = import_statements
#         self.variables = variables
#         self.functions = functions
#         self.classes = classes
#
#     def code_as_strs(self):
#         handler_func_str = self.code_str(self.handler_func)
#
#         import_statement_strs = [str(imp_stmt) for imp_stmt in self.import_statements]
#         variable_strs = [str(v) for v in self.variables]
#         func_strs = [self.code_str(f) for f in self.functions]
#         class_strs = [self.code_str(c) for c in self.classes]
#         return import_statement_strs + variable_strs + func_strs + class_strs + [handler_func_str]
#
#     @staticmethod
#     def code_str(code):
#         return dedent(inspect.getsource(code))

class AWSLambda(object):
    def __init__(self, name, handler):
        self.handler = handler
        self.name = name
