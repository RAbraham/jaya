import unittest
from .handlers import echo_handler, copy_handler
from jaya import S3, AWSLambda, Pipeline, event
from jaya.deployment import deploy
import json
# from localstack.mock import infra
import copy
import jaya
from functools import partial

DEFAULT_REGION = 'us-east-1'
BUCKET1 = 'tsa-test-bucket1'
BUCKET2 = 'tsa-lambda-dest-bucket'
BUCKET3 = 'tsa-bucket3'
ENVIRONMENT = 'development'


# A_S3 = S3(BUCKET1, DEFAULT_REGION, on=[lambda_event(lambda_name)])


def lambda_event(lambda_name):
    return event(trigger=S3.ALL_CREATED_OBJECTS, service_name=lambda_name)


class DeployTestCase(unittest.TestCase):
    def setUp(self):
        # infra.start_infra(async=True, apis=['s3'])

        pass

    def tearDown(self):
        # infra.stop_infra()
        pass

    def test_single_s3_bucket(self):
        p = S3(BUCKET1, DEFAULT_REGION)

        piper = Pipeline('my-first', [p])

        info = deploy.deploy_info(piper, test_mode=True)
        exp_agg = deploy.init_aggregator()
        exp_agg[deploy.S3] = {BUCKET1: {deploy.REGION_NAME: DEFAULT_REGION}}

        self.assertEqual(exp_agg, info)

    def test_s3_lambda(self):
        pipeline_name = 'two-node-pipe'
        lambda_name = 'Echo1'
        qualified_lambda_name = deploy.lambda_name(pipeline_name, lambda_name, True)
        s1 = S3(BUCKET1, DEFAULT_REGION, events=[event(S3.ALL_CREATED_OBJECTS, service_name=lambda_name)])
        l1 = AWSLambda(lambda_name,
                       echo_handler,
                       DEFAULT_REGION,
                       alias=ENVIRONMENT,
                       virtual_environment_path=None,
                       role_name=None,
                       dependencies=[jaya])

        p = s1 >> l1
        piper = Pipeline(pipeline_name, [p])

        lambda_with_modified_name = deploy.copy_lambda(qualified_lambda_name, p.children()[0], copy.copy)

        info = deploy.deploy_info(piper, test_mode=True)

        exp_agg = deploy.init_aggregator()
        exp_agg[deploy.S3] = {BUCKET1: {deploy.REGION_NAME: DEFAULT_REGION}}
        exp_agg[deploy.LAMBDA] = {qualified_lambda_name: {deploy.LAMBDA_INSTANCE: lambda_with_modified_name,
                                                          deploy.S3_NOTIFICATION: {
                                                              BUCKET1: [event(S3.ALL_CREATED_OBJECTS,
                                                                              service_name=lambda_name)]}}}

        self.assertEqual(info, exp_agg)

    def test_s3_lambda_s3(self):
        copy_handler_partial = partial(copy_handler, {})

        lambda_name = 'CopyS3Lambda1'
        pipeline_name = 'three-node-pipe'
        qualified_lambda_name = deploy.lambda_name(pipeline_name, lambda_name, True)

        s1 = S3(BUCKET1, DEFAULT_REGION, events=[lambda_event(lambda_name)])
        l1 = AWSLambda(lambda_name,
                       copy_handler_partial,
                       DEFAULT_REGION,
                       alias=ENVIRONMENT,
                       dependencies=[jaya])
        s2 = S3(BUCKET2, DEFAULT_REGION)

        p = s1 >> l1 >> s2
        piper = Pipeline(pipeline_name, [p])

        lambda_with_modified_name = deploy.copy_lambda(qualified_lambda_name, p.children()[0].root(), copy.copy)

        exp_agg = deploy.init_aggregator()
        exp_agg[deploy.S3] = {BUCKET1: {deploy.REGION_NAME: DEFAULT_REGION},
                              BUCKET2: {deploy.REGION_NAME: DEFAULT_REGION}}
        exp_agg[deploy.LAMBDA] = {qualified_lambda_name: {deploy.LAMBDA_INSTANCE: lambda_with_modified_name,
                                                          deploy.S3_NOTIFICATION: {
                                                              BUCKET1: [event(S3.ALL_CREATED_OBJECTS,
                                                                              service_name=lambda_name)]}}}

        info = deploy.deploy_info(piper, test_mode=True)

        self.assertEqual(info, exp_agg)

    def test_non_qualified_names(self):
        copy_handler_partial = partial(copy_handler, {})

        lambda_name = 'CopyS3Lambda1'
        pipeline_name = 'three-node-pipe'
        dont_qualify_lambda_name = False
        qualified_lambda_name = deploy.lambda_name(pipeline_name, lambda_name, dont_qualify_lambda_name)
        s1 = S3(BUCKET1, DEFAULT_REGION, events=[lambda_event(lambda_name)])
        s2 = S3(BUCKET2, DEFAULT_REGION)
        l1 = AWSLambda(lambda_name,
                       copy_handler_partial,
                       DEFAULT_REGION,
                       alias=ENVIRONMENT,
                       dependencies=[jaya])

        p = s1 >> l1 >> s2
        piper = Pipeline(pipeline_name, [p])

        lambda_with_modified_name = deploy.copy_lambda(qualified_lambda_name, p.children()[0].root(), copy.copy)

        exp_agg = deploy.init_aggregator()
        exp_agg[deploy.S3] = {BUCKET1: {deploy.REGION_NAME: DEFAULT_REGION},
                              BUCKET2: {deploy.REGION_NAME: DEFAULT_REGION}}
        exp_agg[deploy.LAMBDA] = {qualified_lambda_name: {deploy.LAMBDA_INSTANCE: lambda_with_modified_name,
                                                          deploy.S3_NOTIFICATION: {
                                                              BUCKET1: [event(S3.ALL_CREATED_OBJECTS,
                                                                              service_name=lambda_name)]}}}

        info = deploy.deploy_info(piper, test_mode=True, qualify_lambda_name=dont_qualify_lambda_name)
        self.assertEqual(info, exp_agg)
        pass

    def test_unique_lambda_names(self):
        copy_handler_partial = partial(copy_handler, {})

        lambda_name = 'CopyS3Lambda1'
        pipeline_name = 'incorrect-pipe-with-multiple-lambdas-with-same-name'

        s1 = S3(BUCKET1, DEFAULT_REGION, events=[lambda_event(lambda_name)])
        s2 = S3(BUCKET2, DEFAULT_REGION, events=[lambda_event(lambda_name)])
        l1 = AWSLambda(lambda_name,
                       copy_handler_partial,
                       DEFAULT_REGION,
                       alias=ENVIRONMENT,
                       dependencies=[jaya])

        s3 = S3(BUCKET3, DEFAULT_REGION)

        same_named_lambda = AWSLambda(lambda_name,
                                      copy_handler_partial,
                                      DEFAULT_REGION,
                                      alias=ENVIRONMENT,
                                      dependencies=[jaya])
        p = s1 >> l1 >> s2 >> same_named_lambda >> s3
        piper = Pipeline(pipeline_name, [p])
        # ValueError: There are multiple lambdas in the pipeline named CopyS3Lambda1. Please change the names to be unique
        with self.assertRaises(ValueError) as cx:
            deploy.deploy_info(piper, test_mode=True)

    def test_subset_tree(self):
        echo_lambda = partial(AWSLambda,
                              handler_func=echo_handler,
                              region_name=DEFAULT_REGION,
                              alias=ENVIRONMENT,
                              dependencies=[jaya])

        lambda_name = 'Echo1'
        s1 = S3(BUCKET2, DEFAULT_REGION, events=[lambda_event(lambda_name)])
        s2 = S3(BUCKET2, DEFAULT_REGION)

        l1 = echo_lambda(name=lambda_name)
        l2 = echo_lambda(name='OtherEcho')

        inp_exp = [
            (s1, None),
            (l1, l1),
            (l2, None),
            (s1 >> l1, s1 >> l1),
            (s1 >> l2, None),
            (s1 >> l1 >> s2, s1 >> l1),
            (s1 >> l2 >> s2, None),
            (l1 >> s1, l1),
            (l2 >> s2, None)
        ]

        for inp, exp in inp_exp:
            tree = deploy.subset_tree(inp, None, lambda_name)
            self.assertEqual(tree, exp)

    # def test_multi_hop_node_localstack(self):
    #     region = 'us-east-1'
    #     environment = 'development'
    #     conf = config.get_aws_config(environment)
    #
    #     p = S3('tsa-rajiv-bucket1', region, on=[S3.ALL_CREATED_OBJECTS]) \
    #         >> CopyS3Lambda({}, region, environment) \
    #         >> S3('tsa-rajiv-bucket2', 'us-east-1')
    #
    #     piper = Pipeline('three-node-pipe', [p])
    #     info = deploy.create_deploy_stack_info(piper)
    #
    #     deploy.deploy_stack_info_localstack(conf, environment, info)

    # def test_multi_hop_node_moto(self):
    #     region = 'us-east-1'
    #     environment = 'development'
    #     conf = config.get_aws_config(environment)
    #
    #     source = 'tsa-rajiv-bucket1'
    #     destination = 'tsa-rajiv-bucket2'
    #     p = S3(source, region, on=[S3.ALL_CREATED_OBJECTS]) \
    #         >> CopyS3Lambda({}, region, environment) \
    #         >> S3(destination, 'us-east-1')
    #
    #     piper = Pipeline('three-node-pipe', [p])
    #
    #     # with test(piper) as test_harness:
    #     #     s3 = test_harness.s3()
    #     #     a_key = 'a_key'
    #     #     file_content = io.BytesIO(b'Hi Rajiv')
    #     #     s3.Bucket(source).put_object(Key=a_key, Body=file_content)
    #     #     obj = s3.Object(bucket_name=destination, key=a_key)
    #     #     response = obj.get()
    #     #     data = response['Body'].read()
    #     #     self.assertEqual(data, file_content.getvalue())
    #
    #     with test(piper) as test_harness:
    #         s3 = test_harness.s3()
    #         a_key = 'a_key'
    #         file_content = io.BytesIO(b'Hi Rajiv')
    #         s3.Bucket(source).put_object(Key=a_key, Body=file_content)
    #         obj = s3.Object(bucket_name=source, key=a_key)
    #         response = obj.get()
    #         data = response['Body'].read()
    #         self.assertEqual(data, file_content.getvalue())
    #
    #         # info = deploy.create_deploy_stack_info(piper)
    #         #
    #         # deploy.deploy_stack_info_localstack(conf, environment, info)
    #
    #         # def test_localstack(self):
    #         #     from localstack.utils.aws import aws_stack
    #         #     s3 = aws_stack.connect_to_resource('s3')
    #         #     bucket = 'test_bucket_lambda'
    #         #     key = 'test_lambda.zip'
    #         #     s3.create_bucket(Bucket=bucket)
    #         #     file_content = io.BytesIO(b'Hi rajiv')
    #         #     s3.Bucket(bucket).put_object(Key=key, Body=file_content)
    #         #
    #         #     obj = s3.Object(bucket_name=bucket, key=key)
    #         #     response = obj.get()
    #         #     data = response['Body'].read()
    #         #     self.assertEqual(data, file_content.getvalue())


def lambda_repr(a_lambda):
    copied_lambda = copy.copy(a_lambda)

    copied_lambda.__dict__['handler'] = testable_repr_for_partial_func(copied_lambda.__dict__['handler'])
    return copied_lambda


def pipeline_repr(info):
    copy_info = copy.copy(info)
    for a_lambda, a_instance in copy_info[deploy.LAMBDA].items():
        lambda_instance = a_instance[deploy.LAMBDA_INSTANCE]
        copy_info[deploy.LAMBDA][a_lambda][deploy.LAMBDA_INSTANCE] = lambda_repr(lambda_instance)

    return copy_info


def testable_repr_for_partial_func(p):
    return p.func, p.args, p.keywords


def dictify(a_dict):
    return json.loads(json.dumps(a_dict))


# TODO: Test empty tree

if __name__ == '__main__':
    unittest.main()
