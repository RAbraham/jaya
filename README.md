# jaya
[Proof of Concept] [Not Production Ready] Write, Maintain, Unit Test, Deploy AWS Data Pipelines in Python


# Features
## Readability
Currently, we can specify our deployment as a `JSON` dictionary. For a very simple pipeline, check out the PSEUDO ABSOLUTELY INCORRECT CloudFormation JSON Dict
 
```json
{
    "AWSTemplateFormatVersion": "2010-09-09",

    "Resources": {
        "CopyRajiv": {
            "Type": "AWS::Lambda::Function",
            "Properties": {
                "Code": {
                    "S3Bucket": "thescore-tmp",
                    "S3Key": "CopyS3Lambda"
                },
                "FunctionName": "CopyS3Lambda",
                "Handler": "lambda.handler",
                "Runtime": "python3.6",
                "Timeout": 300,
                "Role": "arn:aws:iam::666:role/lambda_s3_exec_role",
            }
        },
        "SrcBucket": {
            "Type": "AWS::S3::Bucket",
            "Properties": {
                "BucketName": "tsa-rajiv-bucket1",
                "NotificationConfiguration": {
                    "LambdaConfigurations": [{
                        "Function": {"Ref": "CopyRajiv"},
                        "Event": "s3:ObjectCreated:*"
                    }]
                }
            }
        },
        "DestBucket": {
            "Type": "AWS::S3::Bucket",
            "Properties": {
                "BucketName": "tsa-rajiv-bucket2"
            }
        },
        "AliasForMyApp": {
            "Type": "AWS::Lambda::Alias",
            "Properties": {
                "FunctionName": "CopyRajiv",
                "FunctionVersion": "$LATEST",
                "Name": "staging"
            }
        },
        "LambdaInvokePermission": {
            "Type": "AWS::Lambda::Permission",
            "Properties": {
                "FunctionName": {"Fn::GetAtt": ["AliasForMyApp", "Arn"]},
                "Action": "lambda:InvokeFunction",
                "Principal": "s3.amazonaws.com",
                "SourceArn": {"Ref": "SrcBucket"}
            }
        }

    }

}

```

What if we could capture the same intent in Python: 

```python
conf = .. AWS Key etc..
s1 = S3("tsa-rajiv-bucket1", 'us-east-1', on=[S3.ALL_CREATED_OBJECTS])
l1 = CopyS3Lambda({}, 'us-east-1', 'development')
s2 = S3("tsa-rajiv-bucket2", "us-east-1")
p = s1 >> l1 >> s2
piper = Pipeline("three-node-pipe", [p])

```

There are many benefits here:
* We can see the flow of data through the pipeline more easily. We see that a `s1` bucket feeds into a `CopyS3Lambda` which writes to a `s2` bucket. Granted that, we could compose the data too in the JSON dict. It may be personal opinion that the tree like syntax reads better. Imagine a complex multi-child tree.
```python
p = n1 >> n2 >> [n3 >> n4 >> [n7,
                              n8],
                 n5 >> n6]
```

* In the CloudFormation Script above, we just see that the lambda code was zipped and placed in an s3 bucket. How do we know which piece of code and from where. In the Python code above, we can use the `Goto Definition` feature in many editors and instantly look at the lambda code. We blur the line between functionality and deployment specific information. 

* We have a class which represents a lambda function i.e. `AWSLambda` (`CopyS3Lambda` internally creates an `AWSLambda` instance). We now have a *language* to describe a Lambda as a Python class.

    - We can share AWSLambda in libraries. We could create a `S3ToFirehoseLambda` and share it!

Here is another pipeline all the way from an S3 bucket to the Redshift database 

```python
from jaya.core import S3, Pipeline, Firehose, Table
from jaya.util.aws_lambda.aws_lambda_utils import MapS3ToFirehoseLambda
import gzip
import sqlalchemy as sa
from jaya.deployment import deploy

def a_mapper(line, bucket, key):
    """
    A function for the MapS3ToFirehoseLambda. Returning Hard Code Values for now. But it should transform the `line` into some JSON list that can be read by Firehose
    """
    return [{'firehose_name': 'my-firehose',
             'result': [{'name': 'Rajiv', 'age': '65'}, {'name': 'Harry', 'age': '72'}]}

            ]

environment = 'staging'
region = 'us-east-1'
source_bucket = 'my-source-bucket'
source_s3 = S3(source_bucket, region, on=[S3.ALL_CREATED_OBJECTS])
firehose_name = 'my-firehose'

db_conf = {'db-name': 'my-db-name',
           'db-user': 'my-user',
           'db-passwd': 'my-passwd',
           'db-server': 'my-cluser-url'}

table = 'my-table'
holding_bucket = 'my-holding-bucket'
aws_conf = {'firehose_role': 'my_role'} # and other AWS Credentials

firehose_s3_prefix = 'my-firehose-prefix'
schema = 'my-schema'  
mapper = MapS3ToFirehoseLambda(open_function=gzip.open,
                               map_function=a_mapper,
                               batch_size=499,
                               memory=1536,
                               timeout=300,
                               region_name=region,
                               alias=environment)

a_firehose = Firehose(firehose_name,
                        db_conf['db-name'],
                        db_conf['db-user'],
                        db_conf['db-passwd'],
                        db_conf['db-server'],
                        table,
                        holding_bucket,
                        aws_conf['firehose_role'],
                        prefix=firehose_s3_prefix,
                        buffering_interval_seconds=60
                        )
a_table = Table(
    db_conf,
    table,
    sa.Column('name', sa.String),
    sa.Column('age', sa.String),
    schema=schema,
    redshift_diststyle='KEY',
    redshift_distkey='name',

)
p = source_s3 \    
    >> mapper \
    >> a_firehose \
    >> a_table
piper = Pipeline('my-trial', [p])

deploy.deploy_pipeline(aws_conf, environment, piper)
```
## Deployment

```python
# Deploy the pipeline
from jaya.deployment import deploy
piper = ...
aws_conf = {}
deploy.deploy_pipeline(aws_conf, 'staging', piper)    

```

## Unit Testing(Future Feature)
```python
from jaya.mock.in_memory_harness import test
import config
import io
from jaya.core import S3, Pipeline
from jaya.util.aws_lambda.aws_lambda_utils import  CopyS3Lambda

# Unit Test the pipeline(Future Feature) by creating an in memory local pipeline
region = 'us-east-1'
environment = 'development'
conf = config.get_aws_config(environment)

source = 'tsa-rajiv-bucket1'
destination = 'tsa-rajiv-bucket2'
p = S3(source, region, on=[S3.ALL_CREATED_OBJECTS]) \
    >> CopyS3Lambda({}, region, environment) \
    >> S3(destination, 'us-east-1')

piper = Pipeline('three-node-pipe', [p])


with test(piper) as test_harness:
    s3 = test_harness.s3()
    a_key = 'a_key'
    file_content = io.BytesIO(b'Hi Rajiv')
    s3.Bucket(source).put_object(Key=a_key, Body=file_content)
    obj = s3.Object(bucket_name=source, key=a_key)
    response = obj.get()
    data = response['Body'].read()
    self.assertEqual(data, file_content.getvalue())

```

