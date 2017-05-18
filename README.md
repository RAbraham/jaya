# jaya
[Proof of Concept] Write, Maintain, Unit Test, Deploy AWS Data Pipelines in Python


# Features
## Readability
Currently(STATE OF THE ART, KONRAD!), we can specify our deployment as a `JSON` dictionary. For a very simple pipeline, check out the PSEUDO ABSOLUTELY INCORRECT CloudFormation JSON Dict
 
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
* We can see the flow of data through the pipeline more easily. We see that a `s1` bucket feeds into a `CopyS3Lambda` which writes to a `s2` bucket. Granted that, we could compose the data too in the JSON dict. It may be personal opinion that the tree like syntax reads better. Imagine a comple multi-child tree.

* In the CloudFormation Script above, we just see that the lambda code was zipped and placed in an s3 bucket. How do we know which piece of code and from where. In the Python code above, we can use the `Goto Definition` feature in many editors and instantly look at the lambda code. We blur the line between functionality and deployment specific information. 

* We have a class which represents a lambda function i.e. `AWSLambda` (`CopyS3Lambda` internally creates an `AWSLambda` instance). We now have a *language* to describe a Lambda as a Python class.

    - We can share AWSLambda in libraries. We could create a `S3ToFirehoseLambda` and share it!
## Deployment

```python
# Deploy the pipeline
info = deploy.create_deploy_stack_info(piper)
deploy.deploy_stack_info(conf, "development", info)    

```

## Unit Testing(Future Feature)
```python

# Unit Test the pipeline(Future Feature) by creating an in memory local pipeline
local_pipe = create_local_pipeline(piper)

local_pipe.put_s3_object("tsa-rajiv-bucket1", "some-key", some_file)
self.assertEqual(local_pipe.exists_s3_object("tsa-rajiv-bucket2", "some-key"), some_file)

```

