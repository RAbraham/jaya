import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


if __name__ == '__main__':
    region = 'us-east-1'
    import jaya.lib.aws as aws
    from jaya.config import config
    from pprint import pprint

    conf = config.get_aws_config('development')
    client = aws.client(conf, 'cloudformation', region_name=region)


    response = client.execute_change_set(
        ChangeSetName='a',
        StackName='RajivTestStack',
    )

    pprint(response)
