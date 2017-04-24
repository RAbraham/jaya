import platform


def handler(event, context):
    print('Do ETL')
    print(platform.python_version())
