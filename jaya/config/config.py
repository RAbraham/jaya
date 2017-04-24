from jaya.lib import util
import configparser

def get(config_file_path):
    """
    Returns a ConfigParser object for the config_file_path
    :param config_file_path: Relative to config folder e.g. if config/aws.conf, then pass 'aws.conf'
    :return: ConfigParser object
    """

    relative_path = util.file_relative_path(__file__, config_file_path)
    config_parser = configparser.ConfigParser()
    config_parser.read(relative_path)
    return config_parser


# environment: 'development' or 'staging' or 'production'
def get_aws_config(environment):
    validate_environment(environment)
    aws_config_parser = get('aws.conf')
    aws_config = aws_config_parser.items(environment)
    result = dict(aws_config)
    if 'tracker_aws_regions' in result:
        result['tracker_aws_regions'] = [r.strip() for r in result['tracker_aws_regions'].split(',')]
    else:
        result['tracker_aws_regions'] = []

    result['etl_source_buckets'] = util.etl_sources(result['tracking_bucket_prefix'], result['tracker_aws_regions'])
    if 'offload_batch_size' in result:
        result['offload_batch_size'] = int(result['offload_batch_size'])
    return result


def get_all_config(application, environment):
    '''

    :param application: Apps which are client based (sports, esports) as opposed to server based(messenger-bot)
    :param environment: staging, production etc.
    :return:
    '''
    validate_environment(environment)
    conf = get_aws_config(environment)
    conf['specs_dir'] = util.parent_dir(__file__) + '/specs/'

    db_section = application + '-' + environment
    db_conf = get_db_conf(db_section)
    result = util.merge_dicts(conf, db_conf)
    return result


def validate_environment(environment):
    valid_environments = ['development', 'development_remote', 'staging', 'production']
    assert environment in valid_environments, \
        "Invalid Environment Name:{0}. Should be one of {1}".format(environment,
                                                                    valid_environments)


def get_messenger_bot_config(environment):
    bot_conf_parser = get('bot.conf')
    result = dict(bot_conf_parser.items(environment))

    if 'offload_batch_size' in result:
        result['offload_batch_size'] = int(result['offload_batch_size'])
    return result


def get_db_conf(environment):
    db_conf_parser = get('db.conf')
    db_config = db_conf_parser.items(environment)
    return dict(db_config)


def lib_folder():
    return util.parent_dir(__file__) + '/lib'


def app_folder():
    return util.parent_dir(__file__) + '/app'


def config_folder():
    return util.parent_dir(__file__) + '/config'


def project_root():
    return util.parent_dir(__file__)
