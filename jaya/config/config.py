from jaya.lib import util
from typing import Dict
from configparser import ConfigParser
import os.path


def lib_folder():
    return util.parent_dir(__file__) + '/lib'


def config_folder():
    return util.parent_dir(__file__) + '/config'


def project_root():
    return util.parent_dir(__file__)


# def get(config_file_path: str) -> Dict[str, str]:
#     assert os.path.isfile(config_file_path), "Invalid configuration file path:" + config_file_path
#     config_parser = ConfigParser()
#     config_parser.read(config_file_path)
#     sections = config_parser.sections()
#     if 'jaya' not in sections:
#         raise ValueError("Expected section jaya in configuration file:" + config_file_path)
#     else:
#         conf_dict = dict(config_parser.items('jaya'))
#         return conf_dict
