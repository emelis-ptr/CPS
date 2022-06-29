import json
from utils import constants as c
import os


def write_json(key, list_records, name_file):
    """ write records in json file
    :param name_file: name too assign to the file
    :param list_records: list o records: categories, series or observations
    :param key: id category, series or observation
    """
    create_directory()
    with open(c.type_file + '/' + name_file + '-' + str(key) + '.' + c.type_file, 'w') as fp:
        json.dump(list_records, fp)


def open_json(file_path):
    """ open a json file
    :param file_path: path json
    :return: json data
    """
    with open(file_path) as data_file:
        return json.load(data_file)


def create_directory():
    if not os.path.exists(c.type_file):
        os.makedirs(c.type_file)
    if not os.path.exists(c.save_type_file):
        os.makedirs(c.save_type_file)
    if not os.path.exists(c.dir_graphs):
        os.makedirs(c.dir_graphs)
