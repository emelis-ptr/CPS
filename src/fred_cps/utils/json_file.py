import json
from src.fred_cps.utils import constants as c
import os


class File:

    def write_json(self, key, name_file, list_records):
        """ write records in json file
        :param name_file:
        :param list_records: list o records: categories, series or observations
        :param key: id category, series or observation
        """
        self.create_directory()
        with open(c.type_file + '/' + name_file + '-' + str(key) + '.' + c.type_file, 'w') as fp:
            json.dump(list_records, fp)

    def open_json(self, file_path):
        """ open a json file
        :param file_path: path json
        :return: json data
        """
        with open(file_path) as data_file:
            return json.load(data_file)

    def create_directory(self):
        if not os.path.exists(c.type_file):
            os.makedirs(c.type_file)
        if not os.path.exists(c.save_type_file):
            os.makedirs(c.save_type_file)
        if not os.path.exists(c.dir_graphs):
            os.makedirs(c.dir_graphs)
