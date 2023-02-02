import requests
import json
import time

from src.fred_cps.utils.json_file import File
from src.fred_cps.utils import constants as c
from .database import Database

time_to_sleep = 20


class ApiFred:

    def __init__(self, api_key):
        self.key = api_key
        self.category_URL = "https://api.stlouisfed.org/fred/category?"
        self.children_URL = "https://api.stlouisfed.org/fred/category/children?"
        self.series_URL = "https://api.stlouisfed.org/fred/category/series?"
        self.observations_URL = "https://api.stlouisfed.org/fred/series/observations?"
        self.db = Database(c.database_name)
        self.file = File()

    def _get_url(self, tag, value):
        url_link = ""
        if tag == c.categories_label:
            url_link = self.category_URL + "category_id=" + str(value) + "&api_key=" + self.key + "&file_type=json"
        elif tag == c.children_label:
            url_link = self.children_URL + "category_id=" + str(value) + "&api_key=" + self.key + "&file_type=json"
        elif tag == c.series_label:
            url_link = self.series_URL + "category_id=" + str(value) + "&api_key=" + self.key + "&file_type=json"
        elif tag == c.observations_label:
            url_link = self.observations_URL + "series_id=" + str(value) + "&api_key=" + self.key + "&file_type=json"
        return url_link

    def _get_json(self, tag, value):
        """" get data of single category
                :param value: 
                :param tag: 
                """
        url = self._get_url(tag, value)
        conn = requests.get(url)
        json_data = conn.text
        return json.loads(json_data)

    # Categories
    def get_categories(self, category_key):
        """ Get all category from a category id
        :param category_key: category id
        :return: dictionary category
        """
        category = self._get_json(c.categories_label, category_key)[c.categories_label]

        dict_categories = {
            category_key: {c.name: category[0][c.name], c.parent_id_label: category[0][c.parent_id_label],
                           c.children_label: {}, c.series_label: {}}}
        self._recursive_categories(dict_categories)
        self.file.write_json(category_key, c.categories_label, dict_categories)
        self.db.insert_categories(dict_categories, c.dir_cat_json + str(category_key) + "." + c.type_file)
        return dict_categories

    def _recursive_categories(self, dict_categories):
        """ Get all category from category id
          """
        for k, v in dict_categories.items():
            if isinstance(v, dict):
                try:
                    for j in self._get_json(c.children_label, k)[c.categories_label]:
                        time.sleep(2)
                        list_values = {c.name: j[c.name], c.parent_id_label: j[c.parent_id_label],
                                       c.children_label: {}, c.series_label: {}}
                        v[c.children_label][j[c.id_label]] = list_values
                    self._recursive_categories(v[c.children_label])
                except Exception as e:
                    print(e)

    # Series
    def get_series(self, category_key):
        """ Get all series from category id
        :param category_key: category_id
        :return: dictionary series
        """
        dict_series = self.get_categories(category_key)
        self._recursive_series(dict_series)
        self.file.write_json(category_key, c.series_label, dict_series)
        self.db.insert_series(dict_series, c.dir_series_json + str(category_key) + "." + c.type_file)
        return dict_series

    def _recursive_series(self, dict_series):
        """ Get all series from category id
        """
        # for each category it takes id
        for k, v in dict_series.items():
            if isinstance(v, dict):
                if v[c.series_label].values():
                    self._recursive_series(v[c.children_label])
                try:
                    for j in self._get_json(c.series_label, k)[c.seriess_label]:
                        time.sleep(2)
                        list_values = {c.title: j[c.title], c.frequency: j[c.frequency_short], c.parent_id_label: k}
                        v['series'][j['id']] = list_values
                    self._recursive_series(v['children'])
                except Exception as e:
                    print(e)

    # Observations
    def get_observations(self, key):
        """ Get all observation from key
        :param key: key
        :return: dictionary observations
        """
        dict_observations = {}
        if isinstance(key, int):
            dict_series = self.get_series(key)
            self._recursive_observations(dict_series, dict_observations)
        else:
            dict_observations = {key: []}
            for k, v in dict_observations.items():
                dict_observations[k] = self._get_observations_no_recursive(k)
        for k in dict_observations.keys():
            self.file.write_json(k, c.observations_label, dict_observations)
            self.db.insert_observations(dict_observations, c.dir_obs_json + str(key) + "." + c.type_file)
        return dict_observations

    def _recursive_observations(self, dict_series, dict_observations):
        """ If key is integer then get all observations recursively
        :param dict_observations: dictionary observations
        :param dict_series: dictionary of series
        """
        # for each category it takes id
        for k, v in dict_series.items():
            if isinstance(v, dict):
                if not v[c.series_label]:
                    self._recursive_observations(v[c.children_label], dict_observations)
                for k1 in v[c.series_label].keys():
                    dict_observations[k1] = self._get_observations_no_recursive(k1)
            self._recursive_observations(v[c.children_label], dict_observations)

    def _get_observations_no_recursive(self, series_key):
        """ From a series_key get all observations e insert it in a list
            :param series_key: id series
            """
        count = 0
        list_observations = []
        for i in self._get_json("observations", series_key)[c.observations_label]:
            count += 1
            list_values = {c.id_label: count, c.date_label: i[c.date_label], c.value_label: i[c.value_label],
                           "series_key": series_key}
            list_observations.append(list_values)
        return list_observations
