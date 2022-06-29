import requests
import json
import time
import database
from utils import constants as c, json_file

time_to_sleep = 20


def get_url_category(category_key, api_key):
    """" get url of category
    :param category_key: category id
    :param api_key: api
    """
    return (
        f"{c.url}category?category_id={category_key}&api_key={api_key}&file_type={c.type_file}")


def get_url(category_key, api_key, tag, limit):
    """" get url of category
        :param limit: limit the number of categories and series to download.
        All categories if 0
        :param category_key: category id
        :param api_key: api
        :param tag: choose between 'children' or 'series'
        """
    url_link = f"{c.url}category/{tag}?category_id={category_key}" \
               f"&api_key={api_key}&file_type={c.type_file}n&limit={limit}"
    if limit == 0:
        url_link = f"{c.url}category/{tag}?category_id={category_key}" \
                   f"&api_key={api_key}&file_type={c.type_file}"
    return url_link


def get_url_observations(series_key, api_key):
    """" get url of category
            :param series_key: series id
            :param api_key: api
            """
    return (
        f"{c.url}series/observations?series_id={series_key}&api_key={api_key}&file_type={c.type_file}"
    )


def get_category_data(category_key, api_key):
    """" get data of single category
            :param category_key: category id
            :param api_key: api
            """
    url_children = get_url_category(category_key, api_key)
    conn_children = requests.get(url_children)
    if conn_children.status_code == 429:
        time.sleep(time_to_sleep)
    return json.loads(conn_children.text)


def get_data(category_key, api_key, tag, limit):
    """" get data of all category or series
            :param limit: limit the number of categories and series to download.
             All categories if 0
            :param category_key: category id
            :param api_key: api
            :param tag: choose between 'children' or 'series'
            """
    url_children = get_url(category_key, api_key, tag, limit)
    conn_children = requests.get(url_children)
    if conn_children.status_code == 429:
        time.sleep(time_to_sleep)
    return json.loads(conn_children.text)


##############################################
# Get all records
##############################################


def get_observations_data(series_key, api_key):
    """" get data of all observations of a series
                :param series_key: series id
                :param api_key: api
                """
    url_observations = get_url_observations(series_key, api_key)
    conn_observations = requests.get(url_observations)
    if conn_observations.status_code == 429:
        time.sleep(time_to_sleep)
    return json.loads(conn_observations.text)


def get_categories(db_file, category_key, api_key, limit):
    """ Get all category from category id
    :param db_file: name database
    :param category_key: category id
    :param api_key: api
    :param limit: limit the number of categories and series to download. All categories if 0
    :return: dictionary category
    """
    category = get_category_data(category_key, api_key)[c.categories_label]

    dict_categories = {
        category_key: {c.name: category[0][c.name], c.parent_id_label: category[0][c.parent_id_label],
                       c.children_label: {}, c.series_label: {}}}
    recursive_categories(dict_categories, api_key, limit)
    json_file.write_json(category_key, dict_categories, c.categories_label)
    database.create_table_category(db_file, category_key, dict_categories)
    return dict_categories


def recursive_categories(dict_category, api_key, limit):
    """ Get all category from category id
    :param dict_category: dictionary of all categories
    :param api_key: api
    :param limit: limit the number of categories and series to download.
    All categories if 0
    """
    for k, v in dict_category.items():
        if isinstance(v, dict):
            try:
                for j in get_data(k, api_key, c.children_label, limit)[c.categories_label]:
                    time.sleep(2)
                    list_values = {c.name: j[c.name], c.parent_id_label: j[c.parent_id_label],
                                   c.children_label: {}, c.series_label: {}}
                    v[c.children_label][j[c.id_label]] = list_values
                recursive_categories(v[c.children_label], api_key, limit)
            except Exception as e:
                print(e)


def get_series(db_file, category_key, api_key, limit):
    """ Get all series from category id
    :param db_file: name database
    :param category_key: category_id
    :param api_key: api
    :param limit: limit category or series to download. All categories if 0
    :return: dictionary series
    """
    dict_series = get_categories(db_file, category_key, api_key, limit)
    recursive_series(dict_series, api_key, limit)
    json_file.write_json(category_key, dict_series, c.series_label)
    database.create_table_series(db_file, category_key, dict_series)
    return dict_series


def recursive_series(dict_series, api_key, limit):
    """ Get all series from category id
    :param dict_series: dictionary of series
    :param api_key: api
    :param limit: limit category or series to download. All categories if 0
    """
    # for each category it takes id
    for k, v in dict_series.items():
        if isinstance(v, dict):
            if v[c.series_label].values():
                recursive_series(v[c.children_label], api_key, limit)
            try:
                for j in get_data(k, api_key, c.series_label, limit)[c.seriess_label]:
                    time.sleep(2)
                    list_values = {c.title: j[c.title], c.frequency: j[c.frequency_short], c.parent_id_label: k}
                    v['series'][j['id']] = list_values
                recursive_series(v['children'], api_key, limit)
            except Exception as e:
                print(e)


def get_observations(db_file, key, api_key, limit):
    """ Get all observation from key
    :param db_file: name database
    :param key: key
    :param api_key: api
    :param limit: limit series to download. All categories if 0
    :return: dictionary observations
    """
    dict_observations = {}
    if isinstance(key, int):
        dict_series = get_series(db_file, key, api_key, limit)
        recursive_observations(dict_series, api_key, dict_observations)
    else:
        dict_observations = {key: []}
        for k, v in dict_observations.items():
            dict_observations[k] = get_observations_no_recursive(k, api_key)
    for k in dict_observations.keys():
        json_file.write_json(k, dict_observations, c.observations_label)
        database.create_table_observations(db_file, k, dict_observations)
    return dict_observations


def recursive_observations(dict_series, api_key, dict_observations):
    """ If key is integer then get all observations recursively
    :param dict_observations: dictionary observations
    :param dict_series: dictionary of series
    :param api_key: api
    """
    # for each category it takes id
    for k, v in dict_series.items():
        if isinstance(v, dict):
            if not v[c.series_label]:
                recursive_observations(v[c.children_label], api_key, dict_observations)
            for k1 in v[c.series_label].keys():
                dict_observations[k1] = get_observations_no_recursive(k1, api_key)
        recursive_observations(v[c.children_label], api_key, dict_observations)


def get_observations_no_recursive(series_key, api_key):
    """ From a series_key get all observations e insert it in a list
        :param series_key: id series
        :param api_key: api
        """
    count = 0
    list_observations = []
    for i in get_observations_data(series_key, api_key)[c.observations_label]:
        count += 1
        list_values = {c.id_label: count, c.date_label: i[c.date_label], c.value_label: i[c.value_label]}
        list_observations.append(list_values)
    return list_observations
