import os
import sqlite3

import pandas as pd

from src.fred_cps.utils import constants as c
from src.fred_cps.utils.json_file import File


class Database:

    def __init__(self, db_file):
        self.file = File()
        if db_file.endswith(".sqlite"):
            self.db_file = db_file
        else:
            self.db_file = db_file + ".sqlite"

        self.conn = sqlite3.connect(self.db_file)
        queries = self._create_table_queries()
        try:
            cursor = self.conn.cursor()

            for q in queries:
                cursor.execute(q)
                self.conn.commit()
        except Exception as e:
            print(e)

    def _create_table_queries(self):
        create_table_categories = "CREATE TABLE IF NOT EXISTS categories (" \
                                  "category_id int PRIMARY KEY, " \
                                  "name varchar(50) not null," \
                                  "parent_id int not null);"
        create_table_series = "CREATE TABLE IF NOT EXISTS series (" \
                              "series_id varchar(10) PRIMARY KEY, " \
                              "title varchar(50) not null," \
                              "category_id int not null," \
                              "foreign key(category_id) references categories(category_id));"
        create_table_observation = "CREATE TABLE IF NOT EXISTS observations (" \
                                   "observation_id INTEGER PRIMARY KEY autoincrement, " \
                                   "date text not null," \
                                   "value float," \
                                   "series_id varchar(10) not null," \
                                   "FOREIGN KEY(series_id) REFERENCES series(series_id));"
        queries = [create_table_categories, create_table_series, create_table_observation]
        return queries

    def insert_categories(self, list_categories, file_path):
        """ insert records in table categories
            :param list_categories: list categories
            :param file_path: json path
            """
        insert_sql = "replace into categories (category_id, name, parent_id) values (?, ?, ?);"

        try:
            # if there is a json file with the values then it gets the records from here,
            # otherwise
            if not os.path.exists(file_path) & (os.stat(file_path).st_size == 0):
                self.insert_value_categories(list_categories, insert_sql)
            else:
                list_categories_json = self.file.open_json(file_path)
                self.insert_value_categories(list_categories_json, insert_sql)
        except Exception as e:
            print("Error for insert categories", e)
        self.conn.commit()

    def insert_value_categories(self, dict_category, sql):
        """ execution sql to insert values in table
            :param dict_category: dictionary categories
            :param cursor: cursor
            :param sql: sql script
            """
        for k, v in dict_category.items():
            data = (k, v[c.name], v[c.parent_id_label])
            self.conn.cursor().execute(sql, data)
            if isinstance(v, dict):
                self.insert_value_categories(v[c.children_label], sql)

    def insert_series(self, list_series, file_path):
        """ insert value in table series
            :param list_series: list series
            :param file_path: json path
            """
        insert_sql = "replace into series (series_id, title, category_id) values (?, ?, ?);"

        try:
            # if there is a json file with the values then it gets the records from here,
            # otherwise
            if not os.path.exists(file_path) & (os.stat(file_path).st_size == 0):
                self.insert_value_series(list_series, insert_sql)
            else:
                list_series = self.file.open_json(file_path)
                self.insert_value_series(list_series, insert_sql)
        except Exception as e:
            print("Error for insert series", e)
        self.conn.commit()

    def insert_value_series(self, dict_series, sql):
        """ execution sql to insert values in table
        :param dict_series: dictionary series
        :param sql: sql script
        """
        for k, v in dict_series.items():
            if isinstance(v, dict):
                if not v[c.series_label]:
                    self.insert_value_series(v[c.children_label], sql)

                for k1, v1 in v[c.series_label].items():
                    data = (k1, v1[c.title], v1[c.parent_id_label])
                    self.conn.cursor().execute(sql, data)
                self.insert_value_series(v[c.children_label], sql)

    def insert_observations(self, dict_observations, file_path):
        """ insert value in table observations
            :param dict_observations: list observations
            :param file_path: json path
            """
        insert_sql = "replace into observations (observation_id, date, value, series_id) " \
                     "values (?, ?, ?, ?);"
        try:
            # if there is a json file with the values then it gets the records from here,
            # otherwise
            if not os.path.exists(file_path) & (os.stat(file_path).st_size == 0):
                self.insert_value_observations(dict_observations, insert_sql)
            else:
                dict_observations = self.file.open_json(file_path)
                self.insert_value_observations(dict_observations, insert_sql)
        except Exception as e:
            print("Error for insert observations", e)
        self.conn.commit()

    def insert_value_observations(self, dict_observations, sql):
        """ execution sql to insert values in table
        :param dict_observations: dictionary observations
        :param sql: sql script
        """
        for k, v in dict_observations.items():
            for v1 in v:
                data = (v1["id"], v1[c.date_label], v1[c.value_label], v1["series_key"])
                self.conn.cursor().execute(sql, data)

    def select_categories(self, category_id):
        select_sql = "select * from categories where category_id = ?"
        result = self._get_from_db(select_sql, category_id)
        return result

    def select_categories_parentid(self, category_id):
        select_sql = "select * from categories where parent_id = ?"
        result = self._get_from_db(select_sql, category_id)
        return result

    def select_series(self, series_id):
        select_sql = "select * from series where series_id =?"
        result = self._get_from_db(select_sql, series_id)
        return result

    def select_observations(self, observation_id):
        select_sql = "select * from observations where series_id =?"
        results = self._get_from_db(select_sql, observation_id)
        return results

    def _get_from_db(self, sql, key):
        result = []
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, [key])

            columns = [col[0] for col in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            print(e)
        return result

    def select_all_categories(self):
        select_sql = "select * from categories"
        category_list = self._get_all_from_db(select_sql)
        return category_list

    def select_all_series(self):
        select_sql = "select * from series"
        series_list = self._get_all_from_db(select_sql)
        return series_list

    def select_all_observation(self):
        select_sql = "select * from observations"
        observation_list = self._get_all_from_db(select_sql)
        return observation_list

    def _get_all_from_db(self, sql):
        result = []
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            print(e)
        return result

    def update_categories(self, category_id, name, parent_id):
        update_sql = "update categories set category_id=?, name=?, parent_id=? where category_id=?"
        values = [category_id, name, parent_id, category_id]
        self._update_db(update_sql, values)

    def update_series(self, series_id, title, category_id):
        update_sql = "update series set series_id=?, title=?, category_id=? where series_id=?"
        values = [series_id, title, category_id, series_id]
        self._update_db(update_sql, values)

    def update_observation(self, observation_id, value, date):
        update_sql = "update observations set date=?, value=?, observation_id=?) where observation_id=?"
        values = [date, value, observation_id, observation_id]
        self._update_db(update_sql, values)

    def _update_db(self, sql, value):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, value)
            self.conn.commit()
        except Exception as e:
            print(e)

    def drop_categories(self):
        drop_table = "DROP TABLE IF EXISTS categories;"
        self._drop_table(drop_table)

    def drop_series(self):
        drop_table = "DROP TABLE IF EXISTS series;"
        self._drop_table(drop_table)

    def drop_observation(self):
        drop_table = "DROP TABLE IF EXISTS observations;"
        self._drop_table(drop_table)

    def _drop_table(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            print(e)

    def check_table_exist(self, name_table):
        """ check if table exist
        :param name_table: name table to check
        """
        cursor = self.conn.cursor()
        sql = "SELECT count(?) FROM sqlite_master WHERE type='table' AND name=?"
        # get the count of tables with the name
        cursor.execute(sql, name_table)

        # if the count is 1, then table exists
        if not cursor.fetchone()[0] == 1:
            print("the table does not exist")

    def close_db(self):
        self.conn.close()
