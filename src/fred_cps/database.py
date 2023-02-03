import os
import sqlite3

from src.fred_cps.utils import constants as c
from src.fred_cps.utils.json_file import open_json


# Classe contenenti funzioni che permettono di creare e rimuovere tabelle, selezionare e modificare
# le tabelle categorie, serie e osservabili
def _create_table_queries():
    """Metodo contenenti le queries per creare le tabelle categories, series e observations """
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


class Database:

    def __init__(self, db_file):
        if db_file.endswith(".sqlite"):
            self.db_file = db_file
        else:
            self.db_file = db_file + ".sqlite"

        self.conn = sqlite3.connect(self.db_file)
        queries = _create_table_queries()
        try:
            cursor = self.conn.cursor()

            for q in queries:
                cursor.execute(q)
                self.conn.commit()
        except Exception as e:
            print(e)

    def insert_categories(self, dict_categories, file_path):
        """ Metodo che permette di inserire record all'interno della tabella categories
            :param dict_categories: dictionary della categoria
            :param file_path: json path
            :exception Exception: """
        insert_sql = "replace into categories (category_id, name, parent_id) values (?, ?, ?);"

        try:
            # if there is a json file with the values then it gets the records from here,
            # otherwise
            if not os.path.exists(file_path) & (os.stat(file_path).st_size == 0):
                self.insert_value_categories(dict_categories, insert_sql)
            else:
                list_categories_json = open_json(file_path)
                self.insert_value_categories(list_categories_json, insert_sql)
        except Exception as e:
            print("Error for insert categories", e)
        self.conn.commit()

    def insert_value_categories(self, dict_category, sql):
        """ Metodo che in modo ricorsivo esegue la query per inserire i record all'interno della tabella
            :param dict_category: dictionary della categoria
            :param sql: sql script """
        for k, v in dict_category.items():
            data = (k, v[c.name], v[c.parent_id_label])
            self.conn.cursor().execute(sql, data)
            if isinstance(v, dict):
                self.insert_value_categories(v[c.children_label], sql)

    def insert_series(self, dict_series, file_path):
        """ Metodo che permette di inserire record all'interno della tabella series
            :param dict_series: dictionary della serie
            :param file_path: json path
            :exception Exception: """
        insert_sql = "replace into series (series_id, title, category_id) values (?, ?, ?);"

        try:
            # if there is a json file with the values then it gets the records from here,
            # otherwise
            if not os.path.exists(file_path) & (os.stat(file_path).st_size == 0):
                self.insert_value_series(dict_series, insert_sql)
            else:
                dict_series = open_json(file_path)
                self.insert_value_series(dict_series, insert_sql)
        except Exception as e:
            print("Error for insert series", e)
        self.conn.commit()

    def insert_value_series(self, dict_series, sql):
        """ Metodo che in modo ricorsivo esegue la query per inserire i record all'interno della tabella
        :param dict_series: dictionary della serie
        :param sql: sql script """
        for k, v in dict_series.items():
            if isinstance(v, dict):
                if not v[c.series_label]:
                    self.insert_value_series(v[c.children_label], sql)

                for k1, v1 in v[c.series_label].items():
                    data = (k1, v1[c.title], v1[c.parent_id_label])
                    self.conn.cursor().execute(sql, data)
                self.insert_value_series(v[c.children_label], sql)

    def insert_observations(self, dict_observations, file_path):
        """ Metodo che permette di inserire record all'interno della tabella observations
            :param dict_observations: dictionary delle osservabili
            :param file_path: json path
            :exception Exception: """
        insert_sql = "replace into observations (observation_id, date, value, series_id) " \
                     "values (?, ?, ?, ?);"
        try:
            # if there is a json file with the values then it gets the records from here,
            # otherwise
            if not os.path.exists(file_path) & (os.stat(file_path).st_size == 0):
                self.insert_value_observations(dict_observations, insert_sql)
            else:
                dict_observations = open_json(file_path)
                self.insert_value_observations(dict_observations, insert_sql)
        except Exception as e:
            print("Error for insert observations", e)
        self.conn.commit()

    def insert_value_observations(self, dict_observations, sql):
        """ Metodo che in modo ricorsivo esegue la query per inserire i record all'interno della tabella
        :param dict_observations: dictionary delle osservabili
        :param sql: sql script """
        for k, v in dict_observations.items():
            for v1 in v:
                data = (v1["id"], v1[c.date_label], v1[c.value_label], v1["series_key"])
                self.conn.cursor().execute(sql, data)

    def select_categories(self, category_id):
        """Metodo che permette di selezionare tutti i record della tabella categories dato un id della categoria
        :param category_id: id della categoria
        :return: records"""
        select_sql = "select * from categories where category_id = ?"
        result = self._get_from_db(select_sql, category_id)
        return result

    def select_categories_parentid(self, parent_id):
        """Metodo che permette di selezionare tutti i record della tabella categories dato un parent_id
        :param parent_id: id della categoria
        :return: records """
        select_sql = "select * from categories where parent_id = ?"
        result = self._get_from_db(select_sql, parent_id)
        return result

    def select_series(self, series_id):
        """Metodo che permette di selezionare tutti i record della tabella series dato un id della serie
                :param series_id: id della serie
                :return: records """
        select_sql = "select * from series where series_id =?"
        result = self._get_from_db(select_sql, series_id)
        return result

    def select_series_category(self, category_id):
        """Metodo che permette di selezionare tutti i record della tabella series dato un id della cetgoria
                :param category_id: id della categoria
                :return: records """
        select_sql = "select * from series where category_id =?"
        result = self._get_from_db(select_sql, category_id)
        return result

    def select_observations(self, series_id):
        """Metodo che permette di selezionare tutti i record della tabella observations dato un id della serie
                :param series_id: id della serie
                :return: records """
        select_sql = "select * from observations where series_id =?"
        results = self._get_from_db(select_sql, series_id)
        return results

    def _get_from_db(self, sql, key):
        """Metodo che esegue la query per ottenere i record dalle tabelle
        :param sql: script sql
        :param key: valore che corrisponde ad un id
        :return: records
        :exception Exception: """
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
        """Metodo che permette di selezionare tutte le categorie presenti
        :return: records che riguardano tutte le categorie """
        select_sql = "select * from categories"
        category_list = self._get_all_from_db(select_sql)
        return category_list

    def select_all_series(self):
        """Metodo che permette di selezionare tutte le serie presenti
                :return: records che riguardano tutte le serie """
        select_sql = "select * from series"
        series_list = self._get_all_from_db(select_sql)
        return series_list

    def select_all_observation(self):
        """Metodo che permette di selezionare tutte le osservabili presenti
                :return: records che riguardano tutte le osservabili """
        select_sql = "select * from observations"
        observation_list = self._get_all_from_db(select_sql)
        return observation_list

    def _get_all_from_db(self, sql):
        """Metodo che esegue la query per ottenere i record dalle tabelle
                :param sql: script sql
                :return: records
                :exception Exception: """
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
        """Metodo che permette di modificare, quindi di aggiornare, i record della tabella categories
        :param category_id: id della categoria
        :param name: nome della categoria
        :param parent_id: id padre """
        update_sql = "update categories set category_id=?, name=?, parent_id=? where category_id=?"
        values = [category_id, name, parent_id, category_id]
        self._update_db(update_sql, values)

    def update_series(self, series_id, title, category_id):
        """Metodo che permette di modificare, quindi di aggiornare, i record della tabella series
                :param series_id: id della serie
                :param title: titolo della serie
                :param category_id: id della categoria a cui appartiene """
        update_sql = "update series set series_id=?, title=?, category_id=? where series_id=?"
        values = [series_id, title, category_id, series_id]
        self._update_db(update_sql, values)

    def update_observation(self, observation_id, value, date):
        """Metodo che permette di modificare, quindi di aggiornare, i record della tabella observations
                :param observation_id: id delle osservabili
                :param value: valore numerico (float)
                :param date: data """
        update_sql = "update observations set date=?, value=?, observation_id=?) where observation_id=?"
        values = [date, value, observation_id, observation_id]
        self._update_db(update_sql, values)

    def _update_db(self, sql, value):
        """Metodo che esegue la query sql per modificare record di una tabella
        :param sql: script sql
        :param value: lista dei parametri da modificare
        :exception Exception: """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, value)
            self.conn.commit()
        except Exception as e:
            print(e)

    def drop_categories(self):
        """Metodo che permette di eliminare la tabella categories se esiste """
        drop_table = "DROP TABLE IF EXISTS categories;"
        self._drop_table(drop_table)

    def drop_series(self):
        """Metodo che permette di eliminare la tabella series se esiste """
        drop_table = "DROP TABLE IF EXISTS series;"
        self._drop_table(drop_table)

    def drop_observation(self):
        """Metodo che permette di eliminare la tabella observations se esiste """
        drop_table = "DROP TABLE IF EXISTS observations;"
        self._drop_table(drop_table)

    def _drop_table(self, sql):
        """Metodo che esegue la query sql per eliminare la tabella
         :exception Exception:"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            print(e)

    def check_table_exist(self, name_table):
        """ Metodo che permette di verificare l'esistenza di una tabella
        :param name_table: nome della tabella """
        cursor = self.conn.cursor()
        sql = "SELECT count(?) FROM sqlite_master WHERE type='table' AND name=?"
        # get the count of tables with the name
        cursor.execute(sql, name_table)

        # if the count is 1, then table exists
        if not cursor.fetchone()[0] == 1:
            print("the table does not exist")

    def close_db(self):
        """Metodo che chiude la connessione con il database """
        self.conn.close()
