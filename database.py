import os
import sqlite3
from utils import constants as c, json_file


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)
    return conn


def create_table_category(db_file, category_key, list_categories):
    """ create a table from the create_table_sql statement
    :param list_categories: list categories
    :param db_file: name database
    :param category_key: id category
    """
    create_table_categories = "CREATE TABLE IF NOT EXISTS categories (" \
                              "category_id int PRIMARY KEY, " \
                              "name varchar(50) not null," \
                              "parent_id int not null);"
    execution(db_file, create_table_categories)
    insert_categories(db_file, category_key, list_categories,
                      c.dir_cat_json + str(category_key) + "." + c.type_file)


def create_table_series(db_file, category_key, list_series):
    """ create a table from the create_table_sql statement
    :param list_series: list series
    :param db_file: name database
    :param category_key: id category
    """
    create_table_serie = "CREATE TABLE IF NOT EXISTS series (" \
                         "series_id varchar(10) PRIMARY KEY, " \
                         "title varchar(50) not null," \
                         "category_id int not null," \
                         "foreign key(category_id) references categories(category_id));"
    execution(db_file, create_table_serie)
    insert_series(db_file, category_key, list_series, c.dir_series_json + str(category_key) + "." + c.type_file)


def create_table_observations(db_file, series_key, list_observations):
    """ create a table from the create_table_sql statement
    :param list_observations: list observations
    :param db_file: name database
    :param series_key: id series
    """
    create_table_observation = "CREATE TABLE IF NOT EXISTS observations (" \
                               "id INTEGER PRIMARY KEY autoincrement, " \
                               "date timestamp not null," \
                               "value float," \
                               "series_id varchar(10) not null," \
                               "FOREIGN KEY(series_id) REFERENCES series(series_id));"
    execution(db_file, create_table_observation)
    insert_observations(db_file, series_key, list_observations, c.dir_obs_json + series_key + "." + c.type_file)


def insert_categories(db_file, category_key, list_categories, file_path):
    """ insert records in table categories
        :param list_categories: list categories
        :param file_path: json path
        :param db_file: name database
        :param category_key: id category
        """
    conn = create_connection(db_file)
    insert_sql = "insert into categories (category_id, name, parent_id) values (?, ?, ?);"
    select_sql = "select name, parent_id from categories where category_id = ?;"

    try:
        cursor = conn.cursor()
        # takes the values of the sql script if there are any
        result = result_fetchone(cursor, select_sql, category_key)
        if not result:
            # if there is a json file with the values then it gets the records from here,
            # otherwise
            if not os.path.exists(file_path) & (os.stat(file_path).st_size == 0):
                insert_value_categories(list_categories, cursor, insert_sql)
            else:
                list_categories = json_file.open_json(file_path)
                insert_value_categories(list_categories, cursor, insert_sql)
    except Exception as e:
        print("Error for insert categories", e)
    conn.commit()


def insert_series(db_file, category_key, list_series, file_path):
    """ insert value in table series
        :param list_series: list series
        :param file_path: json path
        :param db_file: database
        :param category_key: id category
        """
    conn = create_connection(db_file)
    insert_sql = "insert into series (series_id, title, category_id) values (?, ?, ?);"
    select_sql = "select series_id, title, category_id from series where series_id = ?;"

    try:
        cursor = conn.cursor()
        result = result_fetchone(cursor, select_sql, category_key)
        if not result:
            # if there is a json file with the values then it gets the records from here,
            # otherwise
            if not os.path.exists(file_path) & (os.stat(file_path).st_size == 0):
                insert_value_series(list_series, cursor, insert_sql)
            else:
                list_series = json_file.open_json(file_path)
                insert_value_series(list_series, cursor, insert_sql)
    except Exception as e:
        print("Error for insert series", e)
    conn.commit()


def insert_observations(db_file, series_key, dict_observations, file_path):
    """ insert value in table observations
        :param dict_observations: list observations
        :param file_path: json path
        :param db_file: database json
        :param series_key: id series
        """
    conn = create_connection(db_file)
    insert_sql = "insert into observations (date, value, series_id) " \
                 "values (?, ?, ?);"
    select_sql = "select date, value, series_id from observations where series_id = ?;"
    try:
        cursor = conn.cursor()
        cursor.execute(select_sql, (series_key,))
        result = cursor.fetchone()

        if not result:
            # if there is a json file with the values then it gets the records from here,
            # otherwise
            if not os.path.exists(file_path) & (os.stat(file_path).st_size == 0):
                insert_value_observations(dict_observations, cursor, insert_sql)
            else:
                dict_observations = json_file.open_json(file_path)
                insert_value_observations(dict_observations, cursor, insert_sql)
    except Exception as e:
        print("Error for insert observations", e)
    conn.commit()


def select_observations(db_file, series_id):
    """ select records from observations
            :param series_id: id series
            :param db_file: database
            :return
            """
    observations = []
    conn = create_connection(db_file)
    cursor = conn.cursor()
    select_observation = "select date, value from observations where series_id = ?"
    try:
        cursor.execute(select_observation, (series_id,))
        observations = cursor.fetchall()
    except Exception as e:
        print("Error for selecting observations", e)

    conn.commit()
    return observations


def modify_value(db_file, sql):
    """ modify table
    :param sql: script sql
    :param db_file: database
    """
    execution(db_file, sql)


def delete_value(db_file, sql):
    """ delete table
    :param sql: script sql
    :param db_file: database
    """
    execution(db_file, sql)


def drop_table(db_file):
    """ drop all tables
        :param db_file: database
        """
    drop_table_categories = "DROP TABLE IF EXISTS categories;"
    drop_table_series = "DROP TABLE IF EXISTS series;"
    drop_table_observations = "DROP TABLE IF EXISTS observations;"

    execution(db_file, drop_table_categories)
    execution(db_file, drop_table_series)
    execution(db_file, drop_table_observations)


def execution(db_file, sql):
    """ execution database
    :param db_file: name database
    :param sql: sql script
    """
    conn = create_connection(db_file)
    try:
        c = conn.cursor()
        c.execute(sql)
    except Exception as e:
        print(e)
    conn.commit()


def result_fetchone(cursor, sql, key):
    """ Get values from tables through sql
    :param cursor: cursor database
    :param sql: sql script
    :param key: key category or series
    :return:
    """
    cursor.execute(sql, (key,))
    return cursor.fetchone()


def check_table_exist(db_file, key, name_table, list_obs):
    """ check if table exist
    :param db_file: name database
    :param key: id series
    :param name_table: name table to check
    :param list_obs: list observations
    """
    conn = create_connection(db_file)
    cursor = conn.cursor()
    # get the count of tables with the name
    cursor.execute(''' SELECT count(?) FROM sqlite_master WHERE type='table' 
    AND name=? ''', (key, name_table))

    # if the count is 1, then table exists
    if not cursor.fetchone()[0] == 1:
        create_table_observations(db_file, key, list_obs)
    insert_observations(db_file, key, list_obs, c.dir_obs_json + key + "." +
                        c.type_file)


def insert_value_categories(dict_category, cursor, sql):
    """ execution sql to insert values in table
        :param dict_category: dictionary categories
        :param cursor: cursor
        :param sql: sql script
        """
    for k, v in dict_category.items():
        data = (k, v[c.name], v[c.parent_id_label])
        cursor.execute(sql, data)
        if isinstance(v, dict):
            insert_value_categories(v[c.children_label], cursor, sql)


def insert_value_series(dict_series, cursor, sql):
    """ execution sql to insert values in table
    :param dict_series: dictionary series
    :param cursor: cursor
    :param sql: sql script
    """
    for k, v in dict_series.items():
        if isinstance(v, dict):
            if not v[c.series_label]:
                insert_value_series(v[c.children_label], cursor, sql)

            for k1, v1 in v[c.series_label].items():
                data = (k1, v1[c.title], v1[c.parent_id_label])
                cursor.execute(sql, data)
            insert_value_series(v[c.children_label], cursor, sql)


def insert_value_observations(dict_observations, cursor, sql):
    """ execution sql to insert values in table
    :param dict_observations: dictionary observations
    :param cursor: cursor
    :param sql: sql script
    """
    for k, v in dict_observations.items():
        for v1 in v:
            data = (v1[c.date_label], v1[c.value_label], k)
            cursor.execute(sql, data)
