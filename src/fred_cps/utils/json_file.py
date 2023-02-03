import json
from src.fred_cps.utils import constants as c
import os


# Classe contenenti funzioni che gestiscono i file
def open_json(file_path):
    """Metodo che permette la lettura di un file json
    :param file_path: path json
    :return: json data """
    with open(file_path) as data_file:
        return json.load(data_file)


def create_directory():
    """Metodo che permette la creazione di cartelle se non esistono """
    if not os.path.exists(c.type_file):
        os.makedirs(c.type_file)
    if not os.path.exists(c.save_type_file):
        os.makedirs(c.save_type_file)
    if not os.path.exists(c.dir_graphs):
        os.makedirs(c.dir_graphs)


def write_json(key, name_file, list_records):
    """Metodo che scrive tutti i record in un file
    :param name_file: nome del file
    :param list_records: lista dei record: categories, series or observations
    :param key: id category, series or observation """
    create_directory()
    with open(c.type_file + '/' + name_file + '-' + str(key) + '.' + c.type_file, 'w') as fp:
        json.dump(list_records, fp)

