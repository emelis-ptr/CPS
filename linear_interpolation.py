import graphs
import database
import pandas as pd
from utils import constants as c


def interpolate_dataframe(db_file, series_key, list_observations):
    """ linear interpolation of a daframe if nan values exist
    :param list_observations: list observations
    :param db_file: name database
    :param series_key: id series
    :return: dataframe
    """
    dataframe = graphs.get_value_date_observations(db_file, series_key, list_observations)
    dataframe[c.value_label] = pd.to_numeric(dataframe[c.value_label], errors='coerce')
    dataframe[c.value_label] = dataframe[c.value_label].interpolate()
    return dataframe


def if_interpolation(db_file, series_key, list_observations, interpolation_bool):
    """ if the boolean value is true perform linear interpolation, otherwise
    :param list_observations: list observations
    :param db_file: name database
    :param series_key: id series
    :param interpolation_bool: boolean value
    :return: dataframe
    """
    dataframe = pd.DataFrame()
    try:
        dataframe = graphs.get_value_date_observations(db_file, series_key, list_observations)
        if interpolation_bool:
            dataframe = interpolate_dataframe(db_file, series_key, list_observations)
    except Exception as e:
        print("Interpolation", e)
    return dataframe


def linear_interpolation(db_file, series_id):
    """ linear interpolation
    :param series_id: id series
    :param db_file: name database
    """
    observations = database.select_observations(db_file, series_id)
    list_observations = {}

    for i in observations:
        list_observations[i[0]] = i[1]

    for index, value in enumerate(list_observations.values()):
        values = list(list_observations.values())
        keys = list(list_observations.keys())
        # print(values)
        if value == 0.0:
            value = formula_interpolation(index, values)
            list_observations[keys[index]] = value


def formula_interpolation(index, values):
    """ linear interpolation formula
    :param index: index of where the value is
    :param values: list values
    :return: formula
    """
    # it takes the preceding index and the successor of the i-th and
    # returns the formula of the linear interpolation
    index_pre = index - 1
    index_succ = index + 1
    return (values[index_succ] - values[index_pre]) * (index - index_pre) / \
           (index_succ - index_pre) + values[index_pre]
