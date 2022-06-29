from matplotlib import pyplot as pyplt
import pandas as pds
import database
import example
import graphs
import retrieve_data
import linear_interpolation
import case_study_function
from utils import constants as c

api_key = input("Insert your API: ")
db_file = input("Insert name database: ")
interpolation_bool = input("True if interpolation; otherwise: ")

keys = []
n = 3
for i in range(0, n):
    key = input("Insert series id: ")
    keys.append(key)


def case_study(series_id):
    """ From a list of keys series determine the plot, difference,
    percentage difference, covariance and linear regression of the observations
    :param series_id: keys list
    """
    limit = 0
    if not type(series_id) is list:
        series_id = [series_id]

    list_observations = {}
    df_observations = pds.DataFrame()

    # for each series retrieve the values of the observations and write them on json.
    # Create databases and recover data.
    # Plot the observations together
    # Plot observations
    pyplt.figure(figsize=(10, 5))
    for j in series_id:
        list_observations = retrieve_data.get_observations(db_file, j, api_key, limit)
        df = linear_interpolation.if_interpolation(db_file, j, list_observations, interpolation_bool)

        df_observations[j] = df[c.value_label]
        case_study_function.graphs_series(df, j)

    graphs.set_plot(str(list(series_id)), "Observations", "observations")
    pyplt.show()
    # for each series create difference plots
    # Plot difference s(i-1) -s(i)
    pyplt.figure(figsize=(10, 5))
    for j in series_id:
        df = linear_interpolation.if_interpolation(db_file, j, list_observations, interpolation_bool)
        case_study_function.difference_plot(df, j)
    graphs.set_plot(str(list(series_id)), "Difference", "difference")
    pyplt.show()

    # for each series, create plot of the percentage differences
    # Plot difference percentage (s(i-1)-s(i))/s(i)
    pyplt.figure(figsize=(10, 5))
    for j in series_id:
        df = linear_interpolation.if_interpolation(db_file, j, list_observations, interpolation_bool)
        case_study_function.percentage_plot(df, j)
    graphs.set_plot(str(list(series_id)), "Percentage", "percentage")
    pyplt.show()

    # Covariance
    case_study_function.covariance(df_observations, series_id)

    # Linear regression
    for j in series_id:
        df = linear_interpolation.if_interpolation(db_file, j, list_observations, interpolation_bool)
        case_study_function.linear_regression(df, j)


case_study(keys)
quit()


