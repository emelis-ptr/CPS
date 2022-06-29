import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
import pandas as pd
import database
import linear_interpolation
from utils import constants as c


def draw_plots(db_file, series_key, parameter_days, list_observations, interpolation_bool):
    """ create plots: simple plot and media mobile
    :param list_observations: list observations
    :param interpolation_bool:
    :param db_file: name database
    :param series_key: series id
    :param parameter_days: parameters n day for media mobile
    """
    plot_observations(db_file, series_key, list_observations, interpolation_bool)
    media_mobile(db_file, series_key, parameter_days, list_observations, interpolation_bool)


def get_value_date_observations(db_file, series_key, list_observations):
    """ Insert date and value from observations in Dataframe
    :param list_observations: list observations
    :param db_file: name databse
    :param series_key: serie id
    :return: dataframe
    """
    database.check_table_exist(db_file, series_key, c.observations_label,
                               list_observations)
    observation = database.select_observations(db_file, series_key)
    list_date = []
    list_value = []
    for row in observation:
        list_date.append(row[0])
        list_value.append(row[1])

    df = pd.DataFrame(list(zip(list_date, list_value)), columns=[c.date_label,
                                                                 c.value_label])
    return df


def formatter_date_in_axis(dataframe):
    """ format the string to date
    :param dataframe: key-value: date-value
    :return:
    """
    # formatter dates
    date_formatter = [dt.datetime.strptime(d, c.format_date).date() for d in dataframe[c.date_label]]
    # plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    return date_formatter


def define_date_axis():
    """ distance between one date and another
    """
    # frequency date in x-axis
    fig = plt.figure()
    ax = fig.add_subplot()
    year = mdates.YearLocator(10)
    ax.xaxis.set_major_locator(year)
    fig.autofmt_xdate(rotation=45)


def pplot(series_key, dataframe, list_date):
    """ Plots
    :param series_key: series id
    :param dataframe:  key-value: date-value
    :param list_date: list date formatted
    """
    plt.plot(list_date, dataframe[c.value_label], "-", linewidth=1, label=c.observations_label + ' - ' + series_key)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter(c.format_date))


def set_plot(series_key, name_graph, name_png):
    """ Set layout to plot
    :param series_key: series id
    :param name_graph: name to assign to the plot
    :param name_png: name of the image
    """
    # layout plot
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter(c.format_date))
    plt.title(name_graph)
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.grid(which='both', color='#CCCCCC', linestyle='--')
    plt.legend()
    plt.savefig("graph/" + name_png + "-" + series_key + "." + c.type_image)


def plot_observations(db_file, series_key, list_observations, interpolation_bool):
    """ Plot of an observation
    :param list_observations: list observations
    :param interpolation_bool: true if linear interpolation is desired
    :param db_file: name database
    :param series_key: series id
    """
    df_observations = linear_interpolation.if_interpolation(db_file, series_key,
                                                            list_observations,
                                                            interpolation_bool)
    # takes the formatted dates
    list_date = formatter_date_in_axis(df_observations)
    define_date_axis()
    pplot(series_key, df_observations, list_date)
    set_plot(series_key, "Observations", c.observations_label)
    plt.show()


def media_mobile(db_file, series_key, parameter_days, list_observations, interpolation_bool):
    """ Media mobile: linear, weighted and exponential
    :param list_observations: list observations
    :param interpolation_bool: true if linear interpolation is desired
    :param db_file: name database
    :param series_key: series id
    :param parameter_days: parameters n day for media mobile
    """
    df_observations = linear_interpolation.if_interpolation(db_file, series_key,
                                                            list_observations, interpolation_bool)
    list_date = formatter_date_in_axis(df_observations)
    define_date_axis()
    pplot(series_key, df_observations, list_date)

    weights = np.arange(1, parameter_days + 1)
    df_observations['SMA'] = df_observations[c.value_label].rolling(window=parameter_days).mean()
    df_observations['WMA'] = df_observations[c.value_label].rolling(parameter_days)\
        .apply(lambda value: np.dot(value, weights) / weights.sum(), raw=True)
    df_observations['EMA'] = df_observations[c.value_label].ewm(span=parameter_days).mean()
    df_observations.to_csv("csv/media-mobile-" + series_key + "." + c.save_type_file)

    plt.plot(list_date, df_observations['SMA'], linewidth=1, color="green", label='SMA - ' + str(parameter_days))
    plt.plot(list_date, df_observations['WMA'], linewidth=1, color="red", label='WMA - ' + str(parameter_days))
    plt.plot(list_date, df_observations['EMA'], linewidth=1, color="orange", label='EMA - ' + str(parameter_days))
    set_plot(series_key, "Media mobile", "media-mobile-" + str(parameter_days) + "days")
    plt.show()
