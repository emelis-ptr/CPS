import matplotlib.pyplot as plt
from sklearn import linear_model
import pandas as pd
import seaborn as sns
import graphs


def graphs_series(dataframe, series_key):
    """ create plots of observations
    :param dataframe: key-value: date-value
    :param series_key: id series
    """
    list_date = graphs.formatter_date_in_axis(dataframe)
    graphs.pplot(series_key, dataframe, list_date)


def covariance(dataframe, series_keys):
    """ calculate covariance
    :param dataframe: key-value: date-value
    :param series_keys: ids series
    """
    covariance_matrix = pd.DataFrame.cov(dataframe)
    plt.figure(figsize=(20, 5))
    sns.heatmap(covariance_matrix, annot=True, fmt='g', xticklabels=series_keys, yticklabels=series_keys)
    plt.title("Covariance")
    plt.savefig("graph/" + "covariance" + "-" + str(series_keys) + ".png")
    plt.show()


def difference_plot(dataframe, series_key):
    """ calculate difference
    :param dataframe: key-value: date-value
    :param series_key: id series
    """
    list_date = graphs.formatter_date_in_axis(dataframe)
    dataframe['difference'] = dataframe['value'].diff()
    plt.plot(list_date, dataframe['difference'], "-", linewidth=1, label='Observations - ' + series_key)


def percentage_plot(dataframe, series_key):
    """ calculate percentage difference
    :param dataframe: key-value: date-value
    :param series_key: id series
    """
    list_date = graphs.formatter_date_in_axis(dataframe)
    dataframe['percentage'] = dataframe['value'].pct_change()
    plt.plot(list_date, dataframe['percentage'], "-", linewidth=1, label='Observations - ' + series_key)


def linear_regression(dataframe, series_key):
    """ calculate linear regression
    :param dataframe: key-value: date-value
    :param series_key: id series
    """
    list_date = graphs.formatter_date_in_axis(dataframe)
    dataframe['days_from_start'] = (dataframe.index - dataframe.index[0])

    x = dataframe['days_from_start'].values.reshape(-1, 1)
    y = dataframe['value'].values

    regression = linear_model.LinearRegression()
    regression.fit(x, y)
    date_start = list_date[0]
    date_end = list_date[len(list_date) - 1]

    plt.figure(figsize=(10, 5))
    plt.scatter(list_date, y, color='green', marker='x', linewidth=1, label='Observations - ' + str(series_key))
    plt.xlim([date_start, date_end])
    plt.plot(list_date, regression.predict(x), "-", color='red', linewidth=1, label='Regression')

    graphs.set_plot(series_key, "Linear Regression", "linear_regression")
    plt.show()
