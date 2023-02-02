import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
import pandas as pd
import seaborn as sns
from .model import Model
from sklearn import linear_model

import src.fred_cps.utils.constants as c


class Graph:

    def __init__(self, series_key, data_dict):
        self.series_key = series_key
        self.data_dict = data_dict
        self.model = Model(data_dict)

    def media_mobile(self, parameter_days, interpolation_bool):
        """ Media mobile: linear, weighted and exponential
        :param interpolation_bool: true if linear interpolation is desired
        :param parameter_days: parameters n day for media mobile
        """
        if interpolation_bool:
            df_observations = self.model.linear_interpolation()
        else:
            df_observations = self.model.get_observations()

        if not isinstance(self.data_dict, pd.DataFrame):
            df_observations = self.model.get_observations()

        list_date = self._formatter_date_in_axis(df_observations)
        self._define_date_axis()
        self._pplot(df_observations, list_date)

        weights = np.arange(1, parameter_days + 1)
        df_observations['SMA'] = df_observations[c.value_label].rolling(window=parameter_days).mean()
        df_observations['WMA'] = df_observations[c.value_label].rolling(parameter_days) \
            .apply(lambda value: np.dot(value, weights) / weights.sum(), raw=True)
        df_observations['EMA'] = df_observations[c.value_label].ewm(span=parameter_days).mean()
        df_observations.to_csv("csv/media-mobile-" + self.series_key + "." + c.save_type_file)

        plt.plot(list_date, df_observations['SMA'], linewidth=1, color="green", label='SMA - ' + str(parameter_days))
        plt.plot(list_date, df_observations['WMA'], linewidth=1, color="red", label='WMA - ' + str(parameter_days))
        plt.plot(list_date, df_observations['EMA'], linewidth=1, color="orange", label='EMA - ' + str(parameter_days))
        self.set_plot("Media mobile", "media-mobile-" + str(parameter_days) + "days")
        plt.show()

    def covariance(self, series_id, dataframe):
        """ calculate covariance
        :param dataframe:
        :param series_id:
        """
        if not isinstance(dataframe, pd.DataFrame):
            dataframe = self.model.get_observations()
            dataframe['value'] = dataframe['value'].astype('float')
        else:
            dataframe[series_id] = dataframe[series_id].astype('float')

        covariance_matrix = pd.DataFrame.cov(dataframe)
        plt.figure(figsize=(20, 5))
        sns.heatmap(covariance_matrix, annot=True, fmt='g', xticklabels=series_id, yticklabels=series_id)
        plt.title("Covariance")
        plt.savefig("graph/" + "covariance" + "-" + str(series_id) + ".png")
        plt.show()

    def difference_plot(self, interpolation_bool):
        """ calculate difference
        :param interpolation_bool:
        """
        if interpolation_bool:
            dataframe = self.model.linear_interpolation()
        else:
            dataframe = self.model.get_observations()

        if not isinstance(dataframe, pd.DataFrame):
            dataframe = self.model.get_observations()
        list_date = self._formatter_date_in_axis(dataframe)
        dataframe['value'] = dataframe['value'].astype('float')
        dataframe['difference'] = dataframe['value'].diff()
        plt.plot(list_date, dataframe['difference'], "-", linewidth=1, label='Observations - ' + self.series_key)

    def percentage_plot(self, interpolation_bool):
        """ calculate percentage difference
        :param interpolation_bool:
        """
        if interpolation_bool:
            dataframe = self.model.linear_interpolation()
        else:
            dataframe = self.model.get_observations()

        if not isinstance(dataframe, pd.DataFrame):
            dataframe = self.model.get_observations()
        list_date = self._formatter_date_in_axis(dataframe)
        dataframe['value'] = dataframe['value'].astype('float')
        dataframe['percentage'] = dataframe['value'].pct_change()
        plt.plot(list_date, dataframe['percentage'], "-", linewidth=1, label='Observations - ' + self.series_key)

    def linear_regression(self, dataframe):
        """ calculate linear regression
        :param dataframe: key-value: date-value
        """
        list_date = self._formatter_date_in_axis(dataframe)
        dataframe['days_from_start'] = (dataframe.index - dataframe.index[0])

        x = dataframe['days_from_start'].values.reshape(-1, 1)
        y = dataframe['value'].values

        regression = linear_model.LinearRegression()
        regression.fit(x, y)
        date_start = list_date[0]
        date_end = list_date[len(list_date) - 1]

        plt.figure(figsize=(10, 5))
        plt.scatter(list_date, y, color='green', marker='x', linewidth=1,
                    label='Observations - ' + str(self.series_key))
        plt.xlim([date_start, date_end])
        plt.plot(list_date, regression.predict(x), "-", color='red', linewidth=1, label='Regression')

        self.set_plot("Linear Regression", "linear_regression")
        plt.show()

    def plot_observations(self, interpolation_bool):
        """ Plot of an observation
        :param interpolation_bool: true if linear interpolation is desired
        """
        if interpolation_bool:
            df_observations = self.model.linear_interpolation()
        else:
            df_observations = self.model.get_observations()

        if not isinstance(self.data_dict, pd.DataFrame):
            df_observations = self.model.get_observations()
        # takes the formatted dates
        list_date = self._formatter_date_in_axis(df_observations)
        self._define_date_axis()
        self._pplot(df_observations, list_date)
        self.set_plot("Observations", c.observations_label)
        plt.show()

    def _formatter_date_in_axis(self, dataframe):
        """ format the string to date
        :param dataframe: key-value: date-value
        :return:
        """
        # formatter dates
        date_formatter = [dt.datetime.strptime(d, c.format_date).date() for d in dataframe[c.date_label]]
        # plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        return date_formatter

    def set_plot(self, name_graph, name_png):
        """ Set layout to plot
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
        plt.savefig("graph/" + name_png + "-" + self.series_key + "." + c.type_image)

    def _pplot(self, dataframe, list_date):
        """ Plots
        :param dataframe:  key-value: date-value
        :param list_date: list date formatted
        """
        plt.plot(list_date, dataframe[c.value_label], "-", linewidth=1,
                 label=c.observations_label + ' - ' + self.series_key)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter(c.format_date))

    def _define_date_axis(self):
        """ distance between one date and another
        """
        # frequency date in x-axis
        fig = plt.figure()
        ax = fig.add_subplot()
        year = mdates.YearLocator(10)
        ax.xaxis.set_major_locator(year)
        fig.autofmt_xdate(rotation=45)
