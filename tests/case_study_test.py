import unittest
import logging

from matplotlib import pyplot as pyplt
import pandas as pds
from src.fred_cps.graphs import Graph
from src.fred_cps.model import Model
from src.fred_cps.database import Database
from src.fred_cps.utils import constants as c
from src.fred_cps.api import ApiFred

apiKey = "b933025b5b47791c3552c8ce24305e99"
series_id = ["PHMEPRPIHCSA", "MNINEIPIMEPS", "DIMSCTPIMEPS"]
interpolation_bool = True


class ApiTestCase(unittest.TestCase):

    def setUp(self) -> None:
        logging.basicConfig(level=logging.DEBUG)
        self.db = Database(c.database_name)

    def test_all_categories(self):
        api = ApiFred(apiKey)
        categories = api.get_categories(2)

        # scaricati
        self.assertTrue(len(categories) > 0, "Errore. Nessuna categoria è stata scaricata")

        # database
        categories_db = self.db.select_all_categories()
        self.assertTrue(len(categories_db) > 0, "Categorie inserite nel database")

    def test_plot_observation(self):
        """ From a list of keys series determine the plot, difference,
        percentage difference, covariance and linear regression of the observations
        """
        api = ApiFred(apiKey)
        # for each series retrieve the values of the observations and write them on json.
        # Create databases and recover data.
        # Plot the observations together
        # Plot observations
        pyplt.figure(figsize=(10, 5))
        for j in series_id:
            list_observations = api.get_observations(j)

            graph = Graph(j, list_observations)
            graph.plot_observations(True)

        pyplt.show()

    def test_media_mobile(self):
        global graphs
        api = ApiFred(apiKey)

        for j in series_id:
            list_observations = api.get_observations(j)

            graphs = Graph(j, list_observations)
            graphs.media_mobile(5, True)

        pyplt.show()

    def test_diff_prime(self):
        global graphs
        api = ApiFred(apiKey)
        # for each series create difference plots
        # Plot difference s(i-1) -s(i)
        pyplt.figure(figsize=(10, 5))
        for j in series_id:
            list_observations = api.get_observations(j)

            graphs = Graph(j, list_observations)
            graphs.difference_plot(True)

        graphs.set_plot("Difference", "difference")
        pyplt.show()

    def test_diff_percentage(self):
        global graphs
        api = ApiFred(apiKey)
        # for each series, create plot of the percentage differences
        # Plot difference percentage (s(i-1)-s(i))/s(i)
        pyplt.figure(figsize=(10, 5))
        for j in series_id:
            list_observations = api.get_observations(j)

            graphs = Graph(j, list_observations)
            graphs.percentage_plot(True)

        graphs.set_plot("Percentage", "percentage")
        pyplt.show()

    def test_covariance(self):
        # Covariance
        global graphs
        api = ApiFred(apiKey)

        dataframe = pds.DataFrame()
        count = 0
        for j in series_id:
            list_observations = api.get_observations(j)

            graphs = Graph(j, list_observations)
            model = Model(list_observations)

            ob_df = model.get_observations()
            dataframe.insert(count, j, ob_df['value'], True)
            count += 1

        graphs.covariance(series_id, dataframe)

    def test_linear_regression(self):
        # Linear regression
        global graphs
        api = ApiFred(apiKey)

        for j in series_id:
            list_observations = api.get_observations(j)

            graphs = Graph(j, list_observations)
            model = Model(list_observations)

            df = model.interpolate_dataframe()
            graphs.linear_regression(df)


if __name__ == '__main__':
    unittest.main()
