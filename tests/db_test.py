import logging
import unittest

from src.fred_cps.api import ApiFred
from src.fred_cps.database import Database
from src.fred_cps.utils import constants as c

apiKey = "b933025b5b47791c3552c8ce24305e99"
key_ob = "PHMEPRPIHCSA"
key_cat = 2
key_ser = "33967"


class ApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.DEBUG)
        self.db = Database(c.database_name)

    def test_select_categories(self):
        api = ApiFred(apiKey)
        api.get_categories(key_cat)

        categories_db = api.db.select_categories(key_cat)

        self.assertTrue(len(categories_db) > 0, "I valori delle categorie nel database non sono stati "
                                                "inseriti tutti")

        parentid_db = api.db.select_categories_parentid(key_cat)

        print("Numero di elementi che hanno parent_id:" + str(key_cat) + " sono: " + str(len(parentid_db)))
        self.assertTrue(len(parentid_db) > 0, "I valori delle categorie nel database non sono stati "
                                              "inseriti tutti")

    def test_select_series(self):
        api = ApiFred(apiKey)
        api.get_series(key_ser)

        series_db = api.db.select_series_category(key_ser)

        self.assertTrue(len(series_db) > 0, "I valori di osservazioni nel database non sono stati "
                                            "inseriti tutti")

    def test_select_observation(self):
        api = ApiFred(apiKey)
        api.get_observations(key_ob)

        observations_db = api.db.select_observations(key_ob)

        self.assertTrue(len(observations_db) > 0, "I valori di osservazioni nel database non sono stati "
                                                  "inseriti tutti")


if __name__ == '__main__':
    unittest.main()
