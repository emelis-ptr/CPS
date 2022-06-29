import database
import graphs
import main
import retrieve_data
from utils import constants as c


def example(parameter_days, interpolation_bool):
    """ Retrieve categories, series and observations from url and write them to a json file.
    Create databases, tables by entering the values obtained.
    Obtain the plots of the observations
            :param interpolation_bool: true if linear interpolation
            :param parameter_days: parameter for media mobile
        """
    retrieve_data.get_categories(c.database_name, c.category_root, main.api_key, 0)
    retrieve_data.get_series(c.database_name, c.category_root, main.api_key, 0)
    list_observations = retrieve_data.get_observations(c.database_name, c.series_key, main.api_key, 0)
    graphs.draw_plots(c.database_name, c.series_key, parameter_days, list_observations, interpolation_bool)

    database.drop_table(c.database_name)
    database.create_connection(c.database_name).close()



