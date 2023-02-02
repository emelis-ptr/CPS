import pandas as pd
from src.fred_cps.utils import constants as c


class Model:

    def __init__(self, data_dict):
        self.data_dict = data_dict

    def get_observations(self):
        """ Insert date and value from observations in Dataframe
        :return: dataframe
        """
        list_id = []
        list_date = []
        list_value = []
        for row in self.data_dict:
            for i in self.data_dict[row]:
                list_id.append(i['id'])
                list_date.append(i['date'])
                list_value.append(i['value'])

        df = pd.DataFrame(list(zip(list_id, list_date, list_value)), columns=["id", c.date_label, c.value_label])
        return df

    def interpolate_dataframe(self):
        """ linear interpolation of a daframe if nan values exist
            :return: dataframe
            """
        dataframe = self.get_observations()
        dataframe[c.value_label] = pd.to_numeric(dataframe[c.value_label], errors='coerce')
        dataframe[c.value_label] = dataframe[c.value_label].interpolate()

        return dataframe

    def linear_interpolation(self):
        """ linear interpolation
        """
        list_observations = {}

        for i in self.data_dict.items():
            count = 0
            for j in i[1]:
                count += 1
                list_observations[count] = j

        for index, value in enumerate(list_observations.values()):
            values = list(list_observations.values())

            if value['value'] == 0.0:
                value['value'] = self._formula_interpolation(index, values)
                list_observations[index] = value['value']
        return list_observations

    def _formula_interpolation(self, index, values):
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
