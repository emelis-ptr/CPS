import pandas as pd
from src.fred_cps.utils import constants as c


# Classe contententi funzioni per il calcolo dell'interpolazione
def _formula_interpolation(index, values):
    """Metodo contenente la formula per il calcolo dell'interpolazione
    :param index: indice che specifica dove si trova il valore mancante
    :param values: lista dei valori
    :return: formula """
    # it takes the preceding index and the successor of the i-th and
    # returns the formula of the linear interpolation
    index_pre = index - 1
    index_succ = index + 1
    return (values[index_succ] - values[index_pre]) * (index - index_pre) / \
        (index_succ - index_pre) + values[index_pre]


class Model:

    def __init__(self, data_dict):
        self.data_dict = data_dict

    def get_observations(self):
        """Metodo che trasforma una dictionary in un dataframe
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
        """Metodo che calcola l'interpolazione di un dataframe se esistono valori NaN
            :return: dataframe """
        dataframe = self.get_observations()
        dataframe[c.value_label] = pd.to_numeric(dataframe[c.value_label], errors='coerce')
        dataframe[c.value_label] = dataframe[c.value_label].interpolate()

        return dataframe

    def linear_interpolation(self):
        """Metodo che calcola l'interpolazione dei dati dato una dictionary
        :return: dictionary delle osservabili """
        list_observations = {}

        for i in self.data_dict.items():
            count = 0
            for j in i[1]:
                count += 1
                list_observations[count] = j

        for index, value in enumerate(list_observations.values()):
            values = list(list_observations.values())

            if value['value'] == 0.0:
                value['value'] = _formula_interpolation(index, values)
                list_observations[index] = value['value']
        return list_observations
