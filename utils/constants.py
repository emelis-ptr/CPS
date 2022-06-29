# retrieve_data.py #
url = "https://api.stlouisfed.org/fred/"
name = 'name'
categories_label = 'categories'
parent_id_label = 'parent_id'
children_label = 'children'
series_label = 'series'
seriess_label = 'seriess'
id_label = 'id'
title = 'title'
frequency = 'frequency'
frequency_short = 'frequency_short'
observations_label = 'observations'
date_label = 'date'
value_label = 'value'
type_file = 'json'
dir_graphs = "graph"

# database.py
dir_cat_json = type_file + "/" + categories_label + "-"
dir_series_json = type_file + "/" + series_label + "-"
dir_obs_json = type_file + "/" + observations_label + "-"

# graphs.py
format_date = '%Y-%m-%d'
type_image = "png"
save_type_file = "csv"

# example
database_name = "ProgettoCPS.sqlite"
category_root = 0
series_key = "DBKAC"
