# CPS-Project
***
## **Setup** ##
Se si utilizza windows occorre installare SQLite3  e inserire il path nelle variabili d'ambiente.

Per installare il package utilizzare il comando:
https://test.pypi.org/project/ProgettoCPS/0.0.1/


## **Funzioni** ##
### **retrieve_data.py** ###

- Nelle funzioni get_url_category, get_url, get_url observations ritornano l'url delle categorie, serie e osservazioni. 
Bisogna specificare api, l'id della categoria o della seria e nella
seconda attraverso "tag" è necessario specificare se si vuole ottenere l'url dei children o le serie di una data categoria.

- Nelle funzioni get_category_data, get_data i records vengono ritornati come valori json convertiti successivamente in testo.

- Nelle funzioni recursive_categories, recursive_series, recursive observations in modo ricorsivo vengono inserite all'interno di
una directory le categorie, serie e osservazioni. Per ogni id verifica se esistono dei valori li inserisce, altrimenti ritorna la funzione.

- Le funzioni get_categories, get_series e get_observations ritornano i dictionary dei record ottenuti.

### **dataset.py** ###
All'interno di questo file avviena la connessione con il database SQLite3.
Infatti attraverso create_connection viene creata una connessione con il database specificando il nome
che si vuole dare.

- Le funzioni create_table_category, create_table_series, create_table_observations vengono create le rispettive tabelle
e inserisci i record che sono stati ottenuti tramite le funzioni in retrieve_data.py.

- Le funzioni insert_categories, insert_series e insert_observations verifica se esiste un file json con l'id della categoria o serie che sono state
scaricate altrimenti recupera i records dai dictionaries. 

- La funzione select_observations seleziona i valori date e value dalla tabella observations con un determinato id specificato dall'utente.

- Le funzioni insert_value_categories, insert_value_series e insert_value_observations 
in modo ricorsivo recuperano i valori dal file json o dictionary per poi essere inseriti nelle tabelle.

### **graphs.py** ###

- La funzione get_value_date_observations verifica se esiste la tabella delle osservazioni e seleziona i valori con l'id corrispondente.
Questi valori date e value vengono inseriti all'interno di un dataframe come due colonne distinte.

- La funzione fomatter_date_in_axis formatta la dat presa dal database in un valore date.

- La funzione pplot serve semplicemente per poter creare un plot con un id di una serie; inserisce
nell'asse delle x i valori delle date e nell'asse delle y i valori della serie.

- La funzione set_plot serve a creare un layout di un plot. è possibile inserire il nome del plot e il nome
dell'immagine png che si vuole salvare.

- La funzione plot_observations crea un plot di un'osservazione. E se il campo interpolation_bool è True allora 
viene fatta un'interpolazione dei valori Nan.

- La funzione media_mobile_simple crea un plot con le diverse medie mobili: lineare, pesata ed esponenziale.
E' possibile specificare un parametro che corrisponde a n giorni per poter variare l'andamento dei plot.

### **linear_interpolation.py** ###
In questo file esistono due funzioni diverse per poter calcolar el'interpolazione.
Una funzione che attraverso una formula determina il valore dei Nan e un'altra funzione
che attraverso il dataframe calcola automaticamanete l'interpolazione dei valori.

- La funzione interpolate_dataframe attraverso il dataframe riesce a determinare i valori Nan di una lista di osservazioni.

- La funzione if_interpolation ritorna il dataframe con i valori interpolati o meno a seconda se il valore booleano è 
settato a True.

- Le funzioni linear_interpolation e formula_interpolation calcolano l'interpolazione
dei valori attraverso la formula:
> (s(k) - s(j) - (i - j) / (k - j) + s(j)
con k>i e j<i. 
> 

### **json_file.py** ###

- Le funzioni write_json e open_json servono per scrivere e leggere i records.

### **case_study_function.py** ###
Attraverso q0x39, 0x07, 0x75, 0x6e, 0x69, 0x31, 0x45, 0x42, 0x43, 0x07, 0x75, 0x6e, 0x69,
	0x31, 0x45, 0x42, 0x44, 0x07, 0x75, 0x6e, 0x69, 0x31, 0x45, 0x43, 0x36, 0x07, 0x75, 0x6e, 0x69,
	0x