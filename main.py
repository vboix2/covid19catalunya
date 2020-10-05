import pandas as pd
from google.cloud import storage
from io import StringIO

def update(request):
	# Registre de casos diaris per comarca
	casos_comarca = pd.read_json('https://analisi.transparenciacatalunya.cat/resource/c7sd-zy9j.json?$limit=300000', dtype= {'codi':str})

	# Conversió de la data
	casos_comarca['data'] = pd.to_datetime(casos_comarca['data'], format="%Y-%m-%dT%H:%M:%S.%f")
	casos_comarca['data'] = casos_comarca['data'].apply(lambda x: x.strftime("%Y-%m-%d"))

	# Eliminem nom comarca
	casos_comarca.drop(columns=['nom'], inplace=True)

	# Dades població comarques
	comarques = pd.read_csv('https://storage.googleapis.com/datascience-290812/covid19catalunya/poblacio_comarques.csv', dtype = {'codi': str})

	# Unió de les dades
	casos_comarca = pd.merge(casos_comarca, comarques, how = 'left', on = 'codi')

	# Exportació de dades
	f = StringIO()
	casos_comarca.to_csv(f, index=False)
	f.seek(0)
	storage_client = storage.Client.from_service_account_json('datascience-290812-7413488fdd43.json')
	bucket = storage_client.get_bucket('datascience-290812')
	blob = bucket.blob('covid19catalunya/casos_comarca.csv')
	blob.upload_from_string(f.getvalue(), content_type='text/csv')

	return "Dades actualitzades"
