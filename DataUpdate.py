# ---- Paquets

import numpy as np
import pandas as pd
import xml.etree.ElementTree as et
import requests
from google.cloud import storage


# ---- Dades covid

# Registre de casos diaris per comarca
casos_comarca = pd.read_json('https://analisi.transparenciacatalunya.cat/resource/c7sd-zy9j.json?$limit=300000',
                            dtype= {'codi':str})

# Conversió de la data
casos_comarca['data'] = pd.to_datetime(casos_comarca['data'], format="%Y-%m-%dT%H:%M:%S.%f")
casos_comarca['data'] = casos_comarca['data'].apply(lambda x: x.strftime("%Y-%m-%d"))

# Eliminem nom comarca
casos_comarca.drop(columns=['nom'], inplace=True)


# ---- Dades comarques

# Població de Catalunya per comarca
responses = requests.get('https://api.idescat.cat/emex/v1/dades.xml?i=f171&tipus=com')
root = et.fromstring(responses.content)
poblacio = root[1][0][1].text.split(", ")
poblacio = [ int(x) for x in poblacio ]
codi = []
comarca = []
for node in root.iter('col'):
    codi.append(node.attrib['id'])
    comarca.append(node.text)

comarques = pd.DataFrame({'codi': codi, 'habitants':poblacio, 'comarca': comarca})


# ---- Unió de les dades

casos_comarca = pd.merge(casos_comarca, comarques, how = 'left', on = 'codi')


# ---- Exportació de dades

casos_comarca.to_csv('casos_comarca.csv', index=False)

storage_client = storage.Client.from_service_account_json('datascience-290812-7413488fdd43.json')
bucket = storage_client.get_bucket('datascience-290812')

blob = bucket.blob('covid19catalunya/casos_comarca.csv')
blob.upload_from_filename('casos_comarca.csv')