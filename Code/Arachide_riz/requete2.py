#Requête pour trouver les enregistrements où les conditions 
# environnementales sont extrêmes (Température > 30 ou Humidité < 60) et nécessitent une irrigation :
import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client["mydatabase"]

# Définition de la requête pour trouver les enregistrements où les conditions environnementales sont extrêmes et nécessitent une irrigation
query = {
    "Irrigation(Y/N)": "Y",
    "$or": [
        {"Temperature": {"$gt": 30}},
        {"Humidity": {"$lt": 60}}
    ]
}

# Récupération des résultats
result = db.arachides_riz.find(query)

# Préparation des données pour l'affichage
table_data = []
for doc in result:
    table_data.append([
        doc['CropType'],
        doc['Irrigation(Y/N)'],
        doc['Temperature'],
        doc['Humidity'],
        doc['Soil Moisture'],
        doc['Soil Temperature'],
        doc['_id']
    ])

# Définition des en-têtes de colonnes
headers = ["Crop Type", "Irrigation", "Temperature", "Humidity", "Soil Moisture", "Soil Temperature", "ID"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

############### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.arachides_riz.find({
  "Irrigation(Y/N)": "Y",
  $or: [
    { Temperature: { $gt: 30 } },
    { Humidity: { $lt: 60 } }
  ]
})
'''
