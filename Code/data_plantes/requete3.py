#Trouver les cultures avec des conditions de vent et de radiation solaires élevées
#dans des villes avec une altitude inférieure à 100 mètres. La requête sélectionne 
# les six premiers enregistrements répondant à ces critères.

import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client["mydatabase"]

# Seuils pour considérer les conditions de vent et de radiation comme élevées
seuil_vent = 200  # Par exemple, 200 km/h
seuil_radiation = 20  # Par exemple, 20 MJ/m^2

# Requête pour trouver les cultures appropriées
query = {
    "altitude": {"$lt": 100},
    "Wind": {"$gt": seuil_vent},
    "Rad": {"$gt": seuil_radiation}
}

# Exécution de la requête
resultats = list(db.filtred_plants.find(query).limit(6))

# Préparation des données pour l'affichage
table_data = []
for doc in resultats:
    table_data.append([
        doc['crop'],
        doc['city'],
        doc['altitude'],
        doc['Wind'],
        doc['Rad']
    ])

# Définition des en-têtes de colonnes
headers = ["Crop Type", "City", "Altitude", "Wind Speed", "Solar Radiation"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

###################################### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.filtred_plants.find({
  "altitude": { "$lt": 100 },
  "Wind": { "$gt": 200 },
  "Rad": { "$gt": 20 }
}).limit(6)
'''
