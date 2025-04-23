#Trouver les cultures qui nécessitent le plus d'eau pendant le mois de mai
import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.mydatabase

# Pipeline d'agrégation
pipeline = [
    {
        "$match": {"month": "May"}
    },
    {
        "$group": {
            "_id": "$crop",
            "averageWaterReq": {"$avg": "$water req"}
        }
    },
    {
        "$sort": {"averageWaterReq": -1}
    }
]

# Exécution de la requête d'agrégation
result = list(db.filtred_plants.aggregate(pipeline))

# Préparation des données pour l'affichage
table_data = []
for doc in result:
    table_data.append([
        doc['_id'],
        doc['averageWaterReq']
    ])

# Définition des en-têtes de colonnes
headers = ["Crop Type", "Average Water Requirement"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

###################################### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.filtred_plants.aggregate([
  {
    $match: { month: "May" }
  },
  {
    $group: {
      _id: "$crop",
      averageWaterReq: { $avg: "$water req" }
    }
  },
  {
    $sort: { averageWaterReq: -1 }
  }
])
'''
