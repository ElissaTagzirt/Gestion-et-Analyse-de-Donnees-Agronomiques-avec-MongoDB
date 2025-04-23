# Calculer la moyenne des températures minimales et maximales pour chaque type de culture dans chaque ville
import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.mydatabase

# Pipeline d'agrégation
pipeline = [
    {
        "$group": {
            "_id": {"crop": "$crop", "city": "$city"},
            "avgMinTemp": {"$avg": "$Min Temp"},
            "avgMaxTemp": {"$avg": "$Max Temp"}
        }
    },
    {
        "$sort": {"_id": 1}
    }
]

# Exécution de la requête d'agrégation
result = list(db.filtred_plants.aggregate(pipeline))

# Préparation des données pour l'affichage
table_data = []
for doc in result:
    table_data.append([
        doc['_id']['crop'],
        doc['_id']['city'],
        doc['avgMinTemp'],
        doc['avgMaxTemp']
    ])

# Définition des en-têtes de colonnes
headers = ["Crop Type", "City", "Avg Min Temp", "Avg Max Temp"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

############### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.filtred_plants.aggregate([
  {
    $group: {
      _id: { crop: "$crop", city: "$city" },
      avgMinTemp: { $avg: "$Min Temp" },
      avgMaxTemp: { $avg: "$Max Temp" }
    }
  },
  {
    $sort: { _id: 1 }
  }
])
'''
