#Trouver les cultures ayant des températures minimales et maximales les plus élevées et les plus basses pour chaque mois
import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.mydatabase

# Pipeline d'agrégation
pipeline = [
    {
        "$group": {
            "_id": {"month": "$month", "crop": "$crop"},
            "maxMaxTemp": {"$max": "$Max Temp"},
            "minMaxTemp": {"$min": "$Max Temp"},
            "maxMinTemp": {"$max": "$Min Temp"},
            "minMinTemp": {"$min": "$Min Temp"}
        }
    },
    {
        "$sort": {"_id.month": 1}
    }
]

# Exécution de la requête d'agrégation
result = list(db.filtred_plants.aggregate(pipeline))

# Préparation des données pour l'affichage
table_data = []
for doc in result:
    table_data.append([
        doc['_id']['month'],
        doc['_id']['crop'],
        doc['maxMaxTemp'],
        doc['minMaxTemp'],
        doc['maxMinTemp'],
        doc['minMinTemp']
    ])

# Définition des en-têtes de colonnes
headers = ["Month", "Crop Type", "Max Max Temp", "Min Max Temp", "Max Min Temp", "Min Min Temp"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

###################################### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.filtred_plants.aggregate([
  {
    $group: {
      _id: { month: "$month", crop: "$crop" },
      maxMaxTemp: { $max: "$Max Temp" },
      minMaxTemp: { $min: "$Max Temp" },
      maxMinTemp: { $max: "$Min Temp" },
      minMinTemp: { $min: "$Min Temp" }
    }
  },
  {
    $sort: { "_id.month": 1 }
  }
])
'''
