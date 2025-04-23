#Générer un rapport des cultures par mois avec la somme des besoins en eau, groupé par type de sol
import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.mydatabase

# Pipeline d'agrégation
pipeline = [
    {
        "$group": {
            "_id": {
                "month": "$month",
                "soil": "$soil"
            },
            "totalWaterReq": {"$sum": "$water req"}
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
        doc['_id']['soil'],
        doc['totalWaterReq']
    ])

# Définition des en-têtes de colonnes
headers = ["Month", "Soil Type", "Total Water Requirement"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

###################################### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.filtred_plants.aggregate([
  {
    $group: {
      _id: {
        month: "$month",
        soil: "$soil"
      },
      totalWaterReq: { $sum: "$water req" }
    }
  },
  {
    $sort: { "_id.month": 1 }
  }
])
'''
