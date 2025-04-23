##7. **Requête pour générer un rapport détaillé des cultures avec le maximum, minimum et moyenne des jours de culture pour chaque type de culture :**
import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]

# Pipeline d'agrégation pour calculer le max, min, et la moyenne des jours de culture pour chaque type de culture
pipeline = [
    {
        "$group": {
            "_id": "$CropType",  # Groupement par type de culture
            "maxCropDays": {"$max": "$CropDays"},  # Nombre maximum de jours de culture
            "minCropDays": {"$min": "$CropDays"},  # Nombre minimum de jours de culture
            "avgCropDays": {"$avg": "$CropDays"}  # Nombre moyen de jours de culture
        }
    }
]

# Exécution de la requête d'agrégation
result = list(db.arachides_riz.aggregate(pipeline))

# Préparation des données pour l'affichage
table_data = []
for doc in result:
    table_data.append([
        doc['_id'],
        doc['maxCropDays'],
        doc['minCropDays'],
        doc['avgCropDays']
    ])

# Définition des en-têtes de colonnes
headers = ["Crop Type", "Max Crop Days", "Min Crop Days", "Avg Crop Days"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

############### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.arachides_riz.aggregate([
  {
    $group: {
      _id: "$CropType",
      maxCropDays: { $max: "$CropDays" },
      minCropDays: { $min: "$CropDays" },
      avgCropDays: { $avg: "$CropDays" }
    }
  }
])
'''
