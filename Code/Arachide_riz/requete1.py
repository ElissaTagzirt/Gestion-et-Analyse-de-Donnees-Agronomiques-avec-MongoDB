# Cette requête calcule les moyennes de la température du
# sol (Soil Temperature) et de l'humidité du sol (Soil Moisture) 
# en fonction du type de culture et de l'irrigation.
import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]

# Définition du pipeline d'agrégation
pipeline = [
    {
        "$group": {
            "_id": {
                "cropType": "$CropType",
                "irrigation": "$Irrigation(Y/N)"
            },
            "averageSoilTemp": {"$avg": "$Soil Temperature"},
            "averageSoilMoisture": {"$avg": "$Soil Moisture"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "cropType": "$_id.cropType",
            "irrigation": "$_id.irrigation",
            "averageSoilTemp": 1,
            "averageSoilMoisture": 1
        }
    }
]

# Exécution de la requête d'agrégation
result = list(db.arachides_riz.aggregate(pipeline))

# Préparation des données pour l'affichage
table_data = []
for entry in result:
    table_data.append([
        entry['cropType'],
        entry['irrigation'],
        entry['averageSoilTemp'],
        entry['averageSoilMoisture']
    ])

# Définition des en-têtes de colonnes
headers = ["Crop Type", "Irrigation", "Average Soil Temperature", "Average Soil Moisture"]
print("Cette requête calcule les moyennes de la température du sol (Soil Temperature) et de l'humidité du sol (Soil Moisture) en fonction du type de culture et de l'irrigation.")
# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))


############### Requête équivalente à exécuter sur le terminal MongoDB ######################################
'''db.arachides_riz.aggregate([
  {
    $group: {
      _id: {
        cropType: "$CropType",
        irrigation: "$Irrigation(Y/N)"
      },
      averageSoilTemp: { $avg: "$Soil Temperature" },
      averageSoilMoisture: { $avg: "$Soil Moisture" }
    }
  },
  {
    $project: {
      _id: 0,
      cropType: "$_id.cropType",
      irrigation: "$_id.irrigation",
      averageSoilTemp: 1,
      averageSoilMoisture: 1
    }
  }
])
'''


