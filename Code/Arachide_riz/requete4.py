#Requête pour trouver les enregistrements où l'irrigation est nécessaire et
# où la température du sol et l'humidité sont supérieures à la moyenne pour chaque type de culture 
import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]

# Pipeline d'agrégation pour trouver les enregistrements où l'irrigation est nécessaire et où les conditions sont au-dessus de la moyenne
pipeline = [
    # Étape 1 : Calculer la température moyenne du sol et l'humidité pour chaque type de culture
    {
        "$group": {
            "_id": "$CropType",
            "avgSoilTemperature": {"$avg": "$Soil Temperature"},
            "avgHumidity": {"$avg": "$Humidity"}
        }
    },
    # Étape 2 : Trouver les enregistrements où l'irrigation est nécessaire et où les conditions sont au-dessus de leurs moyennes
    {
        "$lookup": {
            "from": "arachides_riz",
            "let": {"type": "$_id", "avgTemp": "$avgSoilTemperature", "avgHum": "$avgHumidity"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$CropType", "$$type"]},
                                {"$gt": ["$Soil Temperature", "$$avgTemp"]},
                                {"$gt": ["$Humidity", "$$avgHum"]},
                                {"$eq": ["$Irrigation(Y/N)", "Y"]}
                            ]
                        }
                    }
                }
            ],
            "as": "records"
        }
    },
    # Flatten the results for easy viewing
    {
        "$unwind": "$records"
    },
    # Make the documents more readable
    {
        "$replaceRoot": {"newRoot": "$records"}
    }
]

# Exécution de la requête d'agrégation
result = list(db.arachides_riz.aggregate(pipeline))

# Préparation des données pour l'affichage
table_data = []
for doc in result:
    table_data.append([
        doc['CropType'],
        doc['Irrigation(Y/N)'],
        doc['Soil Temperature'],
        doc['Humidity'],
        doc['_id']
    ])

# Définition des en-têtes de colonnes
headers = ["Crop Type", "Irrigation", "Soil Temperature", "Humidity", "ID"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

############### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.arachides_riz.aggregate([
  {
    $group: {
      _id: "$CropType",
      avgSoilTemperature: { $avg: "$Soil Temperature" },
      avgHumidity: { $avg: "$Humidity" }
    }
  },
  {
    $lookup: {
      from: "arachides_riz",
      let: { type: "$_id", avgTemp: "$avgSoilTemperature", avgHum: "$avgHumidity" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$CropType", "$$type"] },
                { $gt: ["$Soil Temperature", "$$avgTemp"] },
                { $gt: ["$Humidity", "$$avgHum"] },
                { $eq: ["$Irrigation(Y/N)", "Y"] }
              ]
            }
          }
        }
      ],
      as: "records"
    }
  },
  {
    $unwind: "$records"
  },
  {
    $replaceRoot: { newRoot: "$records" }
  }
])
'''

