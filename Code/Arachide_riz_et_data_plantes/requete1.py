# Cette requête retourne les moyennes des paramètres de culture pour le riz en mai, groupées par type de sol et par ville. Les paramètres incluent :
# averageWaterReq : La moyenne des besoins en eau.
# averageTemp : La moyenne de la température.
# averageHumidity : La moyenne de l'humidité du sol (en pourcentage).
# averageMinTemp : La moyenne de la température minimale.
# averageMaxTemp : La moyenne de la température maximale.
# averageEnvHumidity : La moyenne de l'humidité environnementale.
# averageSoilMoisture : La moyenne de l'humidité du sol.
# averageSoilTemp : La moyenne de la température du sol.

import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]

# Définition du pipeline d'agrégation
pipeline = [
    {
        "$addFields": {
            "normalized_crop_type": {
                "$cond": {
                    "if": {"$eq": ["$CropType", "riz"]},
                    "then": "rice",
                    "else": "$CropType"
                }
            }
        }
    },
    {
        "$match": {
            "normalized_crop_type": "rice"
        }
    },
    {
        "$lookup": {
            "from": "filtred_plants",
            "localField": "normalized_crop_type",
            "foreignField": "crop",
            "as": "matched_env"
        }
    },
    {
        "$unwind": "$matched_env"
    },
    {
        "$match": {
            "matched_env.month": "May"
        }
    },
    {
        "$group": {
            "_id": {
                "soil": "$matched_env.soil",
                "city": "$matched_env.city"
            },
            "averageWaterReq": {"$avg": "$matched_env.water req"},
            "averageTemp": {"$avg": "$Temperature"},
            "averageHumidity": {"$avg": "$matched_env.Humidity"},  # Représente l'humidité du sol
            "averageMinTemp": {"$avg": "$matched_env.Min Temp"},
            "averageMaxTemp": {"$avg": "$matched_env.Max Temp"},
            "averageEnvHumidity": {"$avg": "$matched_env.Humidity"},
            "averageSoilMoisture": {"$avg": "$Soil Moisture"},
            "averageSoilTemp": {"$avg": "$Soil Temperature"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "soil": "$_id.soil",
            "city": "$_id.city",
            "averageWaterReq": 1,
            "averageTemp": 1,
            "averageHumidity": 1,  # Humidité du sol
            "averageMinTemp": 1,
            "averageMaxTemp": 1,
            "averageEnvHumidity": 1,
            "averageSoilMoisture": 1,
            "averageSoilTemp": 1
        }
    }
]

# Exécution de la requête d'agrégation
result = list(db.arachides_riz.aggregate(pipeline))

# Préparation des données pour l'affichage
table_data = []
for entry in result:
    table_data.append([
        entry['soil'],
        entry['city'],
        entry['averageWaterReq'],
        entry['averageTemp'],
        entry['averageHumidity'],  # Humidité du sol
        entry['averageMinTemp'],
        entry['averageMaxTemp'],
        entry['averageEnvHumidity'],
        entry['averageSoilMoisture'],
        entry['averageSoilTemp']
    ])

# Définition des en-têtes de colonnes
headers = ["Soil Type", "City", "Avg Water Req", "Avg Temp", "Avg Soil Humidity", "Avg Min Temp", "Avg Max Temp", "Avg Env Humidity", "Avg Soil Moisture", "Avg Soil Temp"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

###################################### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.arachides_riz.aggregate([
  {
    $addFields: {
      normalized_crop_type: {
        $cond: [
          { $eq: ["$CropType", "riz"] },
          "rice",
          "$CropType"
        ]
      }
    }
  },
  {
    $match: {
      normalized_crop_type: "rice"
    }
  },
  {
    $lookup: {
      from: "filtred_plants",
      localField: "normalized_crop_type",
      foreignField: "crop",
      as: "matched_env"
    }
  },
  {
    $unwind: "$matched_env"
  },
  {
    $match: {
      "matched_env.month": "May"
    }
  },
  {
    $group: {
      _id: {
        soil: "$matched_env.soil",
        city: "$matched_env.city"
      },
      averageWaterReq: { $avg: "$matched_env.water req" },
      averageTemp: { $avg: "$Temperature" },
      averageHumidity: { $avg: "$matched_env.Humidity" },  # Humidité du sol
      averageMinTemp: { $avg: "$matched_env.Min Temp" },
      averageMaxTemp: { $avg: "$matched_env.Max Temp" },
      averageEnvHumidity: { $avg: "$matched_env.Humidity" },
      averageSoilMoisture: { $avg: "$Soil Moisture" },
      averageSoilTemp: { $avg: "$Soil Temperature" }
    }
  },
  {
    $project: {
      _id: 0,
      soil: "$_id.soil",
      city: "$_id.city",
      averageWaterReq: 1,
      averageTemp: 1,
      averageHumidity: 1,  # Humidité du sol
      averageMinTemp: 1,
      averageMaxTemp: 1,
      averageEnvHumidity: 1,
      averageSoilMoisture: 1,
      averageSoilTemp: 1
    }
  }
])
'''
