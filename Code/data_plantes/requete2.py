#Trouver les enregistrements avec des besoins en eau supérieurs à la moyenne pour chaque type de culture.
import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client["mydatabase"]

# Pipeline pour calculer la moyenne des besoins en eau pour chaque type de culture
pipeline_moyenne = [
    {
        "$group": {
            "_id": "$crop",
            "moyenneBesoinEau": {"$avg": "$water req"}
        }
    }
]

# Exécution du pipeline pour obtenir les moyennes
moyennes_besoins_eau = list(db.filtred_plants.aggregate(pipeline_moyenne))
dictionnaire_moyennes = {data['_id']: data['moyenneBesoinEau'] for data in moyennes_besoins_eau}

# Pipeline pour trouver les enregistrements au-dessus de la moyenne
pipeline_sup_moyenne = [
    {
        "$addFields": {
            "moyenneBesoinEau": {
                "$arrayElemAt": [
                    {
                        "$filter": {
                            "input": moyennes_besoins_eau,
                            "as": "item",
                            "cond": {"$eq": ["$$item._id", "$crop"]}
                        }
                    },
                    0
                ]
            }
        }
    },
    {
        "$match": {
            "$expr": {
                "$gt": ["$water req", "$moyenneBesoinEau.moyenneBesoinEau"]
            }
        }
    },
    {
        "$limit": 5  # Limiter les résultats à 5 enregistrements
    }
]

# Exécution de la requête
resultats = list(db.filtred_plants.aggregate(pipeline_sup_moyenne))

# Préparation des données pour l'affichage
table_data = []
for doc in resultats:
    table_data.append([
        doc['crop'],
        doc['water req'],
        doc['moyenneBesoinEau']['moyenneBesoinEau'],
        doc['city'],
        doc['soil']
    ])

# Définition des en-têtes de colonnes
headers = ["Crop Type", "Water Requirement", "Average Water Requirement", "City", "Soil Type"]

# Affichage du tableau
print(tabulate(table_data, headers, tablefmt="grid"))

###################################### Requête équivalente à exécuter sur le terminal MongoDB ######################################

'''
db.filtred_plants.aggregate([
  {
    $group: {
      _id: "$crop",
      moyenneBesoinEau: { $avg: "$water req" }
    }
  },
  {
    $addFields: {
      moyenneBesoinEau: {
        $arrayElemAt: [
          {
            $filter: {
              input: moyennes_besoins_eau,
              as: "item",
              cond: { $eq: ["$$item._id", "$crop"] }
            }
          },
          0
        ]
      }
    }
  },
  {
    $match: {
      $expr: {
        $gt: ["$water req", "$moyenneBesoinEau.moyenneBesoinEau"]
      }
    }
  },
  {
    $limit: 5
  }
])
'''
