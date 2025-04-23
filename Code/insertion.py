from pymongo import MongoClient
import json

# Connexion à MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['mydatabase']

# Lecture des fichiers JSON
with open('data/arachides_riz_generated.json', 'r') as file:
    arachides_riz = json.load(file)

with open('data/filtred_plants.json', 'r') as file:
    filtred_plants = json.load(file)

# Insertion des données dans les collections MongoDB
db.arachides_riz.insert_many(arachides_riz)
db.filtred_plants.insert_many(filtred_plants)

print("Données insérées avec succès.")
