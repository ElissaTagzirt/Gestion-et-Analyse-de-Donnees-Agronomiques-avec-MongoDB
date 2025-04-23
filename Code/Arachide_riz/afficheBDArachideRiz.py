import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.mydatabase

# Accéder à la collection arachides_riz
arachides_riz_collection = db.arachides_riz

# Exemple de requête pour récupérer toutes les entrées
arachides_riz_data = list(arachides_riz_collection.find())

# Préparer les données pour l'affichage
arachides_riz_table = []
for item in arachides_riz_data:
    arachides_riz_table.append([
        item.get('_id'),
        item.get('CropDays'),
        item.get('Soil Moisture'),
        item.get('Soil Temperature'),
        item.get('Temperature'),
        item.get('Humidity'),
        item.get('CropType'),
        item.get('Irrigation(Y/N)')
    ])

# Définir les en-têtes de colonnes
arachides_riz_headers = ["ID", "CropDays", "Soil Moisture", "Soil Temperature", "Temperature", "Humidity", "CropType", "Irrigation(Y/N)"]

# Afficher les résultats sous forme de tableau
print("Arachides Riz Data:")
print(tabulate(arachides_riz_table, headers=arachides_riz_headers, tablefmt="grid"))
