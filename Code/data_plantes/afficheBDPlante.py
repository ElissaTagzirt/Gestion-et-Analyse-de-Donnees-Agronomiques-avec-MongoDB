import pymongo
from tabulate import tabulate

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.mydatabase

# Accéder à la collection filtred_plants
filtred_plants_collection = db.filtred_plants

# Exemple de requête pour récupérer toutes les entrées
filtred_plants_data = list(filtred_plants_collection.find())

# Préparer les données pour l'affichage
filtred_plants_table = []
for item in filtred_plants_data:
    filtred_plants_table.append([
        item.get('_id'),
        item.get('water req'),
        item.get('month'),
        item.get('Min Temp'),
        item.get('Max Temp'),
        item.get('Humidity'),
        item.get('Wind'),
        item.get('Sun'),
        item.get('Rad'),
        item.get('Rain'),
        item.get('altitude'),
        item.get('latitude'),
        item.get('longitude'),
        item.get('crop'),
        item.get('soil'),
        item.get('city')
    ])

# Définir les en-têtes de colonnes
filtred_plants_headers = ["ID", "Water Req", "Month", "Min Temp", "Max Temp", "Humidity", "Wind", "Sun", "Rad", "Rain", "Altitude", "Latitude", "Longitude", "Crop", "Soil", "City"]

# Afficher les résultats sous forme de tableau
print("Filtred Plants Data:")
print(tabulate(filtred_plants_table, headers=filtred_plants_headers, tablefmt="grid"))
