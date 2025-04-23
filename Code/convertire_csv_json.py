import pandas as pd


# Chargement des fichiers CSV
arachides_riz_df = pd.read_csv('data/arachides_riz_generated.csv')
filtred_plants_df = pd.read_csv('data/filtred_plants.csv')

# Conversion en JSON
arachides_riz_json = arachides_riz_df.to_json(orient='records')
filtred_plants_json = filtred_plants_df.to_json(orient='records')

# Sauvegarde des fichiers JSON
with open('data/arachides_riz_generated.json', 'w') as file:
    file.write(arachides_riz_json)

with open('data/filtred_plants.json', 'w') as file:
    file.write(filtred_plants_json)
