from pymongo import MongoClient

host="rhea.isegi.unl.pt"
port="27017"
user="mongo_classes_rhea"
password="bW9uZ29fY2xhc3Nlc19yaGVh"
protocol="mongodb"

client = MongoClient(f"{protocol}://{user}:{password}@{host}:{port}")
db = client.contracts
eu = db.eu