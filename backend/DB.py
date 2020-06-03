from pymongo import MongoClient

host="rhea.isegi.unl.pt"
port="28011"
user="GROUP_11"
password="MTM0MzQ5MzI3NjY4ODM1MzQ2ODc2OTMzMjgyNjQ3NjYyNDcyNjUw"
protocol="mongodb"

client = MongoClient(f"{protocol}://{user}:{password}@{host}:{port}")
db = client.contracts
eu = db.eu