from pymongo import MongoClient
import os

cliente = MongoClient(os.getenv('MONGO_URI'), serverSelectionTimeoutMS=5000)
info = cliente.server_info()
print('[OK] Airflow puede conectar a MongoDB - version:', info['version'])
cliente.close()