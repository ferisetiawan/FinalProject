# Final Project of Data Engineering Class
# Spring Semester 2017
#
# ---- Developed by ----
# Aria Ghora Prabono - 201631011
# Feri Setiawan - 201630189
# Sunder Ali Khowaja - 201632039
#
# This code is used to upload the dataset into MongoDB.

from pymongo import MongoClient
import pandas as pd

data = pd.read_csv('transformed_completeData.csv')
data.columns.values[0] = 'row'
data = data.to_dict(orient='records')

# it connect with our server MongoDB server through port number 27017
mongo_client = MongoClient('220.67.127.67', 27017)
db = mongo_client.final_project
collection = db.socioemotional_data

collection.insert_many(data)