import os
import pymongo
import pandas as pd
from dotenv import load_dotenv
import io
import json
from google.cloud import storage

load_dotenv()

user = os.getenv("MONGODB_USER")
password = os.getenv("MONGODB_PASSWORD")
host = os.getenv("MONGODB_HOST")
mongo_uri = f"mongodb://{user}:{password}@{host}:27017/"
database_name = os.getenv("MONGODB_DB")
client = pymongo.MongoClient(mongo_uri)
db = client[database_name]
collection_name = os.getenv("COLLECTION_NAME")
collection = db[collection_name]

project_id = os.getenv("GCS_PROJECT_ID")
storage_client = storage.Client(project_id)
bucket = storage_client.bucket("glamira_data")
blob = bucket.blob("user_behavior/user_behavior.jsonl")

print("Start data stream to GCS")

jsonl_buffer = io.StringIO()

# with blob.open("wb") as blob_writer:
all_data = collection.find({}, projection={"_id": False}, batch_size=100000)
count = 0
for data in all_data:
  count += 1
  jsonl_buffer.write((json.dumps(data) + "\n").encode("utf-8"))
  print(f"chunk number: {count}")

blob.upload_from_file(jsonl_buffer, 'application/jsonl')

print("Data streamed to GCS")

