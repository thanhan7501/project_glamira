import os
import IP2Location
import pymongo
import pandas as pd 
from dotenv import load_dotenv
from gcp import upload_blob_from_stream
import io
import json

load_dotenv()

"""
    Cache the database into memory to accelerate lookup speed.
    WARNING: Please make sure your system have sufficient RAM to use this feature.
"""

# Default file I/O lookup
database_ipv4 = IP2Location.IP2Location(os.path.join("data", "IP2LOCATION-LITE-DB11.BIN"))
database_ipv6 = IP2Location.IP2Location(os.path.join("data", "IP2LOCATION-LITE-DB11.IPV6.BIN"))
ipTools = IP2Location.IP2LocationIPTools()

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
# distinct_ip = collection.distinct("ip")
jsonl_buffer = io.StringIO()
distinct_ips = collection.aggregate([{"$group": {"_id": "$ip"}}])
for ip in distinct_ips:
    if ipTools.is_ipv4(ip["_id"]) is True:
        country = database_ipv4.get_all(ip["_id"])
        jsonl_buffer.write(json.dumps(country.__dict__) + '\n')
    elif ipTools.is_ipv6(ip["_id"]) is True:
        country = database_ipv6.get_all(ip["_id"])
        jsonl_buffer.write(json.dumps(country.__dict__) + '\n')
        
jsonl_buffer.seek(0)

upload_blob_from_stream("glamira_data", jsonl_buffer, "location/location.jsonl", project_id)
