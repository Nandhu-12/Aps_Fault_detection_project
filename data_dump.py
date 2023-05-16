import pymongo
import pandas as pd
import json

# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

# creating variables for db, collection and for df path
DATABASE = "Aps"
COLLECTION = "Sensor"
DATA_FILE_PATH = "/config/workspace/aps_failure_training_set1.csv"

if __name__ == "__main__":
    df = pd.read_csv(DATA_FILE_PATH)
    print(f"shape of dataset: {df.shape}")

    # dropping index and converting to json format
    df.reset_index(drop = True, inplace = True)
    records = list(json.loads(df.T.to_json()).values())
    print(records[0])

    # inserting the json converted records in db
    client[DATABASE][COLLECTION].insert_many(records)