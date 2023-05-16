import pandas as pd
import numpy as np
import os,sys
from sensor.logger import logging
from sensor.exception import SensorException
from sensor.config import mongo_client

def get_collection_as_dataframe(DATABASE:str, COLLECTION:str)->pd.DataFrame:
    try:
        #reading data
        logging.info(f"reading from {DATABASE} database and {COLLECTION} collection")
        df = pd.DataFrame(mongo_client[DATABASE][COLLECTION].find())

        #dropping id col
        logging.info(f"dropping {'_id'} column from dataset")
        if '_id' in df.columns:
            df.drop("_id",axis = 1,inplace = True)

        logging.info(f"shape of dataset is : {df.shape}")
        return df

    except Exception as e:
        raise SensorException(e,sys)
