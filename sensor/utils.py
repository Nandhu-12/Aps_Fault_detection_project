import pandas as pd
import numpy as np
import os,sys
from sensor.logger import logging
from sensor.exception import SensorException
from sensor.config import mongo_client
import yaml
import dill

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

def write_yaml_file(data : dict, file_path):
    try:
        logging.info(f"creating file dir")
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok = True)

        with open(file_path,'w') as file_writer:
            logging.info(f"dumping message in file dir")
            yaml.dump(data,file_writer)
    except Exception as e:
        raise SensorException(e,sys)


def convert_col_float(df : pd.DataFrame, exclude_columns : list)->pd.DataFrame:
    try:
        for col in df.columns:
            if col not in exclude_columns:
                logging.info(f"converting dtype of all columns to float except target column")
                df[col] = df[col].astype('float')
        return df
    except Exception as e:
        raise SensorException(e,sys)

#like pickle -- serialization
def save_object(file_path : str, obj : object)-> None:
    try:
        logging.info("Entered the save_object method of utils")

        os.makedirs(os.path.dirname(file_path),exist_ok = True)
        with open (file_path,"wb") as file_obj:
            dill.dump(obj,file_obj)
        logging.info("Exited the save_object method of utils")

    except Exception as e:
        raise SensorException(e,sys)
    
    
def load_object(file_path : str)-> object:
    try:
        if not os.path.exists(file_path):
            raise Exception(f"The file: {file_path} is not exists")

        with open(file_path,"rb") as file_obj:
            return dill.load(file_obj)

    except Exception as e:
        raise SensorException(e,sys)


def save_numpy_array_data(file_path : str, array : np.array):
    """
    Save numpy array data to file
    file_path: str location of file to save
    array: np.array data to save
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path,exist_ok = True)
        with open (file_path,"wb") as file_obj:
            np.save(file_obj,array)

    except Exception as e:
        raise SensorException(e,sys)


def load_numpy_array_data(file_path : str) -> np.array:
    """
    load numpy array data from file
    file_path: str location of file to load
    return: np.array data loaded
    """
    try:
        with open(file_path,"rb") as file_obj:
            return np.load(file_obj)

    except Exception as e:
        raise SensorException(e,sys)