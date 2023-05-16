import os,sys
from sensor.logger import logging
from sensor.exception import SensorException
from datetime import datetime

FILE_NAME = 'sensor.csv'
TRAIN_FILE_NAME = 'train.csv'
TEST_FILE_NAME = 'test.csv'

class TrainingPipelineConfig:
    def __init__(self):
        try:
            #creating artifact folder with timestamp
            logging.info("creating artifact folder with timestamp")
            self.artifact_dir = os.path.join(os.getcwd(),"artifact",f"{datetime.now().strftime('%m%d%Y__%H%M%S')}")

        except Exception as e:
            raise SensorException(e,sys)

class DataIngestionConfig:
    def __init__(self,training_pipeline_config:TrainingPipelineConfig):
        try:
            #defining inputs
            logging.info("defining inputs for data_ingestion")
            self.DATABASE = "Aps"
            self.COLLECTION = "Sensor"

            '''
            creating data_ingestion directory under artifact
            creating feature store inside data_ingestion dir
            creating a dataset folder inside data_ingestion dir and to store the splitted train and test data'
            '''
            logging.info("creating directory and files for artifact")
            self.data_ingestion_dir = os.path.join(training_pipeline_config.artifact_dir, "data_ingestion")
            self.feature_store_filepath = os.path.join(self.data_ingestion_dir, "feature store", FILE_NAME)
            self.train_filepath = os.path.join(self.data_ingestion_dir, "dataset", TRAIN_FILE_NAME)
            self.test_filepath = os.path.join(self.data_ingestion_dir, "dataset", TEST_FILE_NAME)
            self.test_size = 0.2

        except Exception as e:
            raise SensorException(e,sys)

    def to_dict(self):
        try:
            return self.__dict__

        except Exception as e:
            raise SensorException(e,sys)


class DataValidationConfig:...
class DataTransformationConfig:...
class ModelTrainerConfig:...
class ModelEvaluationConfig:...
class ModelPusherConfig:...