import logging
import os,sys
from datetime import datetime

#create log directory
log_dir = os.path.join(os.getcwd(),"logs")
os.makedirs(log_dir,exists_ok = True)

#create log file
log_file_name = f"{datetime.now().strftime('%m%d%Y__%H%M%S')}.log"

#path of log file
log_file_path = os.path.join(log_dir,log_file_name)

#log basic config
logging.basicConfig(
    filename = log_file_path,
    format = "[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level = logging.INFO
)
