import logging
import os
import sys
from typing import List
from datetime import datetime
from pathlib import Path

FILE_NAME = datetime.now().strftime("%Y_%m_%d_%H")+".log"

FILE_DIR = os.path.join(os.getcwd(), "logs",
                        datetime.now().strftime("%Y_%m_%d"))

FILE_PATH = os.path.join(FILE_DIR, FILE_NAME)

os.makedirs(FILE_DIR, exist_ok=True)

logging.basicConfig(filename=FILE_PATH,
                    filemode='a', format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p')


if __name__ == "__main__":
    logging.info('Logging Successful')
