import sys
import numpy as np
import pandas as pd
import os
from utils.logger import logging
from utils.exceptor import Custom_Error
from bronzIngest import readBronz, pathList

import warnings
warnings.filterwarnings("ignore")

artifact_read_path = os.path.join(
    os.getcwd(), "portfolio_lookup", "artifacts", "Bronze")
artifact_write_path = os.path.join(
    os.getcwd(), "portfolio_lookup", "artifacts", "Silver")

os.makedirs(artifact_read_path, exist_ok=True)
os.makedirs(artifact_write_path, exist_ok=True)


def writeSilver(read_path=artifact_read_path):
    files = [x for x in os.listdir(read_path) if x.endswith(".parquet")]
    df_list = []
    for _ in files:
        df_list.append(pd.read_parquet(os.path.join(read_path, _)))
    pooled_df = pd.concat(df_list).reset_index(drop=True)
    df_tabl = pooled_df[['Scrip Name ', 'Total PL', 'Sell Date']]
    df_tabl.to_csv(os.path.join(artifact_write_path,
                   "pooledDf.csv"), index=False)
    logging.info('Tableau files written 📉📊📈📉📊📈📉📊📈📉📊📈')


def writeCleansedFiles(read_path=artifact_read_path):
    files = [x for x in os.listdir(read_path) if x.endswith(".parquet")]
    for _ in files:
        df_ = pd.read_parquet(os.path.join(read_path, _))
        df_.drop(columns=['Scrip Code', 'Symbol', 'Buy Rate', 'Sell Rate',
                          'Sell Amt', 'Days', 'Short Term', 'Turn Over', 'Strike Price'], inplace=True)
        df_.to_csv(os.path.join(artifact_write_path,
                   _[:-8]+"Silver.csv"), index=False)
        df_.to_parquet(os.path.join(artifact_write_path, _[
                       :-8]+"Silver.parquet"), engine='pyarrow')

    logging.info('seperate Silver files Written Successfully📝📝📝📝📝📝📝')
    logging.info('calling tablue file function🤝🤝🤝🤝🤝🤝🤝')

    writeSilver()


if __name__ == "__main__":
    try:
        readBronz(pathList)
        logging.info('bronz files re-iterated 💔❤️‍🩹💉💔❤️‍🩹💉💔❤️‍🩹💉💔❤️‍🩹💉💔❤️‍🩹💉')
        writeCleansedFiles()
        logging.info('Silver files Writing completed Successfully👍🎯👍🎯👍🎯👍🎯👍🎯')
    except Exception as e:
        raise Custom_Error(e, sys)
