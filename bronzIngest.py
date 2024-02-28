import sys
import numpy as np
import pandas as pd
import os
from utils.logger import logging
from utils.exceptor import Custom_Error

import warnings
warnings.filterwarnings("ignore")


fiscYrs = [2122, 2223, 2324]
fileList = ["realizedPnL_"+str(x)+"_BS8062.xlsx" for x in fiscYrs]
pathList = [os.path.join(os.getcwd(), "portfolio_lookup",
                         "datasets", x) for x in fileList]


def readBronz(pathList):
    writePath = os.path.join(
        os.getcwd(), "portfolio_lookup", "artifacts", "Bronze")
    os.makedirs(writePath, exist_ok=True)
    for _ in pathList:
        newdf_ = pd.read_excel(_, header=None)
        starts_at = newdf_[newdf_[0] == "Scrip Name "].index[0]
        newdf_ = newdf_.set_axis(
            newdf_.iloc[starts_at].to_list(), axis='columns')
        newdf_.drop(index=range(starts_at+1), axis=0, inplace=True)
        newdf_ = newdf_.reset_index(drop=True)
        name_pq = str(os.path.splitext(
            os.path.split(_)[1])[0][8:16])+".parquet"
        name_csv = str(os.path.splitext(os.path.split(_)[1])[0][8:16])+".csv"
        newdf_.to_csv(os.path.join(writePath, name_csv))
        newdf_.drop(columns=['ISIN', 'Long Term', 'Speculation'], inplace=True)
        newdf_.to_parquet(os.path.join(writePath, name_pq), engine='pyarrow')


if __name__ == '__main__':
    try:
        readBronz(pathList)
    except Exception as e:
        raise Custom_Error(e, sys)
    logging.info('Bronze Files written successfully💉💉💉💉💉')
