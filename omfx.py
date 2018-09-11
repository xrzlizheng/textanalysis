# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 15:47:38 2018

@author: lz
"""
import numpy as np
from multiprocessing import Pool
import sys
sys.path.append("D:/lz/textanalysis/textnlp/sensenlp2.0")
import os
DATA_PATH = os.path.abspath(os.path.curdir)+os.sep+'data'+os.sep
import pandas as pd
import sensenlp as snlp
if __name__ == '__main__':
    lablefile = DATA_PATH+'newlable.csv'
    labledf = pd.read_csv(lablefile)
    df = pd.read_csv(DATA_PATH+'omdata.csv')
    df['点评ID'] = np.arange(len(df))
    from functools import partial
    partial_work = partial(snlp.posmini,labledf = labledf)
    pool = Pool(2)
    dfminni =  pool.map(partial_work,df.iterrows())
    pool.close()
    pool.join()
    
    df_new = pd.concat(dfminni)
    df_new['lable']=df_new['lable'].apply(lambda x:np.NaN if str(x).strip()=="" or str(x).strip() =="~" else x)
    df_new= df_new[df_new['lable'].notnull()]
    df_new = snlp.getonelable(df_new)
    df_new.to_csv(DATA_PATH+'mtdata_lable.csv')