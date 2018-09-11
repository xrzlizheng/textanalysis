# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 16:25:26 2018

@author: lz
"""
import pymysql
import pandas as pd

def read_db(condition):  
    #连接数据库
    #conn = pymysql.connect(host='*******',port= ***,user = '***',passwd='******',db='tbl*****',charset='utf8') #db：库名
    ****}

    }
    dbspar = dbspars['scdb']
    db  = pymysql.connect(**dbspar)
    loop = True
    num = 300
    chunks = []
    condition2 = ""
    while loop:
        try:
            with db.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                sql="SELECT `id`,`fnum`,`user_cabin`,DATE_FORMAT(`fdate`,'%Y%m%d') as `fdate`,`evaluated_text`, `fdep_code`,`farr_code`,FROM_UNIXTIME(`fdplan`) as `fdplan`,FROM_UNIXTIME(`faplan`) as `faplan`,if(`fdactual`=0,0,FROM_UNIXTIME(`fdactual`)) as `fdactual`,if(`faactual`=0,0,FROM_UNIXTIME(`faactual`)) as `faactual`  FROM `***` where {} {} and `evaluated_text_length` > 0 limit {} ".format(condition,condition2,num)
                print(sql)
                cursor.execute(sql)
                result=cursor.fetchall()
            cursor.close()
        except:
            db.rollback()
            print('数据库链接失败')
        else:
            chunk=pd.DataFrame(list(result))
            chunks.append(chunk)
            print(len(chunks))
            id_list = list(chunk['id'].astype(int))
            maxid = max(id_list)
            condition2 = "and `id` > {}".format(maxid)
            if len(chunk)< num:
                loop = False   
    db.close()
    df = pd.concat(chunks)
    df = df.loc[:,['id','fnum', 'user_cabin', 'fdate','evaluated_text', 'fdep_code','farr_code'
                      ,'fdplan','faplan','fdactual','faactual']]
    df.columns = ['*******']
    return df 



def read_nlptagge(condition):  
    #连接数据库
    #conn = pymysql.connect(host='*******',port= ***,user = '***',passwd='******',db='tbl*****',charset='utf8') #db：库名
    dbspars = {'lzdb':{'host':'127.0.0.1','port' :3306,'db':'lz_fy','user':'root','password':'123456','charset':'utf8mb4'}
    ***'}
    }
    dbspar = dbspars['scdb']
    db  = pymysql.connect(**dbspar)
    loop = True
    num = 300
    chunks = []
    condition2 = ""
    while loop:
        try:
            with db.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                sql="SE***  where {} {} limit {}".format(condition,condition2,num)
                print(sql)
                cursor.execute(sql)
                result=cursor.fetchall()
            cursor.close()
        except:
            db.rollback()
            print('数据库链接失败')
        else:
            chunk=pd.DataFrame(list(result))
            chunks.append(chunk)
            print(len(chunks))
            id_list = list(chunk['id'].astype(int))
            maxid = max(id_list)
            condition2 = "and `id` > {}".format(maxid)
            if len(chunk)< num:
                loop = False   
    db.close()
    df = pd.concat(chunks)
    df = df.loc[:,['id','fnum', 'user_cabin', 'fdate','evaluated_text', 'fdep_code','farr_code'
                      ,'fdplan','faplan','fdactual','faactual']]
    df.columns = ['点评ID','*','*','航班*','评论','出*码' ,'目*','计划出发时间','计划到*','实际*','实际*']
    return df 
