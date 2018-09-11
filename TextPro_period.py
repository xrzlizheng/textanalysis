# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 16:58:40 2018

@author: lz
"""
import pymysql
import datetime
import os
import platform
if 'Windows' in platform.system():
    DATA_PATH = os.path.abspath(os.path.curdir)+os.sep+'data'+os.sep
else:
    DATA_PATH = os.path.abspath(os.path.dirname(__file__))+os.sep+'data'+os.sep
import processing_output as outmysql
from dateutil.relativedelta import relativedelta
import sys, getopt
import pandas as pd
import gc

def read_nlptagge(condition):  
    #连接数据库
    #conn = pymysql.connect(host='*******',port= ***,user = '***',passwd='******',db='tbl*****',charset='utf8') #db：库名
    dbspars = {'lzdb':{'host':'127.0.0.1','port' :3306,'db':'lz_fy','user':'root','password':'123456','charset':'utf8mb4'}
    ,'scdb':{'host':'10.77.25.27','port' :8066,'db':'fysurvey_db2','user':'zhuam_fysurvey2','password':'diramdir','charset':'utf8mb4'}
    }
    try:
        dbspar = dbspars['scdb']
        db  = pymysql.connect(**dbspar)
    except Exception as e:
        print(e)
        dbspar = dbspars['lzdb']
        db  = pymysql.connect(**dbspar)
    loop = True
    num = 300
    chunks = []
    condition2 = ""
    while loop:
        try:
            with db.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                sql="SELECT `id`,`hed`,`chilid`,`sub_clause`,`lable`,`fnum`,`irr_f`,`fspace`,`dep_code`,`arr_code`,`fdate`,`period_w`,`period_m`,`airline`,`b1`,`b2`,`b3`,`b4`,`b5`,`b6`,`b7`,`b8`,if(`irr_f`=1,0,`b9`) as `b9`,`j1`,`j2`,`j3`,`j4`,`j6`,`jgp`,`ado`,`emo` FROM nlp_tagge  where {} {} limit {}".format(condition,condition2,num)
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
    df = df.loc[:,['id','chilid','sub_clause','hed','lable','fnum','irr_f','fspace','dep_code','arr_code','fdate','period_w','period_m','airline','b1','b2','b3','b4','b5','b6','b7','b8','b9','j1','j2','j3','j4','j6','jgp','ado','emo']]
    df.columns = ['id','chilid','view','hed','lable','fnum','irr_f','fspace','dep_code','arr_code','fdate','period_w','period_m','airline', 'B1', 'B2', 'B3', 'B4','B5', 'B6', 'B7', 'B8','B9', 'J1', 'J2', 'J3', 'J4','J6','JGP','ADO','emo']
    return df 


def process_period(*argvs):
    import sensenlp as snlp
    t=datetime.datetime.now().strftime('%Y-%m-%d-%V').split("-")
    time = datetime.datetime.strftime(datetime.datetime.now(),"%d %u").split(" ")
    #每月的1号和每周的周一进行数据汇总分析   
    #获取上一周周次和上一月月份
    per_w = datetime.datetime.strftime(datetime.datetime.now()- datetime.timedelta(7),"%G0%V")
    per_m =datetime.datetime.strftime(datetime.datetime.now()-relativedelta(months=1),"%G%m")
    period_range = list(range(int(t[0]+'01'),int(per_m)+1))+list(range(int(t[0]+'001'),int(per_w)+1))
    stopkeyfile = DATA_PATH+'stopkey.txt'
    try:
        argv = argvs[0]
    except:
        if int(time[0])==1 and int(time[1])!=1:
            condition = '`period_m` = {}'.format(per_m)
            df = read_nlptagge(condition)
            df_key = snlp.get_keywordsdf(df,stopkeyfile)
            outmysql.get_json(per_m,df,df_key)
            
        elif int(time[1])==1 and int(time[0])!=1:
            condition = '`period_w` = {}'.format(per_w)
            df = read_nlptagge(condition)
            df_key = snlp.get_keywordsdf(df,stopkeyfile)
            outmysql.get_json(per_w,df,df_key)
        elif int(time[1])==1 and int(time[0])==1:
            condition1 = '`period_m` = {}'.format(per_m)
            df1 = read_nlptagge(condition1)
            condition2 = '`period_w` = {}'.format(per_w)
            df2 = read_nlptagge(condition2)
            df_key1 = snlp.get_keywordsdf(df1,stopkeyfile)
            outmysql.get_json(per_m,df1,df_key1)
            df_key2 = snlp.get_keywordsdf(df1,stopkeyfile)
            outmysql.get_json(per_w,df2,df_key2)
    else:
        inputperiod = ''
        try:
            opts, args = getopt.getopt(argv,"hp:",["iperiod="])
        except getopt.GetoptError:
            print('main.py -p <inputperiod> -f <inputfile>') 
        else:
            for opt, arg in opts: 
                #print('opt:{}, arg:{}'.format(opt, arg))
                if opt == '-h':
                    print('TextPro_period.py -p <inputperiod> ')
                    sys.exit()
                elif opt in ("-p", "--iperiod"):
                    inputperiod = arg
                    if int(inputperiod) in period_range: 
                        print('待处理数据周期为：', inputperiod)
                        if len(str(inputperiod)) == 7:
                            condition  = '`period_w` = {}'.format(inputperiod)
                        elif len(str(inputperiod)) == 6:
                            condition  = '`period_m` = {}'.format(inputperiod)
                        df = read_nlptagge(condition)
                        df_key = snlp.get_keywordsdf(df,stopkeyfile)
                        outmysql.get_json(inputperiod,df,df_key)
                        #print(len(df))
                    else:
                        print('您输入的周期范围不正确')
                        sys.exit()
    for x  in list(locals().keys()):
        del locals()[x]        
        gc.collect()
                        
if __name__ == '__main__':
    argv = sys.argv[1:]
    if argv:
        process_period(argv)
    else:
        print('main.py -p <inputperiod> -f <inputfile>')
        #main()
        from apscheduler.schedulers.blocking import BlockingScheduler
        sched = BlockingScheduler()
        #在1-12月份的第第一天的00:01执行该程序
        sched.add_job(process_period, 'cron', month='1-12', day= 1, hour= '17')
        #任务在每个周一的的01:00执行该程序
        sched.add_job(process_period, 'cron', day_of_week= 0, hour='4')
        sched.start()