# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 13:45:42 2018

@author: lz
"""

import datetime
import os
import platform
if 'Windows' in platform.system():
    DATA_PATH = os.path.abspath(os.path.curdir)+os.sep+'data'+os.sep
else:
    DATA_PATH = os.path.abspath(os.path.dirname(__file__))+os.sep+'data'+os.sep
import gc
import sys, getopt
from dateutil.relativedelta import relativedelta 

def getYesterday(): 
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=today-oneday  
    return yesterday.strftime("%Y%m%d")

def getkey_emo(df):
    import sensenlp as snlp 
    import processing_output as outmysql
    lablefile = DATA_PATH+'newlable.csv'
    stopkeyfile = DATA_PATH+'stopkey.txt'
    df = snlp.getPosdata(lablefile,df)   
    df_key = snlp.get_keywordsdf(df,stopkeyfile)
    del snlp
    gc.collect()
    #保存关键词
    tablename1 = 'nlp_keyword'
    outmysql.do_upinsert(df_key,tablename1)
    print('数据量：',len(df)) 
    #########################################
    #情感分析
    start_e = datetime.datetime.now()
    import baidu_aip as aip
    df = aip.io_aip(df)
    del aip
    gc.collect()
    #最后数据保存mysql
    tablename2 = 'nlp_tagge'
    outmysql.do_upinsert(df,tablename2)
    endtime = datetime.datetime.now() 
    del outmysql
    gc.collect()
    for x  in list(locals().keys()):
        del locals()[x]        
        gc.collect()
    print('情感分析完毕，耗时：{}分钟'.format(round((endtime-start_e).seconds/60,2)))

def process_everyday(*argvs):
    import sensenlp as snlp 
    try:
        argv = argvs[0]
    except:
        yesterday = getYesterday()
        start = datetime.datetime.now()
        df = snlp.loaddata(yesterday)
        end = datetime.datetime.now() 
        print('获取数据已完成，耗时：{}分钟'.format(round((end-start).seconds/60,2)))
        getkey_emo(df)
    else:
        inputperiod = ''
        t=datetime.datetime.now().strftime('%Y-%m-%d-%V').split("-")
        #time = datetime.datetime.strftime(datetime.datetime.now(),"%d %u").split(" ")
        #每月的1号和每周的周一进行数据汇总分析   
        #获取上一周周次和上一月月份
        per_w = datetime.datetime.strftime(datetime.datetime.now()- datetime.timedelta(7),"%G0%V")
        per_m =datetime.datetime.strftime(datetime.datetime.now()-relativedelta(months=1),"%G%m")
        period_range = list(range(int(t[0]+'01'),int(per_m)+1))+list(range(int(t[0]+'001'),int(per_w)+1))
        try:
            opts, args = getopt.getopt(argv,"hp:",["iperiod="])
        except getopt.GetoptError:
            print('TextPro_everyday.py -p <inputperiod>') 
        else:
            for opt, arg in opts: 
                #print('opt:{}, arg:{}'.format(opt, arg))
                if opt == '-h':
                    print('process_everyday.py -p <inputperiod> ')
                    sys.exit()
                elif opt in ("-p", "--iperiod"):
                    inputperiod = arg
                    if int(inputperiod) in period_range: 
                        print('待处理数据周期为：', inputperiod)
                        df = snlp.loaddata(inputperiod)
                        getkey_emo(df)
                    elif len(str(inputperiod))==8:
                        print('待处理数据周期为：', inputperiod)
                        df = snlp.loaddata(inputperiod)
                        print(df.columns)
                        getkey_emo(df)
    del snlp
    gc.collect()
    for x  in list(locals().keys()):
        del locals()[x]        
        gc.collect()

if __name__ == '__main__':
    argv = sys.argv[1:]
    print(argv)
    if argv:
       process_everyday(argv)
    else:
        #process_everyday()
        from apscheduler.schedulers.blocking import BlockingScheduler
        sched = BlockingScheduler()
        #任务在每个天的的01:00执行该程序
        sched.add_job(process_everyday, 'cron', day_of_week= '0-6', hour='11',minute ='50,59')
        sched.start()#开始
