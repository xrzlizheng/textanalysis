#!/home/feeyo_s_lz/anaconda3/bin/python
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 15:36:02 2018

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
import processing_output as outmysql
from dateutil.relativedelta import relativedelta
import sys, getopt
    



def process(df,Period):
    import sensenlp as snlp
    start_p = datetime.datetime.now()
    stopkeyfile = DATA_PATH+'stopkey.txt'
    df_key = snlp.get_keywordsdf(df,stopkeyfile)
    print('成功获取关键词{}'.format(df_key[:10]))
    #########################################
    #保存关键词
    tablename1 = 'nlp_keyword'
    outmysql.do_upinsert(df_key,tablename1)
    endtime_p = datetime.datetime.now() 
    print('分类完毕，耗时：{}分钟'.format(round((endtime_p-start_p).seconds/60,2))) 
    print('数据量：',len(df)) 
    del snlp
    gc.collect()
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
    print('情感分析完毕，耗时：{}分钟'.format(round((endtime-start_e).seconds/60,2)))
    Period.sort()
    for i in Period:
        print('汇总第{}数据'.format(str(i)))
        starttime = datetime.datetime.now()
        outmysql.get_json(i,df,df_key)
        endtime = datetime.datetime.now()
        print('第{}数据完成，耗时：{}秒'.format(str(i),(endtime-starttime).seconds))
    print ('此时程序共耗时：{}分钟'.format(round((endtime-start_p).seconds/60,2)))

def main(*argvs):
    import sensenlp as snlp
    start = datetime.datetime.now()
    lablefile = DATA_PATH+'newlable.csv'
    t=datetime.datetime.now().strftime('%Y-%m-%d-%V').split("-")
    time = datetime.datetime.strftime(datetime.datetime.now(),"%d %u").split(" ")
    #每月的1号和每周的周一进行数据汇总分析   
    #获取上一周周次和上一月月份
    per_w = datetime.datetime.strftime(datetime.datetime.now()- datetime.timedelta(7),"%G0%V")
    per_m =datetime.datetime.strftime(datetime.datetime.now()-relativedelta(months=1),"%G%m")
    period_range = list(range(int(t[0]+'01'),int(per_m)+1))+list(range(int(t[0]+'001'),int(per_w)+1))
    try:
        argv = argvs[0]
    except:
        print('处理上周/月数据')
        try:
            df = snlp.loaddata()
        except:
            sys.exit()
        Period  = []
        if int(time[0])==1 and int(time[1])!=1:
            Period.append(per_m)
        elif int(time[1])==1 and int(time[0])!=1:
            Period.append(per_w)
        elif int(time[1])==1 and int(time[0])==1:
            Period.append(per_w)
            Period.append(per_m)
    else:
        inputperiod = ''
        inputfile = ''
        try:
            opts, args = getopt.getopt(argv,"hp:f:",["iperiod=","ifile="])
        except getopt.GetoptError:
            print('main.py -p <inputperiod> -f <inputfile>') 
        else:
            for opt, arg in opts: 
                #print('opt:{}, arg:{}'.format(opt, arg))
                if opt == '-h':
                    print('main.py -p <inputperiod> -f <inputfile>')
                    sys.exit()
                elif opt in ("-p", "--iperiod"):
                    inputperiod = arg
                    if int(inputperiod) in period_range: 
                        print('待处理数据周期为：', inputperiod)
                        df = snlp.loaddata(inputperiod)
                        Period = [inputperiod]
                        #print(len(df))
                    else:
                        print('您输入的周期范围不正确')
                        sys.exit()
                elif opt in ("-f", "--ifile"):
                    for f, _, i in os.walk(DATA_PATH):
                        if arg in i:
                            inputfile = DATA_PATH+arg
                            print('待处理数据路径为：', inputfile)
                            df = snlp.loaddata(inputfile)
                            Mon = df['月份'].drop_duplicates().tolist()
                            Week = df['周次'].drop_duplicates().tolist()
                            Period = Mon+Week
                            break
                        else:
                            print('您输入的数据文件不存在，请重新输入')
                            sys.exit()
    
                #df = df.loc[:100]
                df = snlp.getPosdata(lablefile,df)
                process(df,Period)
                endtime = datetime.datetime.now()
                print('程序运行完毕，累计耗时：{}分钟'.format(round((endtime-start).seconds/60,2)))
                   

if __name__ == '__main__':
    argv = sys.argv[1:]
    print(argv)
    if argv:
        main(argv)
    else:
        print('main.py -p <inputperiod> -f <inputfile>')
        #main()
        from apscheduler.schedulers.blocking import BlockingScheduler
        sched = BlockingScheduler()
        #在1-12月份的第第一天的00:01执行该程序
        sched.add_job(main, 'cron', month='1-12', day= 1, hour= 0)
        #任务在每个周一的的01:00执行该程序
        sched.add_job(main, 'cron', day_of_week= 0, hour=1)
        sched.start()
