#!/home/feeyo_s_lz/anaconda3/bin/python
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 11:48:09 2018

@author: lz
运用jieba分词，哈工大ltp进行语法分析，百度ai  情感分析
"""

import pandas as pd
import numpy as np
import datetime
import os
import platform
if 'Windows' in platform.system():
    DATA_PATH = os.path.abspath(os.path.curdir)+os.sep+'data'+os.sep
else:
    DATA_PATH = os.path.abspath(os.path.dirname(__file__))+os.sep+'data'+os.sep
import participle as p
import baidu_aip as aip
import gc
import time
import re
import pymysql
from multiprocessing import Pool
import co_occurrence as co
from dateutil.relativedelta import relativedelta
import calendar

def getfirs_endtday(flag):
    """
    输入每周的第一天
    """
    flag = str(flag)
    if len(flag) ==7:
        weekflag = flag
        yearnum = weekflag[0:4]   #取到年份
        weeknum = weekflag[5:7]   #取到周
        stryearstart = yearnum +'0101'   #当年第一天
        yearstart = datetime.datetime.strptime(stryearstart,'%Y%m%d') #格式化为日期格式
        yearstartcalendarmsg = yearstart.isocalendar()  #当年第一天的周信息
        #yearstartweek = yearstartcalendarmsg[1]  
        yearstartweekday = yearstartcalendarmsg[2]
        yearstartyear = yearstartcalendarmsg[0]
        if yearstartyear < int (yearnum):
            daydelat = (8-int(yearstartweekday))+(int(weeknum)-1)*7
        else :
            daydelat = (8-int(yearstartweekday))+(int(weeknum)-2)*7
        start = (yearstart+datetime.timedelta(days=daydelat)).date()
        end = start+datetime.timedelta(days=6)
    elif len(flag)==6:
        monthflag =flag
        yearnum = monthflag[0:4]   #取到年份
        monthnum = monthflag[4:6]   #取到月
        strmonthstart = yearnum +monthnum+'01'   #当年第一天
        start = datetime.datetime.strptime(strmonthstart,'%Y%m%d')
        monthRange = calendar.monthrange(int(yearnum),int(monthnum))
        monthdays = monthRange[1]
        end = start+datetime.timedelta(days=monthdays-1)
        #print(monthRange)

    return start.strftime('%Y%m%d'),end.strftime('%Y%m%d')


def read_db(condition):  
    #连接数据库
    #conn = pymysql.connect(host='*******',port= ***,user = '***',passwd='******',db='tbl*****',charset='utf8') #db：库名
    dbspars = {'lzdb':{'host':'127.0.0.1','port' :3306,'db':'lz_fy','user':'root','password':'123456','charset':'utf8mb4'}
    ,'scdb':{'host':'10.77.25.27','port' :8066,'db':'fysurvey_db2','user':'zhuam_fysurvey2','password':'diramdir','charset':'utf8mb4'}
    ,'test':{'host':'192.168.2.182','port' :8066,'db':'fysurvey2','user':'tom_yangy','password':'6A71bBba188913679d89284b15deCf24','charset':'utf8mb4'}
    ,'capese':{'host':'192.168.15.154','port' :3306,'db':'capse_survey','user':'root','password':'123456','charset':'utf8mb4'}

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
                sql="SELECT `id`,`fnum`,`user_cabin`,DATE_FORMAT(`fdate`,'%Y%m%d') as `fdate`,`evaluated_text`, `fdep_code`,`farr_code`,FROM_UNIXTIME(`fdplan`) as `fdplan`,FROM_UNIXTIME(`faplan`) as `faplan`,if(`fdactual`=0,0,FROM_UNIXTIME(`fdactual`)) as `fdactual`,if(`faactual`=0,0,FROM_UNIXTIME(`faactual`)) as `faactual`  FROM `capse_flight_evaluation` where {} {} and `evaluated_text_length` > 0 limit {} ".format(condition,condition2,num)
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
            #print(len(chunks))
            if len(chunk) > 0:
                id_list = list(chunk['id'].astype(int))
                maxid = max(id_list)
                condition2 = "and `id` > {}".format(maxid)
            if len(chunk)< num:
                loop = False   
    db.close()
    df = pd.concat(chunks)
    if df.empty==False:
        df = df.loc[:,['id','fnum', 'user_cabin', 'fdate','evaluated_text', 'fdep_code','farr_code','fdplan','faplan','fdactual','faactual']]
        df.columns = ['点评ID','航班号','仓位','航班日期','评论','出发地编码' ,'目的地编码','计划出发时间','计划到达时间','实际出发时间','实际到达时间']
        return df 

def loaddata(*arg):
    """
    获取数据并清洗，
    每月第一天和每周第一天处理前一个周期的数据
    周采用的均为基于周的年+周次(周一为第一天)
    """       
    time = datetime.datetime.strftime(datetime.datetime.now(),"%d %u").split(" ")
    t=datetime.datetime.now().strftime('%Y-%m-%d-%V').split("-")
    x = min(calendar.monthrange(int(t[0]),int(t[1]))[1]-int(time[0]),7-int(time[1])) 
    try:
        arg = arg[0]
    except:
        #获取上一周周次和上一月月份
        period_w = datetime.datetime.strftime(datetime.datetime.now()- datetime.timedelta(7),"%G0%V")
        period_m =datetime.datetime.strftime(datetime.datetime.now()-relativedelta(months=1),"%G%m")
        if int(time[0])==1 and int(time[1])!=1:
            #每月第一天处理上月月数据
            startday,endday = getfirs_endtday(period_m)
            condition = "`fdate` between {} and {}".format(startday,endday)
        elif int(time[1])==1 and int(time[0])!=1:
            #每这周第一天处理上周周数据,采用的是基于周的年%x,%v每周一为周的第一天
            startday,endday = getfirs_endtday(period_w)
            condition = "`fdate` between {} and {}".format(startday,endday)
        elif int(time[1])==1 and int(time[0])==1:
            #恰好即使当月第一天又是当周第一天
            startday,endday = getfirs_endtday(period_m)
            condition = "`fdate` between {} and {}".format(startday,endday)
        else:
            print("{}再来找我处理数据吧".format('明天' if x==0 else '{}天后'.format(x)))
            print("****************************************\n默认在1号或周一处理;\n****************************************",
                  "\n****************************************\n可自定义处理周期或本地数据路径:",
                  "\n'main.py -p <inputperiod> -f <inputfile>'\n****************************************")
        df = read_db(condition)
    else:
        try:
            int(arg)
            #print(arg)
        except:
            print("从本地路径获取数据")
            reader = pd.read_csv('{}'.format(arg),usecols=['点评ID', '航班号', '仓位','航班日期','评论','出发地编码','目的地编码','计划出发时间','计划到达时间','实际出发时间','实际到达时间'], iterator=True,low_memory=False)
            loop = True
            chunkSize =100000
            chunks = []
            while loop:
                try:
                    chunk = reader.get_chunk(chunkSize)
                    chunks.append(chunk)
                except StopIteration:
                    loop = False
            df = pd.concat(chunks, ignore_index=True)
            df = df.loc[:1000]
        else:
            if len(arg)==8:
               condition = "`fdate` = {}".format(arg) 
            else:
                startday,endday = getfirs_endtday(arg)
                condition = "`fdate` between {} and {}".format(startday,endday)
            df = read_db(condition)
    if df.empty==False:
        df = df.drop_duplicates(['航班号','航班日期','评论'])
        df['评论']=df['评论'].apply(lambda x:np.NaN if str(x).strip()=="" or\
          p.rinse(str(x).strip())==""else x)
        df = df[df['评论'].notnull()]
        df['irr_f'] = df.apply(lambda x:get_TDOA(x),axis=1)
        df['评论'] = list(df['评论'].apply(lambda x:x.strip().replace("\r\n","。")))
        df['评论'] = df['评论'].apply(lambda x:p.cht_to_chs(x))
        df['月份'] = df['航班日期'].apply(lambda x:int(get_ym(str(x))[0]))
        df['周次'] = df['航班日期'].apply(lambda x:int(get_ym(str(x))[1]))
        df['airline'] = df['航班号'].apply(lambda x:x[0:2])
        df = df.loc[:,['点评ID','评论','航班号','irr_f', '仓位','出发地编码', '目的地编码', '航班日期', '月份', '周次', 'airline']]
        df.index  = np.arange(len(df))
        return df
        
def stampTostrtime(timeStamp):
    dateArray = datetime.datetime.utcfromtimestamp(int(timeStamp))
    otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
    return otherStyleTime


def get_ym(s):
# =============================================================================
#     try:
#         s = datetime.datetime.strptime(s, "%Y-%m-%d")
#     except:
#         s = datetime.datetime.strptime(s, "%Y%m%d")
#     x = datetime.datetime.strftime(s,"%Y%m %G0%V")
# =============================================================================
    s = s.replace('-','')
    x = datetime.datetime.strftime(datetime.datetime.strptime(s, "%Y%m%d"),"%Y%m %G0%V")
    #每年的第几周，使用基于周的年
    #月 使用的是带世纪部分的十制年份
    mon = x.split(" ")[0]
    w = x.split(" ")[1]
    return mon,w

def get_TDOA(x):
    if pd.notnull(x['实际到达时间']) and str(x['实际到达时间']) != ('0' or ''):
        try:
            time.strptime(str(x['实际到达时间']),"%Y-%m-%d %H:%M:%S")
        except:
            a = time.strptime(str(x['实际到达时间']),"%Y/%m/%d %H:%M")
            b = time.strptime(str(x['计划到达时间']),"%Y/%m/%d %H:%M")
        else:
            a = time.strptime(str(x['实际到达时间']),"%Y-%m-%d %H:%M:%S")
            b =time.strptime(str(x['计划到达时间']),"%Y-%m-%d %H:%M:%S")
        arr_TDOA = (time.mktime(b)-time.mktime(a))/60
        if pd.notnull(x['实际出发时间']) and str(x['实际出发时间']) != ('0' or ''):
            try:
                time.strptime(str(x['实际出发时间']),"%Y-%m-%d %H:%M:%S")
            except:
                c =time.strptime(str(x['实际出发时间']),"%Y/%m/%d %H:%M")
                d =time.strptime(str(x['计划出发时间']),"%Y/%m/%d %H:%M")
            else:
                c =time.strptime(str(x['实际出发时间']),"%Y-%m-%d %H:%M:%S")
                d =time.strptime(str(x['计划出发时间']),"%Y-%m-%d %H:%M:%S")
            dep_TDOA = (time.mktime(c)-time.mktime(d))/60
            if  dep_TDOA>30 or arr_TDOA>30:
                return 0
            else:
                return 1
        else:
            if  arr_TDOA>30:
                return 0
            else:
                return 1
    else:  
         return 0

def seo_HEDkeyword(st,words,postag,parse,head):
    '''
    优化hedkeyword
    '''
    #动词性情感词
    special=['可以','满意','还可以','不错','非常周到','非常满意','不满意','很满意',\
             '人性化','周到','比较好','挤','还不错','很舒服','比较差','很棒','较差','比较满意']
    #否定词
    nwords = ['不必', '木有', '怎么不', '从来不', '请勿', '不曾', '不会', '未尝', '禁止', '勿', '拒绝', '放弃', '缺少', '放下',
 '甭', '极少', '不大', '毫无', '毋', '绝非', '不要', '没有', '不能', '没怎么', '尚无', '不怎么', '不该', '莫', '从未有过',
 '反对', '非', '无', '没', '永不', '尚未', '不甚','很少', '从未', '停止', '不', '绝不', '一无', '并非', '切莫', '并未', '杜绝', '未', '几乎不', '终止', '缺乏', '不丁点儿', '不用', '不是', '未曾', '难以', '从没', '不可以', '从不', '毫不']
    keyword= []
    zhPattern = re.compile(u'[\u4e00-\u9fa5]+')
    if st in nwords:
        #如果是否定词，则将否定词后面的词联合输出
        w_id = words.index(st)
        if w_id+1 <len(words):
            if zhPattern.search(words[w_id+1]):
                keyword.append(''.join(words[w_id:w_id+2]))
    elif postag[words.index(st)] == 'v' or postag[words.index(st)] == 'n' and st not in special:
    #如果hed词是动词，则提取语法分析中父节点为hed词的形容词或者未识别次
        for i in range(len(head)):
            if  head[i] == parse.index('HED'):
                if postag[i] in ['a','ws','b','i'] or parse[i].endswith('OB'):
                    keyword.append(words[i])
    elif postag[words.index(st)] =='c':
    #如果hed词是连接词，则把连接词和形容词或者未识别次一起返回
        for i in range(len(head)):
            if  head[i] == words.index(st):
                if postag[i] in ['a','ws','b','i']:
                    try:
                        if postag[i-1] == 'n':
                            keyword.append(words[i-1])
                    except:
                        pass
                    keyword.append(words[i])
                    try:
                        if postag[i+1] == 'n':
                            keyword.append(words[i+1])
                    except:
                        pass
        keyword.append(st)
    if ' ' in keyword:
        keyword.remove(' ')
    if keyword:
        return set(keyword)
    else:
        keyword.append(st)
        return set(keyword)

def seo_stkeyword(st,words,postag,parse,head):
    '''
    优化stkeyword
    '''
    special=['可以','满意','还可以','不错','非常周到','非常满意','不满意','很满意',\
             '人性化','周到','比较好','挤','还不错','很舒服','比较差','很棒','较差','比较满意']
    keyword= []
    zhPattern = re.compile(u'[\u4e00-\u9fa5]+')
    nwords = ['不必', '木有', '怎么不', '从来不', '请勿', '不曾', '不会', '未尝', '禁止', '勿', '拒绝', '放弃', '缺少', '放下',
 '甭', '极少', '不大', '毫无', '毋', '绝非', '不要', '没有', '不能', '没怎么', '尚无', '不怎么', '不该', '莫', '从未有过',
 '反对', '非', '无', '没', '永不', '尚未', '不甚','很少', '从未', '停止', '不', '绝不', '一无', '并非', '切莫', '并未', '杜绝',
 '未', '几乎不', '终止', '缺乏', '不丁点儿', '不用', '不是', '未曾', '难以', '从没', '不可以', '从不', '毫不','不']
    if st in nwords:
        #如果是否定词，则将否定词后面的词联合输出
        w_id = words.index(st)
        if w_id+1 <len(words):
            if zhPattern.search(words[w_id+1]):
                keyword.append(''.join(words[w_id:w_id+2]))
    elif postag[words.index(st)] == 'v' and st not in special:
    #如果hed词是动词，则提取语法分析中父节点为hed词的形容词或者未识别次
        try:
            nindex = words.index(st)-1
        except:
            pass
        else:
            if postag[nindex] in ['n','d','m','a']:
                word =words[nindex]+st
                keyword.append(word)
        if len(keyword)==0:
            for i in range(len(head)):
                if  head[i] == words.index(st):
                    if postag[i] in ['a','ws','b'] or words[i] in special or parse[i].endswith('OB') and postag[i] !='n':
                        try:
                            if postag[i-1] in ['n','d','m','a'] :
                                keyword.append(words[i-1])
                        except:
                            pass
                        keyword.append(words[i])
                        try:
                            if postag[i+1] in ['n','d','m','a']:
                                keyword.append(words[i+1])
                        except:
                            pass

        #st = words[head[parse.index('HED')]]
    elif postag[words.index(st)] =='c':
    #如果hed词是连接词，则把连接词和形容词或者未识别次一起返回
        #keyword.append(st)
        for i in range(len(head)):
            if  head[i] == words.index(st):
                if postag[i] in ['a','ws','b']:
                    try:
                        if postag[i-1] == 'n':
                            keyword.append(words[i-1])
                    except:
                        pass
                    keyword.append(words[i])
                    try:
                        if postag[i+1] == 'n':
                            keyword.append(words[i+1])
                    except:
                        pass
    if ' ' in keyword:
        keyword.remove(' ')
    if keyword:
        return list(set(keyword))
    else:
        keyword.append(st)
        return list(set(keyword))
    


def get_sentences(sentences):
    window_size=5
    '''
    正向匹配，按照通顺度分解句子
    '''
    sents = list(p.sub_sentences(sentences))
    sents_n = []
    idx = 0
    while idx < len(sents):
        scores = []
        sent = []
        for i in range(1,window_size,1):
            #print(i)
            cand = ''.join(sents[idx:idx+i])
            sent.append(cand)
            score = aip.get_ppl(cand)
            scores.append(score)            
        j = scores.index(min(scores))
        sents_n.append(sent[j].encode("utf8"))
        idx += j+1
    return sents_n

def get_ppnum(labledf,sents,irr_f): 
    '''
    获取类目数量，根据数量做下一分割
    '''
    if int(irr_f)==1:
            labledf = labledf.loc[labledf['code']!='B9']#如果是正常航班，则不靠存在不正常航班服务
    matchs  = labledf['关键词'].values
    el = []
    for ec in matchs:
        #if re.search(ec,sents):
        if ec in list(p.cut_jieba(sents)):
            liab = labledf[labledf['关键词'] == ec]['code'].values[0]
            if liab not in el:
                el.append(liab)
            # print(labledf[labledf['关键词'] == ec]['一级指标'].values[0])
    return len(set(el)),list(set(el))


def get_keyword(ec,labledf,i,labdf,head,parse,postag,words):   
    keyword = []
    lg = parse[i]
    #lgn = parse[head[i]]
# =============================================================================
#             and lg.endswith('OB')==False \
#             and lgn.endswith('OB')==False\
# =============================================================================
    #re.match('^(.*?)OB$',lg) #正则匹配 ^表示开头    OB$表示以OB结尾                                           
    if lg.endswith('OB') or lg == 'HED':
        try:
            kwords = {}
            for hi in range(len(head)):
                if head[hi] == i and postag[hi] in ['a','ws','b','v']:
                    kw = words[hi]
                    if postag[hi] =='v':
                        for nh in range(len(head)):
                            if head[nh] ==hi and words[nh] !=ec:
                                kw = kw+words[nh]
                        kwords['{}'.format(hi)]=kw
                    else:
                        kwords['{}'.format(hi-i)]=words[hi]
            print(kwords)
            if kwords:
                if kwords[min(kwords.keys())]!= ec:
                    keyword.append(kwords[min(kwords.keys())])
                elif lg == 'HED' and 'VOB' in parse:
                    keyword.append(words[parse.index('VOB')])
            elif lg.endswith('OB') and 'HED' in parse:
                if parse[parse.index('HED')+1] == 'ADV' and 'VOB' in parse[(parse.index('HED')+1):]:
                    keyword.append(words[parse.index('HED')]+words[parse.index('HED')+1]+words[parse.index('VOB')])
                else:
                    keyword.append(words[parse.index('HED')])          
        except:
            try:
                if parse[i-1] in ['a','ws','b','v']:
                    keyword.append(words[i-1])
            except:
                pass
    elif lg == 'SBV':
        print(1,'SVB')
        try:
            parent_indexn =head[i]
            parent_wordn = words[parent_indexn]
            print(1.1,parent_wordn,parent_indexn)
            if parse[parent_indexn-1] == 'ADV':
                keyword.append(words[parent_indexn-1]+parent_wordn)
            else:
                if parse[parent_indexn] =='VOB':
                    kw = ''
                    for j in range(len(parse)):
                        if parse[j]=='VOB':
                            kw = kw+words[j]  
                    print(1.2,kw)
                    keyword.append(kw)
                elif parse[parent_indexn] =='HED':
                    if parse[parse.index('HED')+1] == 'ADV' and 'VOB' in parse[(parse.index('HED')+1):]:
                        keyword.append(words[parse.index('HED')]+words[parse.index('HED')+1]+words[parse.index('VOB')])
                    else:
                        keyword.append(words[parse.index('HED')])  
                elif parse[parent_indexn] =='COO':
                    kw = ''
                    lst = []
                    lst.append(parent_indexn)
                    for  j in range(len(head)):
                        if head[j]==parent_indexn:
                            lst.append(j)
                    for l in set(lst):
                       kw = kw+words[l] 
                    keyword.append(kw)
                else:
                    keyword.append(parent_wordn)
# =============================================================================
#                         for hi in range(len(head)):
#                             if head[hi] ==head[words.index(ec)] and postag[hi] in ['a','ws','b','v']:
#                                 if parse[hi-1] == 'ADV':
#                                     keyword.append(words[hi-1]+words[hi])
#                                 else:
#                                     keyword.append(words[hi])
# =============================================================================
        except:
            pass
        if keyword:
            pass
        else:
           keyword.append(words[head[words.index(ec)]])
        print(keyword)
    elif lg == 'ATT':
        print(2,'ATT')
        parent_word = words[head[i]]
        parent_index = words.index(parent_word)
        print(2.1,parent_word,parent_index)
        if parse[parent_index] =='COO':
            keyword.append(parent_word)
        elif parse[parent_index] =='SBV':
            try:
                parent_indexn =head[parent_index]
                parent_wordn = words[parent_indexn]
                print(2.2,parent_wordn,parent_indexn)
                if parse[parent_indexn-1] == 'ADV':
                    keyword.append(words[parent_indexn-1]+parent_wordn)
                else:
                    keyword.append(parent_wordn)
            except:
                pass
        elif 'HED' in parse:
            keyword.append(words[parse.index('HED')])
        print(2.3,keyword)
    elif lg == 'COO':
        print(3,'COO')
        for j in range(len(head)):
            if head[j] == i:
                keyword.append(words[j])
                for ji in range(len(head)):
                    if  head[ji] ==j:
                        keyword.append(words[ji])        
        parent_word = words[head[i]]
        parent_index = words.index(parent_word)
        
# =============================================================================
#     if keyword:
#         keyword = keyword
#     else:
#         keyword= words[head[words.index(ec)]]
#         keyword = seo_stkeyword(keyword,words,postag,parse,head)
# =============================================================================
    print(2.4,keyword)
    return keyword

def get(words,postag,parse,head,labledf,irr_f):
    '''
    获取类目el,关键词keyword
    '''
    labledf1 = labledf.loc[(labledf['code']=='J2')|(labledf['code']=='J3')]
    labledf_2 = labledf.loc[(labledf['code']!='J2')& (labledf['code']!='J3')& (labledf['code']!='B9')]
    labledf_3 = labledf.loc[(labledf['code']!='J2')& (labledf['code']!='J3')]
    #matchs  = labledf['关键词'].values
    matchs1  = labledf1['关键词'].values
    judge= ['自助机', '转盘', '指示牌', '饮水机', \
          '休息室', '吸烟室', '推车','免税店','基础设施',\
          '机场设施', '机场建设', '机场环境', '机场广播', '机场服务', '候机座位', '候机区',\
          '行李推车', '贵宾休息室', '贵宾厅', '贵宾室','地勤','地勤服务','地面服务','地服','安检通道',\
          '安检','柜台值机','候机座位','候机楼','候机室','转盘']
    el = []
    keywords = []
    if set(judge).intersection(words) or re.search('机场',' '.join(words)):
        for i in range(len(words)): 
            ec = words[i]
            #判定ec是否在matchs中并判定其在语法状态下其自身与其父节点均不能是宾语           
            if ec in matchs1:
# =============================================================================
#                 keyword = ''
#                 lg = parse[i]
#                 #lgn = parse[head[i]]
# # =============================================================================
# #                 and lg.endswith('OB')==False \
# #             and lgn.endswith('OB')==False
# # =============================================================================
#                 #re.match('^(.*?)OB$',lg) #正则匹配 ^表示开头    OB$表示以OB结尾
#                 liab = labledf1[labledf1['关键词'] == ec]['code'].values[0]                                            
#                 keyword = words[head[words.index(ec)]]
#                 keyword = seo_stkeyword(keyword,words,postag,parse,head)
# =============================================================================
                liab = labledf1[labledf1['关键词'] == ec]['code'].values[0]
                el.append(liab)
                keyword = get_keyword(ec,labledf,i,labledf,head,parse,postag,words)
                for key in keyword:
                    if key!=ec and key in words:
                        print(2.5,key)
                        for s in seo_stkeyword(key,words,postag,parse,head):
                            print(2.6,s)
                            keywords.append(s)
                    else:
                        keywords.append(key)
    else:
        if int(irr_f) ==1:
            labledf2 = labledf_2
        else:
            labledf2 = labledf_3
        matchs2  = labledf2['关键词'].values
        for i in range(len(words)):
            ec = words[i]
            print(i,ec)
            #判定ec是否在matchs中并判定其在语法状态下其自身与其父节点均不能是宾语
            if ec in matchs2:
                liab = labledf2[labledf2['关键词'] == ec]['code'].values[0]     
                keyword = get_keyword(ec,labledf,i,labledf2,head,parse,postag,words)
                for key in keyword:
                    if key!=ec and key in words:
                        print(2.5,key)
                        for s in seo_stkeyword(key,words,postag,parse,head):
                            print(2.6,s)
                            keywords.append(s)
                    else:
                        keywords.append(key)
                el.append(liab)
    return list(set(el)),list(set(keywords))

def getlable(sent,irr_f,labledf):
    import learn_pyltp as ltp
    words = list(p.cut_jieba(sent))
    postag = list(ltp.get_postag(words))
    parse,head = ltp.get_parse(words)
# =============================================================================
#     if 'HED' in parse :
#         keyword1 = words[parse.index('HED')]
#         keyword1 = seo_HEDkeyword(keyword1,words,postag,parse,head)
#     else:
#         keyword1 = []              
# =============================================================================
    el,keyword2=get(words,postag,parse,head,labledf,irr_f)
    #keyword = set([i for i in keyword1]+[i for i in keyword2])
    del ltp
    gc.collect()
    return keyword2,el
    
    
def posmini(row,labledf):
    chilid = []
    docs = []
    docs_rb = []
    idex = []
    doc = []
    hed = []
    lab2 = []      
    sents = row['评论']
    irr_f = row['irr_f']
    pos_num,elo= get_ppnum(labledf,sents,irr_f)

    if pos_num>1:
        if pos_num ==2 and 'B9' in elo:
            sents = [sents]
        else:
            #先分句
            sents_ = list(p.cut_sentences(p.cut_down(sents)))
            sentns = []
            for sent in sents_:
                pos_num2,el2= get_ppnum(labledf,sent,irr_f)
                if pos_num2>1:
                    if pos_num2 ==2 and 'B9' in el2:
                        sentns.append(sent)
                    else:
                        sent_s = list(p.sub_sentences(p.cut_down(sent)))
                        sentns.extend(sent_s)
                        #sentns = [i for i in sent_s]
                else:
                    sentns.append(sent)
            sents = sentns
# =============================================================================
#         #后合并，合并后有许多无用信息，而且速度比较慢      
#         sentns2 = sentns[:]
#         sents2=[]
#         i =0
#         for s in sentns2:
#             if get_ppnum(labledf,s)==0:
#                 n = sentns2.index(s)
#                 if n == len(sentns2)-1:
#                     sents2[i-1]=sents2[i-1]+s
#                 else:
#                     sentns2[n+1]=s+sentns2[n+1]     
#             else:
#                 sents2.append(s)
#                 i= i +1
# 
#         sents = sents2[:]
#         del sentns,sentns2,sents_
#         gc.collect()     
# =============================================================================
            
    else:
        sents = [sents]
    for sent in sents:
        sent_n = p.rebirth_sentence(p.get_abstract(sent))    
        if p.rinse(sent)!='':
            #sent原语句分析
            keyword1,el = getlable(sent,irr_f,labledf)   
            #sent_n重组语句分析
            keyword2,eln= getlable(sent_n,irr_f,labledf) 
            if eln:
                el_=eln
            elif el:
                el_ = el
            else:
                el_ = []
            if el_:
                lables = '、'.join(el_)
                if lables in lab2:
                    #如果存在相同类目，则将两个子句合并
                    docs_rb[lab2.index(lables)] += sent_n
                    docs[lab2.index(lables)] += sent
                    hed[lab2.index(lables)] +='、'.join(set([i for i in keyword1]+[i for i in keyword2]))
                else:
                    chilid.append(sents.index(sent)+1)
                    docs_rb.append(sent_n)
                    docs.append(sent)
                    idex.append(row['点评ID'])
                    doc.append(sents)
                    lab2.append('、'.join(el_))
                    hed.append('、'.join(set([i for i in keyword1]+[i for i in keyword2])))               
    if idex:
        df_new = pd.DataFrame({'点评ID':idex,'评论':doc,'chilid':chilid,'观点':docs,'观点重组':docs_rb,\
                              'lable':lab2,'hed':hed})
        return df_new
    
    
def getPosdata(lablefile,df):
    start = datetime.datetime.now()
    labledf = pd.read_csv(lablefile)
    data=[]
    for index,row in df.iterrows():
        data.append({'点评ID':str(row['点评ID']),'评论':row['评论'],'irr_f':row['irr_f']})
    from functools import partial
    partial_work = partial(posmini,labledf = labledf)
    if 'Linux' in platform.system():
        start2 = datetime.datetime.now()
        pool = Pool(5)
        dfminni =  pool.map(partial_work,data)
        pool.close()
        pool.join()
        end2 = datetime.datetime.now() 
        print('获取lable&key，耗时：{}分钟'.format(round((end2-start2).seconds/60,2)))
    else:
        start1 = datetime.datetime.now()  
        dfminni = [posmini(row,labledf) for row in data]
        end1 = datetime.datetime.now() 
        print('获取lable&key，耗时：{}分钟'.format(round((end1-start1).seconds/60,2)))
# =============================================================================
#         start2 = datetime.datetime.now()
#         pool = Pool(2)
#         dfminni =  pool.map(partial_work,data)
#         pool.close()
#         pool.join()
#         end2 = datetime.datetime.now() 
#         print('data，耗时：{}分钟'.format(round((end2-start2).seconds/60,2)))
# =============================================================================
 
    df_new = pd.concat(dfminni)   
# =============================================================================
#     df_new['lable']=df_new['lable'].apply(lambda x:np.NaN if str(x).strip()=="" or str(x).strip() =="~" else x)
#     df_new= df_new[df_new['lable'].notnull()]
# =============================================================================
    del df['评论']
    df_new['点评ID'] = df_new['点评ID'] .astype(int)
    df['点评ID'] = df['点评ID'] .astype(int)
    #df = df.apply(pd.to_numeric, errors='ignore')
    #df_new = pd.concat([df_new, df], axis=1, join_axes=[df_new['点评ID']])
    df_new = df_new.merge(df,on ='点评ID',how = 'left')
    df_new = df_new[df_new['观点重组'].notnull()]
    df_new = getonelable(df_new)
    #df_new['ADO'] = df_new.apply(lambda x:get_ado(x), axis=1)
    df_new['ADO'] = get_ado(df_new)
    #固定列的顺序
    cols = ['点评ID', '评论', 'chilid', '观点', '观点重组', 'lable', 'hed', '航班号', 'irr_f','仓位',
       '出发地编码', '目的地编码', '航班日期', '月份', '周次', 'airline', 'B1', 'B2', 'B3', 'B4',
       'B5', 'B6', 'B7', 'B8','B9','J1', 'J2', 'J3', 'J4','J6','JGP','ADO']
    df_new=df_new.loc[:,cols]  #.reindex()
    df_new.columns = ['id', 'comment', 'chilid', 'view', 'view_n', 'lable', 'hed', 'fnum','irr_f','fspace',
       'dep_code','arr_code','fdate', 'period_m', 'period_w','airline','B1','B2','B3','B4','B5','B6','B7','B8','B9','J1','J2','J3','J4','J6','JGP','ADO']
    del df #,df_new.lable
    gc.collect()
    end = datetime.datetime.now() 
    print('lable@hed已完成，耗时：{}分钟'.format(round((end-start).seconds/60,2)))
    for x  in list(locals().keys()):
        del locals()[x]        
        gc.collect()
    return df_new



def get_ado(df):
    '''
    评价对象：机场还是航司，B代表航司;J代表机场
    '''
    x = []
    for index,row in df.iterrows():
        y1 = row['B1']+row['B2']+row['B3']+row['B4']+row['B5']+row['B6']+row['B7']+row['B8']+row['JGP']
        y2 = row['J1']+row['J2']+row['J3']+row['J4']+row['J6']
        if y1>0 and y2==0:
            x.append('B')
        elif y1==0 and y2>0:
            x.append('J')
        elif y1 >0 and y2 >0:
            x.append('BJ')
        else:
            x.append('')
    return x

def getonelable(df):
    '''
    每个标签单独标识，用于后期统计分析
    '''
    import re
# =============================================================================
#     lables = ['不正常航班','购票','行李服务','机场安检','机场服务与设施',\
#               '机场交通','机场商贸','机供品','机上餐食','机上广播',\
#               '机上娱乐','客舱设施','空乘服务','值机与离港']
# =============================================================================
    lables = ['B1','B2','B3','B4','B5','B6','B7','B8','B9','JGP',\
              'J1','J2','J3','J4','J6']
    lable = list(df['lable'])
    for lab in lables:
        lb = []
        for i in range(len(lable)):
            if re.search(lab, lable[i]):
                lb.append(1)
            else:
                lb.append(0)    
        df['{}'.format(lab)] = lb
        df['{}'.format(lab)].astype(int)
    return df

def get_keywordsdf(df,stopkeyfile):
    '''
    将提取出的关键词提取出来，方便后面单独存入数据库
    '''
    import re
    #匹配中文的分词
    zhPattern = re.compile(u'[\u4e00-\u9fa5]+')
    idd=[]
    key=[]
    chilid = []
    uuid = []
    f = open(stopkeyfile , "r",encoding = 'utf8')
    stopkeywords =list(set
                ([line.strip()for line in f.readlines()])
                )
    f.close()  
    for index,row in df.iterrows():
         keys =  row['hed'].split('、')
         uid = 0
         for i in keys:
             if zhPattern.search(i) and i not in stopkeywords:
                uid +=1
                idd.append(row['id'])
                chilid.append(row['chilid'])
                key.append(i)
                uuid.append(int(str(row['id'])+str(row['chilid'])+str(uid)))               
    del df
    gc.collect()
    df_key = pd.DataFrame({'uuid':uuid,'id':idd,'chilid':chilid,'keyword':key})
    df_key.uuid.astype(int)
    df_key.id.astype(int)
    df_key.chilid.astype(int)
    df_key.keyword.astype(str)
    #固定列的顺序
    cols=['uuid','id','chilid','keyword']
    df_key=df_key.loc[:,cols]

    return df_key




def getCoMatrix(df,lables):
    '''
    获取词共现矩阵,保存为json
    '''
    import json
    Matrixjs ={}
    for j in lables:
        #print(j)
        df_n = df.loc[df['{}'.format(j)]==1]
        #print(j)
        if len(df_n)>0:
            CooccurrenceMatrix = co.get_CooccurrenceMatrix(list(df_n['view'].astype(str)))
            Matrixjs['{}'.format(j)]=json.dumps(CooccurrenceMatrix)
    return  Matrixjs
      

if __name__ == '__main__':
    starttime = datetime.datetime.now()
    file = DATA_PATH+'2018_33.csv'
    lablefile = DATA_PATH+'newlable.csv'
    df = loaddata(file)
    #df = df.loc[:100]
    df = getPosdata(lablefile,df)
    #df.to_csv(path+'data/tagger/201801_06_tagg_0712.csv',index = False,sep = ',',encoding='gbk')
# =============================================================================
#     stopkeyfile = DATA_PATH+'stopkey.txt'
#     df_key = get_keywordsdf(df,stopkeyfile)
#     df_key.head(5)
# =============================================================================
    endtime = datetime.datetime.now() 
# =============================================================================
#     del df,df_key
#     gc.collect()
# =============================================================================
    print('程序运行完毕，耗时：{}秒'.format((endtime-starttime).seconds))
