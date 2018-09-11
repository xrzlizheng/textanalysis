#!/home/feeyo_s_lz/anaconda3/bin/python
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 26 10:18:39 2018

@author: lz
"""
import pandas as pd
import os
import platform
if 'Windows' in platform.system():
    DATA_PATH = os.path.abspath(os.path.curdir)+os.sep+'data'+os.sep
else:
    DATA_PATH = os.path.abspath(os.path.dirname(__file__))+os.sep+'data'+os.sep
import gc
import datetime
import json 
import sensenlp as snlp
import pymysql
#将每行更新或写入数据库中
def doupinsert(df,sql_insert):
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
    cur = db.cursor() 
    df = df.fillna(0)
    n = 0
    pn = 1
    for index,row in df.iterrows():
        data = list(row)
        cur.execute(sql_insert,data)
        n+=1
        if n>300:
            try:
                print('写入{}次'.format(pn))
                db.commit()
                n = 0
                pn+=1
            except Exception as e:
                print(e)
                #错误回滚
                db.rollback()
    try:
        db.commit()
    except Exception as e:
        print(e)
    #错误回滚
        db.rollback()
    db.close() 
    
    
def do_upinsert(df,tablename):

    if tablename =='nlp_month_data':
        stat = datetime.datetime.now()
        sql_insert ="""/*sense_nlp*/insert  into nlp_month_data(void, period_m, keywordsdata) values(%s,%s,%s) on duplicate key update keywordsdata = values(keywordsdata)"""
        doupinsert(df,sql_insert)
        end =  datetime.datetime.now()
        print('nlp_month_data:{}秒'.format((end-stat).seconds/60))
    elif tablename =='nlp_week_data':
        stat = datetime.datetime.now()
        sql_insert ="""/*sense_nlp*/insert  into nlp_week_data(void, period_w, keywordsdata) values(%s,%s,%s) on duplicate key update keywordsdata = values(keywordsdata)"""  
        doupinsert(df,sql_insert)
        end =  datetime.datetime.now()
        print('nlp_week_data:{}秒'.format((end-stat).seconds/60))
    elif tablename =='nlp_tagge':
        stat = datetime.datetime.now()          
        sql_insert ="""/*sense_nlp*/insert  into nlp_tagge(id,evaluated_text,chilid,clause,sub_clause,lable,hed,fnum,irr_f,fspace,dep_code,arr_code,fdate,period_m,period_w,airline,b1,b2,b3,b4,b5,b6,b7,b8,b9,j1,j2,j3,j4,j6,jgp,ado,emo) 
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on duplicate key update clause = values(clause),sub_clause = values(sub_clause),lable = values(lable),hed= values(hed),irr_f = values(irr_f),ado= values(ado),emo= values(emo),b1=values(b1),b2=values(b2),b3=values(b3),b4=values(b4),b5=values(b5),b6=values(b6),b7=values(b7),b8=values(b8),b9=values(b9),j1=values(j1),j2=values(j2),j3=values(j3),j4=values(j4),j6=values(j6)"""  
        doupinsert(df,sql_insert)
        end =  datetime.datetime.now()
        print('nlp_tagge:{}秒'.format((end-stat).seconds/60))
    elif tablename =='nlp_keyword':
        stat = datetime.datetime.now()
        sql_insert ="""insert into nlp_keyword(uuid,id,chilid,keyword) values(%s,%s,%s,%s) on duplicate key update keyword = values(keyword)""" 
        doupinsert(df,sql_insert)
# =============================================================================
#         col = list(df.columns)
#         for index,row in df.iterrows():
#             data = []
#             for i in col:
#                  data.append(row['{}'.format(i)])
#             #sql_insert ="""replace into nlp_keyword_test1 values(%s,%s,%s,%s)"""     
#             data.append(data[-1])
#             sql_insert ="""/*sense_nlp*/insert into nlp_keyword()
#             values(%s,%s,%s,%s)
#             on duplicate key update keyword =values(keyword)
#             """ 
#             cur.execute(sql_insert,data)
#         try:
#             db.commit()
#         except Exception as e:
#             print(e)
#             #错误回滚
#             db.rollback()
# =============================================================================
        end =  datetime.datetime.now()
        print('nlp_keyword:{}秒'.format((end-stat).seconds))

        


def get_js(df,df_keywords,emo,lables):
    js_irr={}#航班明细数据
    #num = {}
    for irr_f in [0,1,2]:
        if irr_f == 0:
            strt = 'normal'#正常航班
            df_airp = df.loc[df['irr_f'] == 1]
        elif irr_f ==1:
            strt = 'abnormal'#不正常航班
            df_airp = df.loc[df['irr_f'] == 0]
        else:
            strt = 'all'#全部航班
            df_airp = df
        #print('df_airp:',len(df_airp))
        if len(df_airp)>0:
# =============================================================================
#             df_num =df_airp.drop_duplicates(['emo','id']).groupby('emo')['id'].count()
#             num['{}'.format(strt)]= df_num.to_dict()
# =============================================================================
            js_lab = {}
            for j in lables:
                js1 = {}
                df_lab = df_airp.loc[df_airp['{}'.format(j)]==1]
                if len(df_lab)>0:
                    df_lab_num = df_lab.drop_duplicates(['lable','emo','id']).groupby('emo')['id'].count()
                    js1['com_num']=df_lab_num.to_dict()
                    del df_lab_num
                    gc.collect()
                    js_emo = {}
                    for e in emo:
                        js2={}
                        df1 = pd.DataFrame(df_lab.loc[df_lab['emo']== int('{}'.format(e))].loc[:,['id','chilid']]).drop_duplicates()
                        if len(df1)>0:
                            df1['idd'] = df1.apply(lambda x:str(x['id'])+str(x['chilid']),axis=1)
                            t=[]
                            for idd in list(df1['idd']):
                                t.append(df_keywords.loc[df_keywords.idd == idd])
                            df_key = pd.concat(t)
                            df_key = df_key.groupby('keyword')['id'].count()
                            js2=df_key.to_dict()
                            del df_key
                            gc.collect()
                        js_emo['{}'.format(e)] = sorted(js2.items(),key = lambda item:item[1],reverse = True)
                    js1['keywords'] = js_emo
                    js_lab['{}'.format(j)] = js1
            js_irr['{}'.format(strt)] = js_lab
    return js_irr

def get_json(period,df,df_key):
    if int(period) < 1000000:
        par = 'period_m'
    else:
        par = 'period_w'
    df_keywords = df_key
    df_keywords['idd'] = df_keywords.apply(lambda x:str(x['id'])+str(x['chilid']),axis=1)
    lables_line = ['B1','B2','B3','B4','B5','B6','B7','B8','B9','JGP']
    lables_port = ['J1','J2','J3','J4','J6']
    emo = [0,1,2]
    Void = []
    JS=[]
    Period = [] 
    CoMatrix_js = []
    df_B = df.loc[(df.ADO =='B')|(df.ADO =='BJ')]
    df_B =df_B.loc[df_B['{}'.format(par)]==int(period)]
    airline = list(set(df_B['airline'].tolist()))
    if len(airline)>0:
        for i in airline:
            js={}
            df_airline = df_B.loc[df_B.airline=='{}'.format(i)]
            CoMatrix_js.append(json.dumps(snlp.getCoMatrix(df_airline,lables_line)))
            #df_words = df_keywords.merge(df_airline.loc[:,['id','chilid','emo','irr_f']],on = ['id','chilid'],how = 'inner')
# =============================================================================
#             df_irr_f = df_airline.drop_duplicates(['irr_f','id']).groupby(['irr_f','lable'])['id'].count()
#             js['irr_num'] = df_irr_f.to_dict()
#             del df_irr_f
#             gc.collect()
# =============================================================================
            js_irr = get_js(df_airline,df_keywords,emo,lables_line)
            js['detail'] = js_irr
            df_airline = df_airline.drop_duplicates(['irr_f','emo','id'])#根据irr_f/emo/id去重
            overview = pd.crosstab(df_airline.emo,df_airline.irr_f, margins=True)
            #overview.index = ['normal','abnormal','ALL']
            js['overview'] =overview.to_dict()#生成透视表
            js = sorted(js.items(), key=lambda item: item[0], reverse=True)
            #print(js)
            Void.append(i)
            JS.append(json.dumps(js,ensure_ascii=False))
            #print(json.dumps(js))
            Period.append(period)
    df_J = df.loc[(df.ADO =='J')|(df.ADO =='BJ')]
    df_J =df_J.loc[df_J['{}'.format(par)]==int(period)]
    ls1 = df_J['dep_code'].tolist()
    ls2 = df_J['arr_code'].tolist() 
    airport= list(set(ls1+ls2))
    del ls1,ls2
    gc.collect()
    if len(airport)>0:
        for i in airport:
            js={}
            df_airport = df_J.loc[(df_J['dep_code']=='{}'.format(i))|(df_J['arr_code']=='{}'.format(i))]
            CoMatrix_js.append(json.dumps(snlp.getCoMatrix(df_airport,lables_port)))
# =============================================================================
#             df_irr_f = df_airport.drop_duplicates(['irr_f','id']).groupby('irr_f')['id'].count()
#             js['irr_num']=df_irr_f.to_dict()
#             del df_irr_f
#             gc.collect()
# =============================================================================
            js_irr = get_js(df_airport,df_keywords,emo,lables_port)
            js['detail'] =  js_irr
            df_airport =  df_airport.drop_duplicates(['irr_f','emo','id'])#根据irr_f/emo/id去重
            overview = pd.crosstab(df_airport.emo,df_airport.irr_f, margins=True)
            #overview.index = ['normal','abnormal','ALL']
            js['overview'] =overview.to_dict()#生成透视表
            js = sorted(js.items(), key=lambda item: item[0], reverse=True)
            #print(js)
            Void.append(i)
            JS.append(json.dumps(js,ensure_ascii=False))
            #print(json.dumps(js,ensure_ascii=False))
            Period.append(period)

    del airport,airline,df,df_B,df_J
    gc.collect()  
    # 用sqlalchemy构建数据库链接engine
    #engine = create_engine("mysql+pymysql://root:123456@192.168.15.154:3306/capse_survey?charset=utf8") #1
    if par == 'period_m':
        df = pd.DataFrame({'void':Void,'period_m':Period,'keywordsdata':JS,'CoMatrix':CoMatrix_js})
        df = df.loc[:,['void','period_m','keywordsdata'
                       #,'CoMatrix' #词共现矩阵
                       ]]

        tablename = 'nlp_month_data'
        do_upinsert(df,tablename)

    else:
        df = pd.DataFrame({'void':Void,'period_w':Period,'keywordsdata':JS,'CoMatrix':CoMatrix_js})
        df = df.loc[:,['void','period_w','keywordsdata'
                       #,'CoMatrix' #词共现矩阵
                       ]]
        tablename = 'nlp_week_data'
        do_upinsert(df,tablename)
