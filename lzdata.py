# coding=utf8
import pandas as pd
import os
import platform
import participle as p
if 'Windows' in platform.system():
    DATA_PATH = os.path.abspath(os.path.curdir)+os.sep+'data'+os.sep
else:
    DATA_PATH = os.path.abspath(os.path.dirname(__file__))+os.sep+'data'+os.sep
import pymysql
import datetime
import re
import sys
#设置编码
import importlib
importlib.reload(sys)
#获得系统编码格式
type = sys.getfilesystemencoding()
import json

import gzip
import re
import http.cookiejar
import urllib.request
import urllib.parse
 
def ungzip(data):
    try:        # 尝试解压
        print('正在解压.....')
        data = gzip.decompress(data)
        print('解压完毕!')
    except:
        print('未经压缩, 无需解压')
    return data
 
def getXSRF(data):
    cer = re.compile('name=\"_xsrf\" value=\"(.*)\"', flags = 0)
    strlist = cer.findall(data)
    return strlist[0]
 
def getOpener(header):
    # deal with the Cookies
    #cj = http.cookiejar.CookieJar()
    #pro = urllib.request.HTTPCookieProcessor()
    opener = urllib.request.build_opener()
    headers = []
    for key, value in header.items():
        elem = (key, value)
        headers.append(elem)
    opener.addheaders = headers
    return opener


def get_json(url):
    #第一步：取得用户名与密码的动态名称，名称是动态的，及其他动态登录信息
    #url = "http://om.capse.com.cn/admin/web/admin/login/login"
    url = 'http://om.capse.com.cn/admin/web/admin/index'
    #cookie = '__Manage_identity=520ab438b96be4668f9adeb100ea9c6edfa6afe0664773f7f8ede360b19321ffa%3A2%3A%7Bi%3A0%3Bs%3A17%3A%22__Manage_identity%22%3Bi%3A1%3Bs%3A28%3A%22%5B%22100%22%2C%22test100key%22%2C2592000%5D%22%3B%7D; expires=Fri, 05-Oct-2018 07:24:50 GMT; Max-Age=2592000; path=/; httponly'
    header = { #"User-Agent" : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
               "Referer": 'http://om.capse.com.cn/admin/web/admin/index'
               ,'Connection': 'Keep-Alive'
               ,'Content-Type': 'application/json; charset=UTF-8'
               ,'Set-Cookie': '__Manage_identity=520ab438b96be4668f9adeb100ea9c6edfa6afe0664773f7f8ede360b19321ffa%3A2%3A%7Bi%3A0%3Bs%3A17%3A%22__Manage_identity%22%3Bi%3A1%3Bs%3A28%3A%22%5B%22100%22%2C%22test100key%22%2C2592000%5D%22%3B%7D; PHPSESSID=4j1tqc9mhj6al816ap9huhboj1'
               ,'Vary': 'Accept-Encoding'
               ,'Content-Encoding': 'gzip'
               ,'Pragma': 'no-cache'
                }
    opener = urllib.request.build_opener()
    opener.addheader = header
# =============================================================================
#     op = opener.open(url)
#     data = op.read()
#     data = ungzip(data)     # 解压
#     _xsrf = getXSRF(data.decode())
# =============================================================================
    id = 'admin'
    password = 'admin'
    postDict = {
            'info[username]': id,
            'info[rememberMe]': password,
            'info[password]': '1'
    }
    postData = urllib.parse.urlencode(postDict).encode()
    url = url + '/query-sql?sql=SELECT%20*%20FROM%20`nlp_week_data`%20where%20period_w=2018033%20limit%2010;'
    op = opener.open(url,postData) 
    data = op.read()
    data = ungzip(data)
    print(type(data))
     
    print(data.decode())
    
    



def readTextFile(fname):
    try:
        fobj=open(fname,'r',encoding='UTF-8')
        text = json.load(fobj.readlines())
        
    except IOError as e:
        print('文件打开错误:',e)
    else:
        return text



def getYesterday(): 
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=today-oneday  
    return yesterday.strftime("%Y%m%d")

url = 'http://om.capse.com.cn/admin/web/admin/index/query-sql?sql=SELECT%20%20*%20FROM%20`nlp_week_data`%20where%20period_w%20between%202018035%20and%202018035%20%20and%20void%20=%20%27SC%27;'


for i in data:
        i['evaluated_answers'] = ''
        i['evaluated_images'] = ''
        i['evaluated_text'] = p.transferred(str(i['evaluated_text']))#.encode('utf-8').decode('utf-8')
        InsertData('capse_flight_evaluation',i)
