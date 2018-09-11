#!/home/feeyo_s_lz/anaconda3/bin/python
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 21 15:45:58 2018

@author: lz
分词模块 分别为普通分词、tfidf主题分词、textrank主题分词
"""
import datetime
import os
import platform
if 'Windows' in platform.system():
    DATA_PATH = os.path.abspath(os.path.curdir)+os.sep+'data'+os.sep
else:
    DATA_PATH = os.path.abspath(os.path.dirname(__file__))+os.sep+'data'+os.sep
import jieba
import numpy as np
from jieba import analyse 
jieba.load_userdict(DATA_PATH+"newdict_add_sentimentwords.txt")
stopwords = {}.fromkeys([line.rstrip() for line in \
            open(DATA_PATH+"stopwords.txt",encoding = 'utf-8')])
stopwords.update({}.fromkeys([' ', '\t', '\n']))
import re
#匹配中文的分词
zhPattern = re.compile(u'[\u4e00-\u9fa5]+')
Pattern = re.compile(u'[\a-zA-Z]+')
import networkx as nx  
from sklearn.feature_extraction.text import TfidfVectorizer, TfidfTransformer  
import langconv as conv
import pandas as pd
import gc
from sklearn.cluster import KMeans 
import warnings
warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')

def cut_jieba(sent):
    return jieba.cut(sent,cut_all=False)

def cut_stopwords(sent):
    segs = jieba.cut(sent,cut_all=False)
    out = []
    for seg in segs:
        if seg not in stopwords:
            out.append(seg)
    return ''.join(out)


def transferred(sent):
    #转义句子中的单引号或者双引号
    sent=sent.replace("'","\\\'")  #将单引号转成\单引号
    sent=sent.replace('"','\\\"')  #将双引号转成\双引号
    return sent


def cleantxt(sent):
	fil = re.compile(u'[^0-9a-zA-Z\u4e00-\u9fa5.，,。？“”。;；.。！!？?\....\.]+', re.UNICODE)
	return fil.sub(' ', sent)

def cut_sentences(sentences):  
    """ 
    分句 
    """ 
    sentences = cleantxt(sentences)
    zhPattern1 = re.compile(u'[\.。;；.。！!？?\....\.]')
    zhPattern2 = re.compile(u'[\.\,\，\、\\\ ]')
    zhPattern =  re.compile(u'[\u4e00-\u9fa5_a-zA-Z]+')
    if not isinstance(sentences, str):  
        sentences = sentences.decode('utf-8')
    sentences = re.sub(r'[…_~^$%：^_^～~]', ';', sentences) #/./u
    #判断是否有转折词，如果有转折词，且还这词前面没有。！，？，直接新增一个句号
    whereas = ['但是','就是','然而','反而','而','偏偏','只是','不过','至于',\
               '可是','致','不料','不过','岂知','但','然后','再者','另外','此外','再有',\
               '由于','好在','另方面','另方面','倒是','反倒是','值得一提的是','最重要的是','最主重要的是']
    words = list(cut_jieba(sentences))
    intersection = list(set(whereas).intersection(set(words)))
    for i in intersection:
        idex = words.index(i)
        if idex >0:
            match1 = zhPattern1.search(words[idex-1])
            match2 = zhPattern2.search(words[idex-1])
            if not match1:
                if not match2:
                    words.insert(idex,'。')
                else:
                    words[idex-1]='。'
                    
    sentences = ''.join(words)

    if zhPattern1.search(sentences):
        delimiters = frozenset(u'.。！!？?;；... ') 
    elif zhPattern2.search(sentences):
        delimiters = frozenset(u',， ') 
    else:
        delimiters = frozenset(u' ')
    buf = []  
    for ch in sentences:
        buf.append(ch)
        if delimiters.__contains__(ch):
            if len(buf)>1:
                if zhPattern.search(''.join(buf)):
                    yield ''.join(buf)  
            buf = []  
    if buf:  
        yield ''.join(buf)  
        

def sub_sentences(sentences):  
    """ 
    分句之后的细分，用主题提取
    """ 
    zhPattern =  re.compile(u'[\u4e00-\u9fa5_a-zA-Z]+')
    if not isinstance(sentences, str):  
        sentences = sentences.decode('utf-8')
    delimiters1 = frozenset(u'.,，。!？。！?、')
    delimiters2 = frozenset(u'[\ \]')
    buf = []  
    for ch in sentences: 
        buf.append(ch)
        if delimiters1.__contains__(ch):
            if len(buf)>1:
                if zhPattern.search(''.join(buf)):
                    yield ''.join(buf)  
            buf = []  
        elif delimiters2.__contains__(ch):
            if len(buf)>1:
                if zhPattern.search(''.join(buf)):
                    yield ''.join(buf)  
            buf = [] 
    if buf:  
        yield ''.join(buf)  
        
    
def rebirth_sentence(sentence):
    '''
    句子重组：结巴分词，去除停用词后句子重新组合,压缩语句
    '''  
    zhPattern =  re.compile(u'[\u4e00-\u9fa5_a-zA-Z]|[\；\，\。\！\,\.\?\？]')
    outstr = []
    if zhPattern.search(sentence):
        segs = jieba.cut(sentence,cut_all=False)
        for seg in segs:
            if zhPattern.search(seg) and seg not in stopwords:
                if len(outstr) ==0:
                    outstr.append(seg) 
                elif  len(outstr) >0 and seg !=outstr[len(outstr)-1]:
                    outstr.append(seg)
        out = "".join(outstr)
    else:
        segs = sentence.split(' ')
        for seg in segs:
            if Pattern.search(seg) and seg not in stopwords:
                if len(outstr) ==0:
                    outstr.append(seg) 
                elif  len(outstr) >0 and seg !=outstr[len(outstr)-1]:
                    outstr.append(seg)
        out = " ".join(outstr)
    return out
    
    

def get_wordslist(sentence):
    '''
    分词
    '''    
    if zhPattern.search(sentence):
        out = [] 
        segs = jieba.cut(sentence,cut_all=False)
        for seg in segs:
            if zhPattern.search(seg) and seg not in stopwords:
                    out.append(seg)
        outstr = out+Pattern.findall(sentence)
        while ' ' in outstr:
            outstr.remove(' ')
    else:
        outstr = sentence.split(' ')
        while ' ' in outstr:
            outstr.remove(' ')
    return list(set(outstr))
       
def cut_words(sentence):
    '''
    分词
    '''   
    if zhPattern.search(sentence):
        outstr = [] 
        segs = jieba.cut(sentence,cut_all=False)
        for seg in segs:
            if zhPattern.search(seg) and seg not in stopwords:
    # =============================================================================
    #             if len(outstr) ==0:
    #                 outstr.append(seg) 
    #             elif  len(outstr) >0 and seg !=outstr[len(outstr)-1]:
    # =============================================================================
                    outstr.append(seg)
        outstr = outstr+Pattern.findall(sentence)
        out = " ".join(outstr)
        
    else:
        out = " ".join(sentence.split(' '))
    return out
    #return " ".join(filter(lambda x: not stopwords.__contains__(x)and zhPattern.search(x), jieba.cut(sentence)))


def cut_down(sentence):
    '''
    去重，压缩中文语句
    '''
    outstr = [] 
    segs = jieba.cut(sentence,cut_all=False)
    for seg in segs:
        if len(outstr) ==0:
            outstr.append(seg) 
        elif  len(outstr) >0 and seg !=outstr[len(outstr)-1]:
            outstr.append(seg)
    out = "".join(outstr)
    return out

def extract_tfkeyword(text):
    """ 
    TF-IDF关键词抽取
    """  
    tfidf = analyse.extract_tags# 引入TF-IDF关键词抽取接口
    outstr = [] 
    segs = tfidf(text,topK=10,allowPOS=('l', 'a','al','ad','ag','x','uyy'))
    for seg in segs:
        if zhPattern.search(seg):
            if seg not in stopwords:
                outstr.append(seg)
    # 基于TF-IDF算法进行关键词抽取
    out = " ".join(outstr)
    return out

def extract_trkeyword(text):
    """ 
    TextRank关键词抽取
    """  
    trank = analyse.textrank# 引入TextRank关键词抽取接口
    outstr = [] 
    segs = trank(text,topK=10,allowPOS=('ns', 'n', 'vn', 'v','a','x','d','l','al'))
    for seg in segs:
        if zhPattern.search(seg):
            if seg not in stopwords:
                outstr.append(seg)
    # 基于TextRank算法进行关键词抽取
    out = " ".join(outstr)
    return out



def get_tfidfvec(docs):
    tfidf_model = TfidfVectorizer(tokenizer=cut_words,ngram_range=(1,5),min_df=3, max_df=0.9,use_idf=1,smooth_idf=1, sublinear_tf=1)
    tfidf_matrix = tfidf_model.fit_transform(docs) 
    return tfidf_matrix


def get_abstract(content):  
    """ 
    利用textrank提取摘要 
    """ 
    size=10
    docs = list(map(cut_down,sub_sentences(content)))
    if len(docs)>size:
        if zhPattern.search(content):
            tfidf_model = TfidfVectorizer(tokenizer=cut_words,ngram_range=(1,2))
            tfidf_matrix = tfidf_model.fit_transform(docs) 
            #tfidf_matrix = get_vec(docs)
            normalized_matrix = TfidfTransformer().fit_transform(tfidf_matrix)  
            similarity = nx.from_scipy_sparse_matrix(normalized_matrix * normalized_matrix.T)  
            scores = nx.pagerank(similarity)  #调用pagerank算法打分
            tops = sorted(scores.items(), key=lambda x: x[1], reverse=True)  
            size = min(size, len(docs))  
            indices = list(map(lambda x: x[0], tops))[:size]
            return "/".join(map(lambda idx: docs[idx], indices))
        
        else:
            tfidf_model = TfidfVectorizer(tokenizer=cut_words,max_df = 0.9)
            tfidf_matrix = tfidf_model.fit_transform(docs) 
            #tfidf_matrix = get_vec(docs)
            normalized_matrix = TfidfTransformer().fit_transform(tfidf_matrix)  
            similarity = nx.from_scipy_sparse_matrix(normalized_matrix * normalized_matrix.T)  
            scores = nx.pagerank(similarity)  #调用pagerank算法打分
            tops = sorted(scores.items(), key=lambda x: x[1], reverse=True)  
            size = min(size, len(docs))  
            indices = list(map(lambda x: x[0], tops))[:size]
            return "/".join(map(lambda idx: docs[idx], indices))
    else:
        return  content
     
def rinse(x):
    """
    匹配中英文和数字，删除标点符号等
    """
    zhPattern =  re.compile(u'[\u4e00-\u9fa5_a-zA-Z]+')
    return "".join(zhPattern.findall(x))



def cht_to_chs(line):
    """
    转换繁体到简体
    """
    line = conv.Converter('zh-hans').convert(line)
    line.encode('utf-8')
    return line


def get_cluster(x):   
    starttime = datetime.datetime.now()
    km = KMeans(n_clusters = 100)  
    km.fit(get_tfidfvec(x))  
    pred = km.predict(get_tfidfvec(x))
    endtime = datetime.datetime.now() 
    print('kmeans完成，耗时：{}秒'.format((endtime-starttime).seconds))
    return  pred


def data_clean(file):
    """
    数据清洗+主题词抽取
    """
    df = pd.read_csv(file)[:100]
    df['评论']=df['评论'].apply(lambda x:np.NaN if str(x).strip()=="" or rinse(str(x).strip())=="" else x)
    df = df[df['评论'].notnull()]
    df.index = np.arange(len(df))
    df2 = df.loc[:,['点评ID', '航班号', '仓位','航班日期','评论','出发地编码','目的地编码','计划出发时间','计划到达时间','实际出发时间','实际到达时间']]
    del df
    gc.collect()
    df2['评论'] = df2['评论'].apply(lambda x:x.strip().replace("\r\n",""))
    df2['评论'] = df2['评论'].apply(lambda x:cht_to_chs(x)) 
    df2['评论s'] = list( map(get_wordslist,df2['评论']))
    df2['comment'] = list( map(cut_words,df2['评论']))
    starttime = datetime.datetime.now()
    print('单程开始')
    df2['评论abstract'] = list(map(get_abstract,df2['评论']))
    endtime = datetime.datetime.now() 
    print('单程完成，耗时：{}秒'.format((endtime-starttime).seconds))
    starttime = datetime.datetime.now()
# =============================================================================
#     from multiprocessing import Pool
#     p = Pool(8)
#     df2['评论abstract'] = p.map(get_abstract,list(df2['评论'].astype(str)))
#     p.close()
#     p.join()
#     endtime = datetime.datetime.now() 
# =============================================================================
    print('主题提取并行完成，耗时：{}秒'.format((endtime-starttime).seconds))
    df2['abstract'] = list( map(cut_words,df2['评论abstract']))#提取主题并分词
# =============================================================================
#     df2['abstract_tf'] = list(map(extract_tfkeyword,df2['评论abstract']))
#     df2['abstract_tr'] = list(map(extract_trkeyword,df2['评论abstract']))
#     df2['km'] = get_cluster(list(df2['comment']))
#     df2['km'] = get_cluster(list(df2['abstract'])) 
#     df2.index  = np.arange(len(df2))
# =============================================================================
    return df2



if __name__ == "__main__":
    starttime = datetime.datetime.now()
    file = DATA_PATH + '201805_06.csv'
    df = data_clean(file)
    endtime = datetime.datetime.now() 
    print('已完成，耗时：{}秒'.format((endtime-starttime).seconds))