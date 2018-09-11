#!/home/feeyo_s_lz/anaconda3/bin/python
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  2 17:38:07 2018

@author: lz
"""
import os
import platform
if 'Windows' in platform.system():
    DATA_PATH = os.path.abspath(os.path.curdir)+os.sep+'data'+os.sep
    LTP_DATA_DIR =os.path.abspath(os.path.curdir)+os.sep+'ltp_data_v3.4.0'+os.sep # ltp模型目录的路径
else:
    DATA_PATH = os.path.abspath(os.path.dirname(__file__))+os.sep+'data'+os.sep
    LTP_DATA_DIR =os.path.abspath(os.path.dirname(__file__))+os.sep+'ltp_data_v3.4.0'+os.sep # ltp模型目录的路径
from pyltp import Segmentor
from pyltp import Postagger
from pyltp import Parser
from pyltp import NamedEntityRecognizer
from pyltp import SentenceSplitter
from pyltp import SementicRoleLabeller
# =============================================================================
# #加载分词模型
# segmentor = Segmentor()  # 初始化实例
# cws_model_path = os.path.join(LTP_DATA_DIR, 'cws.model') 
# segmentor.load_with_lexicon(cws_model_path,DATA_PATH+'newdict_add_sentimentwords.txt')
# =============================================================================
# 加载词性标注模型
postagger = Postagger() # 初始化实例
pos_model_path = os.path.join(LTP_DATA_DIR, 'pos.model') # 词性标注模型路径，模型名称为`pos.model`
postagger.load(pos_model_path)  # 加载模型
#加载句法分析模型
parser = Parser() # 初始化实例
par_model_path = os.path.join(LTP_DATA_DIR, 'parser.model') 
# 依存句法分析模型路径，模型名称为`parser.model`
parser.load(par_model_path)  # 加载模型
#使用 pyltp 进行语义角色标注
labeller = SementicRoleLabeller() # 初始化实例
if 'Windows' in platform.system():
    srl_model_path = os.path.join(LTP_DATA_DIR, 'pisrl_win.model') 
elif 'Linux' in platform.system():
    srl_model_path = os.path.join(LTP_DATA_DIR, 'pisrl.model') 
# =============================================================================
# # 语义角色标注模型目录路径，模型目录为`srl`。
# labeller.load(srl_model_path)  # 加载模型
# #加载实体标注模型
# #命名实体识别模型路径，模型名称为`pos.model`
# recognizer = NamedEntityRecognizer() # 初始化实例
# ner_model_path = os.path.join(LTP_DATA_DIR, 'ner.model') 
# recognizer.load(ner_model_path)  # 加载模型
# =============================================================================


def cut_sents(text):
    sents = SentenceSplitter.split(text)  # 分句
    return sents

def cut_words(text): 
    words = segmentor.segment(text)  # 分词
    return list(words)
    segmentor.release()  # 释放模型


def get_postag(words):
    postags = postagger.postag(words)  # 词性标注
    return postags
    postagger.release()  # 释放模型


def get_arcs(words):
    postags = get_postag(words)
    arcs = parser.parse(words, postags)  # 句法分析
    return arcs
    parser.release()  # 释放模型    

def get_parse(words):
    arcs = get_arcs(words)  # 句法分析
    #print("\t".join("%d:%s" % (arc.head,arc.relation) for arc in arcs))
    parse = [arc.relation for arc in arcs]
    head = [arc.head for arc in arcs]
    head = transform_head(head)
    return parse,head
    
    
def get_role(words):
    postags = get_postag(words)# 词性标注
    arcs = get_arcs(words)  # 句法分析
    roles = labeller.label(words, postags, arcs)  # 语义角色标注
    for role in roles:
        print (role.index, "".join(["%s:(%d,%d)" % \
                                    (arg.name, arg.range.start, arg.range.end)\
                                    for arg in role.arguments]))
    
    labeller.release()  # 释放模型

def get_recognize(words):

    postags = get_postag(words)
    netags = recognizer.recognize(words, postags)  # 命名实体识别
    return netags
    recognizer.release()  # 释放模型

def transform_head(head):
    '''
    因为ltp的语法关系是从root节点开始的，
    0A,1B变成了0root，1A,2B
    为了后期使用方便，我们可以通过一个转换，让语法关系与词表的词一一对应
    '''
    head_new = []
    for i in head:
        if i == 0:
            i=0
        else:
            i = i-1
        head_new.append(i)
    return head_new

if __name__ == '__main__':
    text = "餐食服务都不粗"
    sents = cut_sents(text)
    print(list(sents))
    for sent in sents:
        words = cut_words(sent)
        get_role(words)
        print(words)
        print(list(get_postag(words)))
        parse,head = get_parse(words)
        print(parse)
        print(head)
    
        #print(list(get_recognize(words)))
