#!/usr/bin/python3
import argparse
import os
import sys
import time
import json
import pandas as pd
import gzip
import numpy as np

def prod_researcher():
    df_analyze['before.nullauthor_id']=df_analyze['before.author_id'].replace(np.nan,-1)
    df_analyze['after.nullauthor_id']=df_analyze['after.author_id'].replace(np.nan,-1)
    df_analyze['before.zeroauthor_id']=df_analyze['before.author_id'].replace(0,-2)
    df_analyze['after.zeroauthor_id']=df_analyze['after.author_id'].replace(0,-2)


    df_analyze['before.status']=df_analyze['before.status'].replace('ACTIVE',np.nan)
    df_analyze['before.inactive_flg']=df_analyze['before.inactive_flg'].replace('Y',np.nan)

    df_analyze['after.status']=df_analyze['after.status'].replace('ACTIVE',np.nan)
    df_analyze['after.inactive_flg']=df_analyze['after.inactive_flg'].replace('Y',np.nan)

    df_msg=df_analyze[['op','after.res_id','after.home_inst_id','after.version',
                       'after.status','after.inactive_flg',
                       'before.res_id','before.home_inst_id','before.version',
                       'before.status','before.inactive_flg',
                       'after.author_id','before.author_id',
                       'after.nullauthor_id','after.zeroauthor_id',
                       'before.nullauthor_id','before.zeroauthor_id']]
    df_msg[df_msg['op'] =='c'].isna().to_csv('OpC_Msg_R.csv')
    df_msg[df_msg['op'] =='u'].isna().to_csv('OpU_Msg_R.csv')
    df_msg[df_msg['op'] =='d'].isna().to_csv('OpD_Msg_R.csv')
    df_msg.to_csv('OpAll_Msg_R.csv')

def prod_document_set():
    df_analyze['before.inactive_flg']=df_analyze['before.inactive_flg'].replace('Y',np.nan)
    df_analyze['before.status']=df_analyze['before.status'].replace('DYNAMIC_PENDING',np.nan)
    df_analyze['before.status']=df_analyze['before.status'].replace('DYNAMIC',np.nan)
    #df_analyze['after.author_id']=df_analyze['after.author_id'].replace(0,np.nan)
    df_analyze['after.status']=df_analyze['after.status'].replace('DYNAMIC',np.nan)
    df_analyze['after.status']=df_analyze['after.status'].replace('DYNAMIC_PENDING',np.nan)
    df_analyze['after.inactive_flg']=df_analyze['after.inactive_flg'].replace('Y',np.nan)

    df_msg=df_analyze[['op','after.ds_id','after.home_inst_id','after.version',
                       'after.status','after.inactive_flg',
                       'before.ds_id','before.home_inst_id','before.version',
                       'before.status','before.inactive_flg']]
    df_msg[df_msg['op'] =='c'].isna().to_csv('OpC_Msg_DS.csv')
    df_msg[df_msg['op'] =='u'].isna().to_csv('OpU_Msg_DS.csv')
    df_msg[df_msg['op'] =='d'].isna().to_csv('OpD_Msg_DS.csv')
    df_msg.to_csv('OpAll_Msg_DS.csv')

def prod_research_area():
    df_analyze['before.inactive_flg']=df_analyze['before.inactive_flg'].replace('Y',np.nan)
    df_analyze['before.status']=df_analyze['before.status'].replace('DYNAMIC',np.nan)
    df_analyze['before.status']=df_analyze['before.status'].replace('DYNAMIC_PENDING',np.nan)
    #df_analyze['after.author_id']=df_analyze['after.author_id'].replace(0,np.nan)
    df_analyze['after.status']=df_analyze['after.status'].replace('DYNAMIC',np.nan)
    df_analyze['after.status']=df_analyze['after.status'].replace('DYNAMIC_PENDING',np.nan)
    df_analyze['after.inactive_flg']=df_analyze['after.inactive_flg'].replace('Y',np.nan)

    df_msg=df_analyze[['op','after.ra_id','after.home_inst_id','after.version',
                       'after.status','after.inactive_flg',
                       'before.ra_id',
                       'before.home_inst_id','before.version',
                       'before.status','before.inactive_flg']]
    df_msg[df_msg['op'] =='c'].isna().to_csv('OpC_Msg_RA.csv')
    df_msg[df_msg['op'] =='u'].isna().to_csv('OpU_Msg_RA.csv')
    df_msg[df_msg['op'] =='d'].isna().to_csv('OpD_Msg_RA.csv')
    df_msg.to_csv('OpAll_Msg_RA.csv')


def prod_researcher_group():
    df_analyze['before.inactive_flg']=df_analyze['before.inactive_flg'].replace('Y',np.nan)
    df_analyze['before.status']=df_analyze['before.status'].replace('DYNAMIC',np.nan)
    df_analyze['after.status']=df_analyze['after.status'].replace('DYNAMIC',np.nan)
    df_analyze['after.inactive_flg']=df_analyze['after.inactive_flg'].replace('Y',np.nan)

    df_msg=df_analyze[['op','after.rg_id','after.home_inst_id','after.version',
                       'after.status','after.inactive_flg',
                       'before.rg_id','before.home_inst_id','before.version',
                       'before.status','before.inactive_flg']]
    df_msg[df_msg['op'] =='c'].isna().to_csv('OpC_Msg_RG.csv')
    df_msg[df_msg['op'] =='u'].isna().to_csv('OpU_Msg_RG.csv')
    df_msg[df_msg['op'] =='d'].isna().to_csv('OpD_Msg_RG.csv')
    df_msg.to_csv('OpAll_Msg_RG.csv')


parser = argparse.ArgumentParser()
parser.add_argument("-t", "--topic", dest="topic", type=str , default="cert")
args = parser.parse_args()
#topic=args.topic[0]
topic=args.topic


# proc_researcher_group()
#proc_research_area()
if topic == 'all':
    df_analyze = pd.read_pickle('prod_document_set_prod.pkl')
    rpds=prod_document_set()
    df_analyze = pd.read_pickle('prod_researcher_group_prod.pkl')
    rrg=prod_researcher_group()
    df_analyze = pd.read_pickle('prod_researcher_prod.pkl')
    rr=prod_researcher()
    df_analyze = pd.read_pickle('prod_research_area_prod.pkl')
    rra=prod_research_area()

else:
    inputfile=topic+"_prod.pkl"
    df_analyze = pd.read_pickle(inputfile)
    run_proc = globals()[topic]()

# ue_query = globals()[topic]()

# with open(inputfile,'rb') as f:
# for lineraw in file1:
#     line = lineraw.rstrip()
#     count=count+1
#     # print("count",count,":",len(line),":",len(line.rstrip()),":",line)
#
#     if line.rstrip() != "null":
#         data = json.loads(line)
#         df = pd.json_normalize(data,max_level=1)
#         # pd_cr= pd.concat([pd_cr, df])
#     if count%5000 ==0 :
#         print("count:",count)
#     #
# print("pd_cr size",pd_cr.size)
# # pd_cr.to_pickle(topic+"_prod.pkl")