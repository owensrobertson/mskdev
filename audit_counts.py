#!/usr/bin/python3

from neo4j import GraphDatabase
import argparse
import pandas as pd
import pymysql
import logging
import sys
import json
#from paramiko import SSHClient
#from sshtunnel import SSHTunnelForwarder
from os.path import expanduser

import botocore
import botocore.session
# from aws_secretsmanager_caching import SecretCache, SecretCacheConfig





class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd

        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    @staticmethod
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getLogger("neo4j").addHandler(handler)
        logging.getLogger("neo4j").setLevel(level)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db='scivalint'):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database='scivalint') if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


class MySQL_Conn():



    # def __init__(self,tunnel):
    def __init__(self):
        # self.tunnel = tunnel
        # self.__user = user
        # self.__pwd = pwd
        # self.qry = input_qry
        # self.cursor = None

        # sql_hostname = 'localhost'
        self.sql_username = 'scivalkafka'
        self.sql_password = 'test123passwd'
        self.sql_main_database = 'scival2common'
        self.sql_port = 3306

        # print(self.sql_username)
        #conn = pymysql.connect(host='certdb.nonprod.scival.com', user=self.sql_username,
        conn = pymysql.connect(host='certdb.nonprod.scival.com', user=self.sql_username,
                                        passwd=self.sql_password, db=self.sql_main_database, port=3306)
        #conn = pymysql.connect(host='certdb.nonprod.scival.com', user=self.sql_username,
        #                                passwd=self.sql_password, db=self.sql_main_database,
        #                                port=tunnel.local_bind_port)
        curs = conn.cursor()
        print("MySQL Connect curs:",curs)
        exe = curs.execute('select 4321')
        print("exe:", exe)
        data = curs.fetchone()
        print("data",data)
        #return conn

    # def __connect__(self):
    #     with SSHTunnelForwarder( (self.ssh_host, self.ssh_port),
    #             ssh_username=self.ssh_user, ssh_pkey=self.mypkey,
    #             remote_bind_address=(self.sql_hostname, self.sql_port)) as tunnel:
    #         self.conn = pymysql.connect(host='127.0.0.1', user=self.sql_username,
    #                                passwd=self.sql_password, db=self.sql_main_database,
    #                                port=tunnel.local_bind_port)
    #         query = '''SELECT database();'''
    #         print("connect query",query)
    #         data = pd.read_sql_query(query, self.conn)
    #         print(data)
    #         return self.conn

    def query(self):
        print("query block:")

        self.conn2 = self.conn  # self.__connect__()
        # print("query conn", query_conn)
        self.curs2 = self.conn2.cursor()

        print("cur:", self.curs2)
        exe = self.curs.execute('select 1234')
        print("exe:", exe)
        output = self.curs.fetchall()
        print("output", output)




def neo4j_creds():
    client = botocore.session.get_session().create_client('secretsmanager')
    cache_config = SecretCacheConfig()
    cache = SecretCache(config=cache_config, client=client)
    secret = cache.get_secret_string('scival_bigdata_neo4j_admin')
    neo4j_secret = json.loads(secret)
    return neo4j_secret


def mysql_creds():
    client = botocore.session.get_session().create_client('secretsmanager')
    cache_config = SecretCacheConfig()
    cache = SecretCache(config=cache_config, client=client)
    secret = cache.get_secret_string('scival-bigdata-mysql-msk')
    mysql_secret = json.loads(secret)
    return mysql_secret


def prepare_audit_sql():
    OFFSET_DAYS = " 10 "
    RES_AUDIT = "SELECT (CASE WHEN (ISNULL(b.doc_id) OR (a.status <> 'ACTIVE') OR (a.inactive_flg <> 'N')  ) \
    THEN 'INVALID' ELSE 'VALID' END) AS VALID, a.res_id AS res_id, a.web_user_id AS web_user_id, \
    a.version AS version, a.home_inst_id ,a.name AS name, cast(a.last_modified_ts as date) AS last_modified_ts, \
     a.request_id AS request_id, COALESCE(a.author_id, 0) AS author_id, a.status AS status, \
     a.inactive_flg AS testinactive_flg, b.doc_id AS doc_id, b.author_nbr AS author_nbr, \
     b.selected AS selected FROM researcher a LEFT JOIN researcher_authorships b ON (a.res_id = b.res_id) \
     WHERE a.status = 'ACTIVE' and ((a.inactive_flg <> 'Y') AND (b.selected <> 0) \
     AND (a.last_modified_ts > (NOW() - INTERVAL " + OFFSET_DAYS + " DAY))   ) "

    AUDIT_QRY = "select cast(last_modified_ts as date) as mod_dt,VALID,res_id,version, home_inst_id,case WHEN \
    (author_id is not null AND author_id <> 0) THEN author_id ELSE 0 END author_id,status, \
    count(distinct doc_id) as doc_count from ( " + RES_AUDIT + ")  XX where selected=1  \
    group by cast(last_modified_ts as date),VALID,res_id,version,home_inst_id, \
    case WHEN (author_id is not null AND author_id <> 0) THEN 1 ELSE 0 END , status"
    print(AUDIT_QRY)

def start_tunnel():
    ssh_host = 'bastion.nonprod.scival.com'
    ssh_user = 'ec2-user'
    mypkey = "/Users/robertsono/.ssh/scival-nonProd.pem"
    sql_hostname = 'certdb.nonprod.scival.com'
    ssh_port = 22
    sql_port = 3306
    sql_ip = '1.1.1.1.1'
    tunnel = SSHTunnelForwarder((ssh_host, ssh_port),
                                ssh_username=ssh_user, ssh_pkey=mypkey,
                                remote_bind_address=(sql_hostname, sql_port))
    tunnel.start()
    return tunnel

def prepare_researcher_count(output,env,startDt):
    if output == 'summary':
        where_cond = "where my_res_id <> coalesce(sv_res_id,0) or    my_home_inst_id <> coalesce(sv_home_inst_id,0) \
                or    my_version <> coalesce(sv_version,0) " 
        add_author_id= " or    my_author_id <> coalesce(sv_author_id,0)"
        add_author_flg= " or  my_author_flg <> coalesce(sv_author_flg,-1)"
    else:
        where_cond = " "

    query_string1 = '''
    WITH "jdbc:mysql://%s:3306/scival2common?user=%s&password=%s"  as url
    CALL apoc.load.jdbc(url,"select a.res_id,a.home_inst_id,a.author_id,a.version,cast(a.last_modified_ts as date) as mod_dt from researcher a 
    where cast(a.last_modified_ts as date) > '%s' and a.inactive_flg <> 'Y' order by a.last_modified_ts"
    ) YIELD row 
    WITH row 
    optional MATCH (r:Researcher {researcherId:row.res_id} )
    optional MATCH(r)-[:ALIAS]-(p:Person) 
    with  p.personId as sv_author_id ,r, row.res_id as my_res_id,r.researcherId as sv_res_id, row.mod_dt as mod_dt,
        row.home_inst_id as my_home_inst_id,r.customerId as sv_home_inst_id,row.version as my_version,r.version as sv_version,
        row.author_id as my_author_id, 
        case when p.personId <> 0 and p.personId is not null then 1 else 0 end as sv_author_flg,
        case when row.author_id <> 0 and 
        row.author_id is not null then 1 else 0 end as 
        my_author_flg '''%(env,'scivalkafka','test123passwd',startDt)
    query_string2 = ''' return mod_dt,my_res_id,sv_res_id,my_author_id,sv_author_id,my_author_flg,sv_author_flg,
            my_home_inst_id,sv_home_inst_id,my_version,sv_version '''

    #print( query_string1 + where_cond + query_string2)
    return query_string1 + where_cond + add_author_flg + query_string2


def prepare_researcher_grp_count(output,env,startDt):
    if output == 'summary':
        where_cond = "where my_rg_id <> coalesce(sv_rg_id,0) or    my_home_inst_id <> coalesce(sv_home_inst_id,0) \
                or    my_version <> coalesce(sv_version,0) "
    else:
        where_cond = " "

    query_string1 = '''
    WITH "jdbc:mysql://%s:3306/scival2common?user=%s&password=%s"  as url
    CALL apoc.load.jdbc(url,"select a.rg_id,a.home_inst_id,a.version,cast(a.last_modified_ts as date) as mod_dt 
    from researcher_group a 
    where cast(a.last_modified_ts as date) > '%s' and a.inactive_flg <> 'Y' 
    and a.status in ('DYNAMIC')order by a.last_modified_ts"
    ) YIELD row 
    WITH row 
    optional MATCH (r:ResearcherGroup {researcherGroupId:row.rg_id} )
    with  r, row.rg_id as my_rg_id,r.researcherGroupId as sv_rg_id,
        row.home_inst_id as my_home_inst_id,r.customerId as sv_home_inst_id,
        row.version as my_version,r.version as sv_version
    '''%(env,'scivalkafka','test123passwd',startDt)
    # where my_rg_id <> coalesce(sv_rg_id,0)
    # or    my_home_inst_id <> coalesce(sv_home_inst_id,0)
    # or    my_version <> coalesce(sv_version,0)

    query_string2 = '''
    return my_rg_id,sv_rg_id,my_home_inst_id,sv_home_inst_id,my_version,sv_version
    ''' 


    #return query_string1
    return query_string1 + where_cond + query_string2



def prepare_document_set_count(output,env,startDt):

    if output == 'summary':
      where_cond = ''' where my_ds_id <> coalesce(sv_ds_id,0) or    my_home_inst_id <> coalesce(sv_home_inst_id,0) 
      or    my_version <> coalesce(sv_version,0) '''
      query_string2='''
      return mod_dt,my_ds_id,sv_ds_id,my_home_inst_id,sv_home_inst_id,my_version,sv_version
      ''' 
    else:
      where_cond = " "
      query_string2='''
      return mod_dt,my_ds_id,sv_ds_id,my_home_inst_id,sv_home_inst_id,my_version,sv_version
      ''' 
    query_string1 = '''
    WITH "jdbc:mysql://%s:3306/scival2common?user=%s&password=%s"  as url
    CALL apoc.load.jdbc(url,"select cast(a.last_modified_ts as date) as mod_dt,a.ds_id,a.home_inst_id,a.version,
    cast(a.last_modified_ts as date) as mod_dt from document_set a 
    where cast(a.last_modified_ts as date) > '%s' and a.inactive_flg <> 'Y' 
    and a.status in ('DYNAMIC','DYNAMIC_PENDING') order by a.last_modified_ts"
    ) YIELD row 
    WITH row 
    optional MATCH (r:DocumentSet {documentSetId:row.ds_id} )
    with  r, row.ds_id as my_ds_id,r.documentSetId as sv_ds_id,row.mod_dt as mod_dt,
        row.home_inst_id as my_home_inst_id,r.customerId as sv_home_inst_id,row.version 
        as my_version,r.version as sv_version
    '''%(env,'scivalkafka','test123passwd',startDt)
    #where my_ds_id <> coalesce(sv_ds_id,0)
    #or    my_home_inst_id <> coalesce(sv_home_inst_id,0)
    #or    my_version <> coalesce(sv_version,0)
    # print( query_string1+where_cond+query_string2)
    return query_string1+where_cond+query_string2

def prepare_research_area_count(output,env,startDt):
    if output == 'summary':
        where_cond = "where my_ra_id <> coalesce(sv_ra_id,0) or    my_home_inst_id <> coalesce(sv_home_inst_id,0) \
                or    my_version <> coalesce(sv_version,0) "
    else:
        where_cond = " "

    query_string1 = '''
    WITH "jdbc:mysql://%s:3306/scival2common?user=%s&password=%s"  as url
    CALL apoc.load.jdbc(url,"SELECT a.ra_id,a.home_inst_id,a.version,cast(a.last_modified_ts as date) as mod_dt FROM research_area a 
        WHERE cast(a.last_modified_ts as date) > '2021-01-01' AND a.inactive_flg <> 'Y' AND a.status in ('DYNAMIC','DYNAMIC_PENDING') 
        ORDER BY a.last_modified_ts") YIELD row 
    WITH row
    optional MATCH (r:ResearchArea {researchAreaId:row.ra_id})
    WITH r, row.ra_id as my_ra_id,r.researchAreaId as sv_ra_id,row.mod_dt as mod_dt,
        row.home_inst_id as my_home_inst_id,r.customerId as sv_home_inst_id,row.version as my_version,r.version as sv_version
    '''%(env,'scivalkafka','test123passwd',startDt)
    #WHERE my_ra_id <> coalesce(sv_ra_id,0)
    #or    my_home_inst_id <> coalesce(sv_home_inst_id,0)
    #or    my_version <> coalesce(sv_version,0)
    query_string2='''
    return mod_dt,my_ra_id,sv_ra_id,my_home_inst_id,sv_home_inst_id,my_version,sv_version
    ''' 
    return query_string1 + where_cond + query_string2


def run_researcher_count(output,env,start_dt):
    researcher_count_query=prepare_researcher_count(output,env,start_dt)
    #print(neo4jConn.query(query_string))
    res_count_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(researcher_count_query)])
    res_count_df = res_count_df.reset_index(drop=True)
    #print('res_count_df',res_count_df.keys())
    #print("SHAPE-9574530: ",res_count_df[res_count_df['my_res_id'].isin([9574530])])
    (res_count_df,res_count_mod)=analyze_researcher_authors(res_count_df)
    #print('res_count_mod',res_count_mod.keys())
    #res_count_df = res_count_df.reset_index(drop=True)
    if not res_count_df.empty:
        if output == 'summary': print("ERROR: Researcher identified:") 
        new_dtypes = { "mod_dt": str, "my_res_id": "Int64", "sv_res_id": "Int64", "my_author_id": "Int64", "sv_author_id": "Int64", "my_author_flg": "Int64", \
            "sv_author_flg": "Int64", "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version": "Int64", "sv_version": "Int64" }
        res_count_df = res_count_df.astype(new_dtypes)
    counter=0
    #res_count_df.to_pickle("/var/tmp/res_count_df.pkl")
    for index, row in res_count_df.iterrows():
        counter+=1
        #print("RES:",index, row['mod_dt'],row['my_res_id'],row['sv_res_id'], \
        #        row['my_author_id'],row['sv_author_id'], row['my_author_flg'],row['sv_author_flg'], \
        #        row['my_home_inst_id'],row['sv_home_inst_id'],row['my_version'],row['sv_version'])

    # print("found ",counter," error records")

    analyze_researcher(res_count_df)
    #res_count_df=analyze_researcher_authors(res_count_df)

def analyze_researcher(res_df):
    if res_df.empty:
        print("No missing Researchers identified: Systems in Sync on Researcher")
    else:
        print("MISSING Researchers Need Correction::")
        missing_res=res_df=res_df[res_df['sv_res_id'].isna()]
        print(missing_res)



def analyze_researcher_authors(res_df):
    if res_df.empty:
        return res_df,res_df
    #df.groupby('').groups
    #print(res_count_df[res_count_df['sv_author_id'].isna()]['mod_dt','my_res_id','my_author_id'])

    # Reset Pandas reporting output
    pd.set_option("display.max_rows", 1000, "display.max_columns", 100)

    #res_df =res_df[~res_df['sv_author_id'].isnull()] 
    #res_df = res_df[~res_df['my_res_id'].isin([9574530])]
    #res_df = res_df.reset_index(drop=True)
    # print("1-SHAPE-9574530: ",res_df[res_df['my_res_id'].isin([9574530])])
    #print("SHAPE",res_df.shape,"      :res_df")
    res_df_save=res_df
    # Select all results that have an author id of NA from SciVal Graph 
    #res_df = res_df.reset_index(drop=True)
    res_df_na=res_df[res_df['sv_author_id'].isna()] 
    res_df_na = res_df_na.reset_index(drop=True)

    #res_df_na=res_df[res_df['sv_author_id'].isna()] 
    #res_df_na=res_df_na[res_df['sv_author_id'].isnull()] 

    
    #print("2-SHAPE-9574530: ",res_df[res_df['my_res_id'].isin([9574530])])

    #print("SHAPE",res_df_na.shape,"      :res_df_na(sv-author-id present)")

    # Exclude from this subset any place the sv-res-id is NA indicating no record exists
    res_df_na=res_df_na[~res_df['sv_res_id'].isnull()]
    # TEST
    res_df_na=res_df_na[res_df['sv_author_id'].isnull()]
    #print("SHAPE",res_df_na.shape,"      :res_df_na(remove res-id missing)")

    # Reset index and level properties to default
    res_df_na = res_df_na.reset_index()

    # print("3-SHAPE-9574530: ",res_df_na[res_df_na['my_res_id'].isin([9574530])])

    # Extract unique author-ids from subset of missing author-ids and place into an integer list
    #print("===============================================")
    #print(res_df_na[["sv_res_id","my_author_id","sv_author_id"]])
    #print("===============================================")
    #print("===============================================")
    #print(res_df_na[["my_author_id"]])
    #print("===============================================")
    res_auth_exception_list=res_df_na['my_author_id'].astype(int).unique().tolist()

    # Create a sting based list of these exception author ids for Cypher query
    # res_auth_exception_list = res_auth_exception_list.reset_index()
    authors_list=', '.join([str(item) for item in res_auth_exception_list])
    #size=len(authors_id)
    authors_list='[' + authors_list + ']'
    # print(res_auth_exception_list)
    #print("AUTHORS_LIST",authors_list)

    # Construct Cypher query to determine which are missing Authors vs. possible CDC issues.
    authors_query1=''' with '''
    authors_query2=''' as AuthorList unwind  AuthorList as authIds 
    optional MATCH(p:Person) where p.personId = authIds with p.personId as pId,authIds
    where pId is null return authIds as my_author_id'''
    authors_query_final=authors_query1+authors_list+authors_query2
    #print("QUERY: ",authors_query_final)

    # Execute Cypher to determine existing authors in SciVal Graph
    # Note the author list is static and based upon the weekly Scopus extracts
    res_miss_auth_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(authors_query_final)])
    # print("MISSINGAUTH",res_miss_auth_df)

    #print("SHAPE",res_miss_auth_df.shape,"      :res_miss_auth_df(List of Auths in SV Graph)")

    # Determine how many of the author ids are simply not present in SciVal Graph, 
    # indicates known exceptions
    res_miss_auth_list = res_miss_auth_df['my_author_id'].astype(int).unique().tolist()
    #print("Missing Auth List",res_miss_auth_list)
    # missing_authors_ids = ', '.join([str(item) for item in res_miss_auth_df])

    count_missing_author_ids=res_df[res_df["my_author_id"].isin(res_miss_auth_list)].count()['my_res_id']
    print("Found Missing Author Ids (false positives):",count_missing_author_ids)
    res_df_na=res_df[~res_df['my_author_id'].isin(res_miss_auth_list)]
    # print("SHAPE2: ",res_df[~res_df['my_author_id'].isin(res_miss_auth_list)])
    #print("SHAPE",res_df_na.shape,"      :res_df_na(After Removed vals in list)")

    res_df_na = res_df_na.reset_index(drop=True)

    print("SHAPE",res_df_na[['mod_dt','my_res_id','my_author_id','sv_res_id','sv_author_id']])
    #print("type:",type(res_miss_auth_list))

    #x=res_auth_exception_list["my_author_id.tolist()

    # print("SHAPE",res_df_na.shape,"      :res_df_na(After Removed vals in list)")
    #print("SHAPE",res_df_na.keys)
    return res_df_na,res_df_save

    

def run_researcher_grp_count(output,env,start_dt):
    researcher_grp_count_query=prepare_researcher_grp_count(output,env,start_dt)
    res_grp_count_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(researcher_grp_count_query)])
    res_grp_count_df = res_grp_count_df.reset_index()
    if not res_grp_count_df.empty:
        if output == 'summary': print("ERROR: Researcher Group identified:") 
        new_dtypes = { "my_rg_id": "Int64", "sv_rg_id": "Int64", \
            "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version": "Int64", "sv_version": "Int64" }
        res_grp_count_df = res_grp_count_df.astype(new_dtypes)
    else:
        print("No missing Researcher Groups identified: Systems in Sync on Researcher Group")
    counter=0
    #res_grp_count_df.to_pickle("/var/tmp/res_grp_count_df.pkl")
    for index, row in res_grp_count_df.iterrows():
        counter=+1
        print("RESGRP:",index, row['my_rg_id'],row['sv_rg_id'],row['my_home_inst_id'],row['sv_home_inst_id'],row['my_version'],row['sv_version'])
    # print("found ",counter," error records")
    

def run_document_set_count(output,env,start_dt):

    document_set_count_query=prepare_document_set_count(output,env,start_dt)
    doc_set_count_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(document_set_count_query)])
    doc_set_count_df = doc_set_count_df.reset_index()
    if not doc_set_count_df.empty:
        if output == 'summary': print("ERROR: Document Set identified:") 
        new_dtypes = { "my_ds_id": "Int64", "sv_ds_id": "Int64",  \
            "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version": "Int64", "sv_version": "Int64" }
        doc_set_count_df = doc_set_count_df.astype(new_dtypes)
    else:
        print("No missing Document Sets identified: Systems in Sync on Document Set")
    counter=0
    # doc_set_count_df.to_pickle("/var/tmp/doc_set_count.pkl")
    for index, row in doc_set_count_df.iterrows():
        counter=+1
        print("DS:",index, row['mod_dt'],row['my_ds_id'],row['sv_ds_id'],row['my_home_inst_id'],row['sv_home_inst_id'],row['my_version'],row['sv_version'])
    # print("found ",counter," error records")
    
def run_research_area_count(output,env,start_dt):
    research_area_count_query=prepare_research_area_count(output,env,start_dt)
    res_area_count_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(research_area_count_query)])
    res_area_count_df = res_area_count_df.reset_index()
    if not res_area_count_df.empty:
        if output == 'summary': print("ERROR: Research Area identified:") 
        new_dtypes = { "my_ra_id": "Int64", "sv_ra_id": "Int64", "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version": "Int64", "sv_version": "Int64" }
        res_area_count_df = res_area_count_df.astype(new_dtypes)
    else:
        print("No missing Research Area identified: Systems in Sync on Research Area ")
    counter=0
    #res_area_count_df.to_pickle("/var/tmp/res_area_count_df.pkl")
    for index, row in res_area_count_df.iterrows():
        counter+=1
        print("RA:",index, row['mod_dt'],row['my_ra_id'],row['sv_ra_id'],row['my_home_inst_id'],row['sv_home_inst_id'], row['my_version'],row['sv_version'])
               
    # print("found ",counter," error records")
    


class MyParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)





if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user-entity", dest = "audit_list", nargs='+', default = "all", help="researcher, researcher_grp, document_set, research_area")
    parser.add_argument("-a", "--audit-type", dest = "audit_type", default = "summary", help="summary|detail")
    parser.add_argument("-f", "--fix", dest = "xxx", default = "summary", help="summary|detail")
    parser.add_argument("-e", "--env", dest = "environ", default = "cert", help="local|cert|prod")
    parser.add_argument("-d", "--start-date", dest = "last_mod_dt", default = "2022-06-01", help="2022-01-01")
    if len(sys.argv)==1:
       parser.print_help(sys.stderr)
       sys.exit(1)
    args = parser.parse_args()
    audit_ue_list=list(args.audit_list)    
    audit_output_type=args.audit_type
    audit_environ=args.environ
    audit_start_dt=args.last_mod_dt
    #result = x if a > b else y

    # Establish the correct operating environment
    # Check the current host where executing.  If it is not CERT or PROD
    # then look for a localhost connection and assume CERT or Dev
    

    Neo4jConnection.enable_log(logging.INFO, sys.stdout)
    # neo4jConn = Neo4jConnection(uri="neo4j+ssc:////prod-neo4j-core-bolt.hpcc-prod.scival.com:7687", user='neo4j', pwd='initial_value')
    neo4jConn = Neo4jConnection(uri="neo4j+ssc://127.0.0.1:7687",     user='neo4j',  pwd='initial_value')
    # conn = Neo4jConnection(uri="neo4j+ssc://127.0.0.1:7687",
    print("neo4jConn:",neo4jConn)
    print("Connection Established")
    if ('all' in audit_ue_list) :
        audit_ue_list=['researcher','researcher_grp','document_set','research_area']

    for user_entities in audit_ue_list:
        # print("args list ",user_entities)
        globals()['run_' + user_entities + '_count'](audit_output_type,audit_environ,audit_start_dt)


    neo4jConn.close()

# pd.set_option('display.max_rows', 200)
# pd.set_option('display.max_columns', 5)
# pd.set_option('display.max_colwidth', 200)
# pd.set_option('display.float_format',  '{:,.0f}'.format)




