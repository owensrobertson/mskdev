#!/usr/bin/python3

from neo4j import GraphDatabase
import argparse
import pandas as pd

import logging
import sys

#from paramiko import SSHClient
#from sshtunnel import SSHTunnelForwarder
# from os.path import expanduser

#import botocore
#import botocore.session
# from aws_secretsmanager_caching import SecretCache, SecretCacheConfig





class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        # self.__db = sv_db

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
    # def query(self, query, parameters=None, db='scivalint'):
    def query(self, query, db):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        #print("Neo4j query def:",db,self.__user,self.__pwd,self.__uri)
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


def analyze_researcher_authors_tmp(res_df):
    if res_df.empty:
        return res_df,res_df
    #df.groupby('').groups
    #print(res_count_df[res_count_df['sv_author_id'].isna()]['mod_dt','my_res_id','my_author_id'])

    # Reset Pandas reporting display
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
    res_df_na = res_df_na.reset_index(drop=True)

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
    print("QUERY: ",authors_query_final)

    # Execute Cypher to determine existing authors in SciVal Graph
    # Note the author list is static and based upon the weekly Scopus extracts
    res_miss_auth_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(authors_query_final,db=scival_db)])
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



def prepare_ueproperty_properties(ue_property):
    # return (ue_node, ue_property, mysql_key, mysql_auth, mysql_table, mysql_key_sel, mysql_key_return,mysql_auth_ret) =  ('Researcher','researcherId','a.res_id',
    # ',a.author_id','researcher','a.res_id','row.res_id',' ,row.author_id')
    if ue_property=='researcher':
        return 'Researcher','researcherId','a.res_id',',a.author_id','researcher','a.res_id','row.res_id',' ,row.author_id'
    elif ue_property=='researcher_grp':
        return 'ResearcherGroup','researcherGroupId','a.rg_id',' ','researcher_group','a.rg_id','row.rg_id',' '
    elif ue_property=='document_set':
        return 'DocumentSet','documentSetId','a.ds_id',' ','document_set','a.ds_id','row.ds_id',' '
    elif ue_property=='research_area':
        return 'ResearchArea','researchAreaId','a.ra_id',' ','research_area','a.ra_id','row.ra_id',' '
    else:
        return None,None,None,None,None,None,None,None


def prepare_ueproperty_count(rpt_level, user_entity, mysql_uri, mysql_user, mysql_passwd):
    print(user_entity)
    (ue_node, ue_property, mysql_key, mysql_auth, mysql_table, mysql_key_sel,  mysql_key_return,mysql_auth_ret)  = \
        prepare_ueproperty_properties(user_entity)
    Q1 = '''
    MATCH(r:%s) where r.version is null or r.customerId is null  with collect(toString(r.%s)) as rIds ,
    '''%(ue_node,ue_property)
    Q2='''"jdbc:mysql://%s:3306/scival2common?user=%s&password=%s" as url
    with  '('+ apoc.text.join(rIds, ',') +')' as v1,url
    '''%(mysql_uri,mysql_user,mysql_passwd)
    Q3=''' with url, 'select %s,a.home_inst_id %s ,a.version,a.status,a.inactive_flg,
    cast(a.last_modified_ts as date) as mod_dt from %s a where %s in ' +v1 as query
    CALL apoc.load.jdbc(url,query) yield row
    return row.mod_dt,%s,row.home_inst_id,row.version %s ,row.status,row.inactive_flg;
    '''%(mysql_key,mysql_auth,mysql_table,mysql_key,mysql_key_return,mysql_auth_ret)
    query_string1= Q1+Q2+Q3

    print( query_string1)
    return query_string1

def run_ueproperty_count(ueproperty_query,db):
    # researcher_count_query=prepare_researcher_count(rpt_level,env,start_dt)
    res_count_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(ueproperty_query,db)])
    res_count_df = res_count_df.reset_index(drop=True)
    print('res_count_df',res_count_df.keys())
    #print("SHAPE-9574530: ",res_count_df[res_count_df['my_res_id'].isin([9574530])])
    logging.info("DEBUG:UE Properties: Count and Set of Null Graph Author Id(sv_author_id): %i"%(len(res_count_df)))
    logging.info('\t'+ res_count_df.head(2).to_string().replace('\n', '\n\t'))


    if not res_count_df.empty:
        # if rpt_level == 'summary': print("ERROR: Researcher identified:")
        new_dtypes = { "mod_dt": str, "my_res_id": "Int64", "sv_res_id": "Int64", "my_author_id": "Int64",
                       "sv_author_id": "Int64", "my_author_flg": "Int64",
                       "sv_author_flg": "Int64", "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version":
                           "Int64", "sv_version": "Int64" }
        #new_dtypes = { "mod_dt": str }
        res_count_df = res_count_df.reset_index(drop=True)
        #              "my_res_id": "Int64", "sv_res_id": "Int64",  \                      "my_author_flg": "Int64", \                       "sv_author_flg": "Int64", "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version": \                           "Int64", "sv_version": "Int64" }

    counter=0
    #res_count_df.to_pickle("/var/tmp/res_count_df.pkl")
    for index, row in res_count_df.iterrows():
        counter+=1
        #print("RES:",index, row['mod_dt'],row['my_res_id'],row['sv_res_id'], \
        #        row['my_author_id'],row['sv_author_id'], row['my_author_flg'],row['sv_author_flg'], \
        #        row['my_home_inst_id'],row['sv_home_inst_id'],row['my_version'],row['sv_version'])

    # print("found ",counter," error records")
    return res_count_df

    #res_count_df=analyze_researcher_authors(res_count_df)


def prepare_researcher_count(rpt_level,startDt,mysql_uri,mysql_user,mysql_passwd):
    if rpt_level == 'summary':
        where_cond = "where my_res_id <> coalesce(sv_res_id,0) or    my_home_inst_id <> coalesce(sv_home_inst_id,0) \
                or    my_version <> coalesce(sv_version,0) " 
        add_author_id= " or    my_author_id <> coalesce(sv_author_id,0)"
        add_author_flg= " or  my_author_flg <> coalesce(sv_author_flg,-1)"
    else:
        where_cond = " "
        add_author_flg = " "
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
        my_author_flg '''%(mysql_uri,mysql_user,mysql_passwd,startDt)
    query_string2 = ''' return  mod_dt,my_res_id,sv_res_id,my_author_id,sv_author_id,my_author_flg,sv_author_flg,
            my_home_inst_id,sv_home_inst_id,my_version,sv_version '''

    #print( query_string1 + where_cond + query_string2)
    return query_string1 + where_cond + add_author_flg + query_string2

def run_researcher_count(res_query,db):
    # researcher_count_query=prepare_researcher_count(rpt_level,env,start_dt)
    res_count_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(res_query,db)])
    res_count_df = res_count_df.reset_index(drop=True)
    #print('res_count_df',res_count_df.keys())
    #print("SHAPE-9574530: ",res_count_df[res_count_df['my_res_id'].isin([9574530])])

    #if 'summary' == 'summary':
    #    (res_count_df,res_count_mod)=analyze_researcher_authors(res_count_df)
        # audit_debug = logging.DEBUG if args.debug_flg =='DEBUG'  else logging.INFO
    #print('res_count_mod',res_count_mod.keys())
    #res_count_df = res_count_df.reset_index(drop=True)
    if not res_count_df.empty:
        # if rpt_level == 'summary': print("ERROR: Researcher identified:")
        new_dtypes = { "mod_dt": str, "my_res_id": "Int64", "sv_res_id": "Int64", "my_author_id": "Int64",
                       "sv_author_id": "Int64", "my_author_flg": "Int64",
                       "sv_author_flg": "Int64", "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version":
                        "Int64", "sv_version": "Int64" }
        #new_dtypes = { "mod_dt": str }
        res_count_df = res_count_df.reset_index(drop=True)
        #              "my_res_id": "Int64", "sv_res_id": "Int64",  \                      "my_author_flg": "Int64", \                       "sv_author_flg": "Int64", "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version": \                           "Int64", "sv_version": "Int64" }
        #print("DEBUG",res_count_df)
        #res_count_df = res_count_df.astype(new_dtypes)
        #print("DEBUG",res_count_df)
    counter=0
    #res_count_df.to_pickle("/var/tmp/res_count_df.pkl")
    for index, row in res_count_df.iterrows():
        counter+=1
        #print("RES:",index, row['mod_dt'],row['my_res_id'],row['sv_res_id'], \
        #        row['my_author_id'],row['sv_author_id'], row['my_author_flg'],row['sv_author_flg'], \
        #        row['my_home_inst_id'],row['sv_home_inst_id'],row['my_version'],row['sv_version'])

    # print("found ",counter," error records")
    return res_count_df

    #res_count_df=analyze_researcher_authors(res_count_df)

def analyze_result(user_entity,comp_results_df,rpt_level):
    ''' Analyze Result looks for any missing user entity key mismatch i.e. missing from graph
    the scival graph key value is expected to be in column 3 of the dataframe'''

    logging.info("ANALYZE RESULTS: %s"%(user_entity))
    # keep a copy for processing resarchers
    missing_key_df=comp_results_df
    #missing_key_df = missing_key_df.reset_index(drop=True)

    #logging.debug("DEBUG:"+'\t'+ comp_results_df.to_string().replace('\n', '\n\t'))
    # "Author Analysis",missing_key_df[missing_key_df.columns[1]].count())
    if user_entity == 'researcher':
        comp_results_df,missing_res_id_df = analyze_researcher_authors(comp_results_df)
        comp_results_df = comp_results_df.reset_index(drop=True)
        missing_res_id_df = missing_res_id_df.reset_index(drop=True)
        logging.debug("DEBUG:return: analyze researcher authors "+'\t'+ comp_results_df.to_string().replace('\n', '\n\t'))

    if missing_res_id_df.empty and rpt_level=='summary':
        print("RESULTS: Missing Researchers : No missing %s's identified"%(user_entity))
    else:
        print("RESULTS: Missing Researchers :",missing_res_id_df.to_csv())

    if comp_results_df.empty and rpt_level=='summary':
        print("RESULTS: Summary : No missing %s's identified"%(user_entity))
    elif  not (comp_results_df.empty) and rpt_level=='summary' :
        #print(comp_results_df.iloc[:,2].name)
        comp_results_df=comp_results_df[comp_results_df[comp_results_df.iloc[:,2].name].isna()]
        comp_results_df = comp_results_df.reset_index(drop=True)
        print("ERRORS: Missing %s Need Correction"%(user_entity))
        pd.set_option("display.max_rows", 1000, "display.max_columns", 7)
        print("================= %s record errors ================="%(user_entity))
        #missing_res=comp_results_df[comp_results_df['sv_res_id'].isna()]
        # comp_results_df=comp_results_df[comp_results_df['sv_author_id'].isna()]
        pd.set_option("display.max_rows", 1000, "display.max_columns", 7)
        comp_results_df.to_csv(sys.stdout)
    elif rpt_level =='detail':
        comp_results_df.to_csv(sys.stdout)

def analyze_researcher_authors(res_df):
    if res_df.empty:
        return res_df
    # Reset Pandas reporting display
    pd.set_option("display.max_rows", 1000, "display.max_columns", 100)
    #print("DUMP1:all Researcher Exceptions (bad authoers included):",res_df.to_csv())

    #logging.debug("Author Analysis Source res_df:",res_df[res_df.columns[1]].count())
    # Select all results that have an author id of NA from SciVal Graph 
    res_auth_na_df=res_df[res_df['sv_author_id'].isna()]

    res_auth_na_df = res_auth_na_df.reset_index(drop=True)
    #print("DUMP2:Missing Author Ids:",res_auth_na_df.to_csv())
    logging.debug("DEBUG:Author: Count and Set of Null Graph Author Id(sv_author_id): %i"%(len(res_auth_na_df)))
    logging.debug('\t'+ res_auth_na_df.head(2).to_string().replace('\n', '\n\t'))

    # Exclude from this subset any place the sv-res-id is NA indicating no record exists
    # print(res_auth_na_df[~res_auth_na_df['sv_res_id'].isna()].to_csv())
    # DEV 1
    res_id_na_df = res_df
    res_id_na_df = res_auth_na_df.reset_index(drop=True)
    res_id_na_df = res_id_na_df[res_id_na_df['sv_res_id'].isna()]


    res_id_na_df = res_id_na_df.reset_index(drop=True)
    # logging.debug("pandas info:"+res_id_na_df.info(verbose=False))
    #
    # print("DUMP3:Missing Reseachers:",res_id_na_df.to_csv())
    # logging.debug("DEBUG:Author: Count ad Set after Removal of Null Graph Researcher Id(sv_res_id): %i"%(len(res_auth_na_df)))
    # logging.debug('\t'+ res_auth_na_df.head(2).to_string().replace('\n', '\n\t'))
    # TEST
    #res_auth_na_df=res_auth_na_df[res_df['sv_author_id'].isnull()]

    # Reset index and level properties to default
    if res_auth_na_df.empty:
        return res_df,res_df
    res_auth_exception_list=res_auth_na_df['my_author_id'].astype(int).unique().tolist()
    logging.debug("DEBUG:Author: Count of Researchers with Missing Authors :%i"%(len(res_auth_exception_list)))
    # Create a sting based list of these exception author ids for Cypher query
    # res_auth_exception_list = res_auth_exception_list.reset_index()
    authors_list=', '.join([str(item) for item in res_auth_exception_list])
    authors_list='[' + authors_list + ']'
    # Construct Cypher query to determine which are missing Authors vs. possible CDC issues.
    authors_query1=''' with '''
    authors_query2=''' as AuthorList unwind  AuthorList as authIds 
    optional MATCH(p:Person) where p.personId = authIds with p.personId as pId,authIds
    where pId is null return distinct authIds as my_author_id'''
    authors_query_final=authors_query1+authors_list+authors_query2
    print(authors_query_final)
    logging.debug("QUERY:Researcher Authorships Valdiation : %s"%(authors_query_final))

    # Execute Cypher to determine existing authors in SciVal Graph
    # Note the author list is static and based upon the weekly Scopus extracts
    res_auth_notin_svg_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(authors_query_final,db=scival_db)])
    # print("MISSINGAUTH",res_auth_notin_svg_df)

    # Determine how many of the author ids are simply not present in SciVal Graph, 
    # indicates known exceptions
    res_miss_auth_list = res_auth_notin_svg_df['my_author_id'].astype(int).unique().tolist()
    #print("Missing Auth List",res_miss_auth_list)
    # missing_authors_ids = ', '.join([str(item) for item in res_miss_auth_df])

    count_missing_author_ids=res_auth_na_df[res_auth_na_df["my_author_id"].isin(res_miss_auth_list)].count()['my_res_id']
    logging.debug("DEBUG:Authors: Missing Entity Authors without a Scopus author-id (false positives):"+str(count_missing_author_ids))

    res_auth_na_df=res_auth_na_df[~res_auth_na_df['my_author_id'].isin(res_miss_auth_list)]
    logging.debug("DEBUG:Authors: Exceptions After Removing Entity Authors without a Scopus author-id :"+str(len(res_auth_na_df)))


    print("LOGGING:Authors: res_auth_na_df is empty") if \
         res_auth_na_df.empty  else logging.debug('\t'+ res_auth_na_df.to_string().replace('\n', '\n\t'))
    # print("SHAPE2: ",res_df[~res_df['my_author_id'].isin(res_miss_auth_list)])
    #print("SHAPE",res_df_na.shape,"      :res_df_na(After Removed vals in list)")

    res_auth_na_df = res_auth_na_df.reset_index(drop=True)
    print("shape of res_id_na_df",res_id_na_df.shape)
    return res_auth_na_df,res_id_na_df

def prepare_researcher_grp_count(rpt_level,startDt,mysql_uri,mysql_user,mysql_passwd):
    if rpt_level == 'summary':
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
    '''%(mysql_uri,mysql_user,mysql_passwd,startDt)
    # where my_rg_id <> coalesce(sv_rg_id,0)
    # or    my_home_inst_id <> coalesce(sv_home_inst_id,0)
    # or    my_version <> coalesce(sv_version,0)

    query_string2 = '''
    return my_rg_id,sv_rg_id,my_home_inst_id,sv_home_inst_id,my_version,sv_version
    '''


    #return query_string1
    return query_string1 + where_cond + query_string2
def run_researcher_grp_count(res_query,db):
    #researcher_grp_count_query=prepare_researcher_grp_count(rpt_level,env,start_dt)
    res_grp_count_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(res_query,db)])
    res_grp_count_df = res_grp_count_df.reset_index()
    if not res_grp_count_df.empty:
        #if rpt_level == 'summary': print("ERROR: Researcher Group identified:")
        new_dtypes = { "my_rg_id": "Int64", "sv_rg_id": "Int64",
                       "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version": "Int64", "sv_version": "Int64" }
        res_grp_count_df = res_grp_count_df.astype(new_dtypes)
    #else:
    #    print("No missing Researcher Groups identified: Systems in Sync on Researcher Group")
    counter=0
    #res_grp_count_df.to_pickle("/var/tmp/res_grp_count_df.pkl")
    for index, row in res_grp_count_df.iterrows():
        counter=+1
        print('MERGE(rg:ResearcherGroup {researcherGroupId:',row['my_rg_id'],'}) '
        ,'  set rg.customerId=',row['my_home_inst_id'], ' , rg.version=',row['my_version'],';' )
        # print(row)
        #fix_researcher_grp(row)
        # print("RESGRP:",index, row['my_rg_id'],row['sv_rg_id'],row['my_home_inst_id'],row['sv_home_inst_id'],row['my_version'],row['sv_version'])
    # print("found ",counter," error records")
    print(res_grp_count_df.to_csv(sys.stdout) )
    return res_grp_count_df
def fix_researcher_grp(resgrp_errors):
    print(resgrp_errors)

def prepare_document_set_count(rpt_level,startDt,mysql_uri,mysql_user,mysql_passwd):

    if rpt_level == 'summary':
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
    '''%(mysql_uri,mysql_user,mysql_passwd,startDt)
    #where my_ds_id <> coalesce(sv_ds_id,0)
    #or    my_home_inst_id <> coalesce(sv_home_inst_id,0)
    #or    my_version <> coalesce(sv_version,0)
    # print( query_string1+where_cond+query_string2)
    return query_string1+where_cond+query_string2
def run_document_set_count(res_query,db):

    #document_set_count_query=prepare_document_set_count(rpt_level,env,start_dt)
    doc_set_count_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(res_query,db)])
    doc_set_count_df = doc_set_count_df.reset_index()
    if not doc_set_count_df.empty:
        #if rpt_level == 'summary': print("ERROR: Document Set identified:")
        new_dtypes = { "my_ds_id": "Int64", "sv_ds_id": "Int64",
                       "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version": "Int64", "sv_version": "Int64" }
        doc_set_count_df = doc_set_count_df.astype(new_dtypes)
    #else:
    #    print("No missing Document Sets identified: Systems in Sync on Document Set")
    counter=0
    # doc_set_count_df.to_pickle("/var/tmp/doc_set_count.pkl")
    for index, row in doc_set_count_df.iterrows():
        counter=+1
        # print("DS:",index, row['mod_dt'],row['my_ds_id'],row['sv_ds_id'],row['my_home_inst_id'],row['sv_home_inst_id'],row['my_version'],row['sv_version'])
    # print("found ",counter," error records")
    return doc_set_count_df

def prepare_research_area_count(rpt_level,startDt,mysql_uri,mysql_user,mysql_passwd):
    if rpt_level == 'summary':
        where_cond = "where my_ra_id <> coalesce(sv_ra_id,0) or    my_home_inst_id <> coalesce(sv_home_inst_id,0) \
                or    my_version <> coalesce(sv_version,0) "
    else:
        where_cond = " "

    query_string1 = '''
    WITH "jdbc:mysql://%s:3306/scival2common?user=%s&password=%s"  as url
    CALL apoc.load.jdbc(url,"SELECT a.ra_id,a.home_inst_id,a.version,cast(a.last_modified_ts as date) as mod_dt FROM research_area a 
        WHERE cast(a.last_modified_ts as date) > '%s' AND a.inactive_flg <> 'Y' AND a.status in ('DYNAMIC','DYNAMIC_PENDING') 
        ORDER BY a.last_modified_ts") YIELD row 
    WITH row
    optional MATCH (r:ResearchArea {researchAreaId:row.ra_id})
    WITH r, row.ra_id as my_ra_id,r.researchAreaId as sv_ra_id,row.mod_dt as mod_dt,
        row.home_inst_id as my_home_inst_id,r.customerId as sv_home_inst_id,row.version as my_version,r.version as sv_version
    '''%(mysql_uri,mysql_user,mysql_passwd,startDt)
    #WHERE my_ra_id <> coalesce(sv_ra_id,0)
    #or    my_home_inst_id <> coalesce(sv_home_inst_id,0)
    #or    my_version <> coalesce(sv_version,0)
    query_string2='''
    return mod_dt,my_ra_id,sv_ra_id,my_home_inst_id,sv_home_inst_id,my_version,sv_version
    '''
    return query_string1 + where_cond + query_string2
def run_research_area_count(res_query,db):
    #research_area_count_query=prepare_research_area_count(rpt_level,env,start_dt)
    res_area_count_df = pd.DataFrame([dict(_) for _ in neo4jConn.query(res_query,db)])
    res_area_count_df = res_area_count_df.reset_index()
    if not res_area_count_df.empty:
        # if rpt_level == 'summary': print("ERROR: Research Area identified:")
        new_dtypes = { "my_ra_id": "Int64", "sv_ra_id": "Int64", "my_home_inst_id": "Int64", "sv_home_inst_id": "Int64", "my_version": "Int64", "sv_version": "Int64" }
        res_area_count_df = res_area_count_df.astype(new_dtypes)
    #else:
    #    print("No missing Research Area identified: Systems in Sync on Research Area ")
    counter=0
    #res_area_count_df.to_pickle("/var/tmp/res_area_count_df.pkl")
    for index, row in res_area_count_df.iterrows():
        counter+=1
        # print("RA:",index, row['mod_dt'],row['my_ra_id'],row['sv_ra_id'],row['my_home_inst_id'],row['sv_home_inst_id'], row['my_version'],row['sv_version'])
    return res_area_count_df
    # print("found ",counter," error records")


def environ_setup(env_setup,tunnel):
    print(env_setup)

    if env_setup == "cert":
        if tunnel:
            return 'certdb.nonprod.scival.com','scivalint','scivalkafka','test123passwd','neo4j+ssc://127.0.0.1:7687'
        else:
            return 'certdb.nonprod.scival.com','scivalint','scivalkafka','test123passwd','neo4j+s://cert-neo4j-core-bolt.hpcc-nonprod.scival.com:7687'
    elif env_setup == "prod":
        if tunnel:
            return 'proddb.scival.com','scivalprod','scivalkafka','test123passwd', \
                   'neo4j+ssc://127.0.0.1:9687'
        else:
            return 'proddb.scival.com','scivalprod','scivalkafka','test123passwd', \
        'neo4j+ssc://prod-neo4j-core-bolt.hpcc-prod.scival.com:7687'




class MyParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user-entity", dest = "audit_list", nargs='+', default = "all", help="researcher, researcher_grp, document_set, research_area")
    parser.add_argument("-a", "--audit-type", dest = "audit_type", default = "summary", help="property|summary|detail")
    parser.add_argument("-e", "--env", dest = "environ", nargs='+', default = "cert", help="cert|prod|cert tunnel")
    parser.add_argument("-d", "--start-date", dest = "last_mod_dt", default = "2022-06-01", help="2022-01-01")
    parser.add_argument("-x", "--debug", dest = "debug_flg",action='store_true', default = False, help="INFO|DEBUG")
    parser.add_argument("-db", "--schema", dest = "schema_db", default = 'scivalint', help="scivalint|scivalcert|scivalprod")
    if len(sys.argv)==1:
       parser.print_help(sys.stderr)
       sys.exit(1)
    args = parser.parse_args()
    audit_ue_list=list(args.audit_list)    
    audit_output_type=args.audit_type

    audit_start_dt=args.last_mod_dt
    # audit_debug=args.debug_flg
    audit_schema=args.schema_db
    audit_env_list=args.environ
    (audit_environ,audit_tunnel) = ( audit_env_list[0] ,True if( audit_env_list[1]=='tunnel') else False ) \
        if (len(audit_env_list) == 2 ) else (audit_env_list[0],False)


    if args.debug_flg:
        audit_debug = logging.DEBUG
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(message)s' )
        logging.debug("env"+str(audit_environ)+"::"+str(audit_tunnel))
        logging.debug("Debug Flag is :"+str(audit_debug)+"::"+str(args.debug_flg))
    else:
        audit_debug = logging.INFO
        logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(message)s' )
        logging.info("env"+str(audit_environ)+"::"+str(audit_tunnel))


    Neo4jConnection.enable_log(audit_debug, sys.stdout)

    # Establish the correct operating environment
    # Check the current host where executing.  If it is not CERT or PROD
    # then look for a localhost connection and assume CERT or Dev
    audit_environ,scival_db,username,password,environ_uri = environ_setup(audit_environ,audit_tunnel)
    if audit_schema:
        scival_db=audit_schema
        logging.debug("SCHEMA/DB:"+str(audit_schema)+'::'+str(scival_db))

    #Neo4jConnection.enable_log(logging.DEBUG, sys.stdout)

# neo4jConn = Neo4jConnection(uri="neo4j+ssc:////prod-neo4j-core-bolt.hpcc-prod.scival.com:7687", user='neo4j', pwd='initial_value')
    neo4jConn = Neo4jConnection(uri=environ_uri,     user='neo4j',  pwd='initial_value')
    # conn = Neo4jConnection(uri="neo4j+ssc://127.0.0.1:7687",
    # print("neo4jConn:",neo4jConn)
    if ('all' in audit_ue_list) :
        audit_ue_list=['researcher','researcher_grp','document_set','research_area']

    if audit_output_type == 'property':
        for user_entities in audit_ue_list:
            logging.info("PROPERTY AUDIT"+user_entities)
            property_query = prepare_ueproperty_count(audit_output_type, user_entities, audit_environ, 'scivalkafka','test123passwd')
            x=run_ueproperty_count(property_query,scival_db)

        #ue_query=prepare_ueproperty_count(audit_output_type,'researcher',audit_environ,'scivalkafka','test123passwd')
        #run_ueproperty_count(ue_query,scival_db)
    if audit_output_type in ['summary','detail']:
        for user_entities in audit_ue_list:
            # print("args list ",user_entities)
            ue_query=globals()['prepare_' + user_entities + '_count'](audit_output_type,audit_start_dt,audit_environ,
                                                                      'scivalkafka','test123passwd')
            # print("ue_query:",ue_query)
            result_df=globals()['run_' + user_entities + '_count'](ue_query,scival_db)
            print(user_entities,"results empty") if result_df.empty else \
                analyze_result(user_entities,result_df,audit_output_type)

    neo4jConn.close()


    # Feature List
    # 1) COMPLETE Add schema specific parameter ; this allows for non default scivalint/scivalprod or other neo4j aliases
    # use-case will be focussed on validating new weekly constructed scival graph instance.
    # 2) Corrective updates on Researcher fixes for the below
    #   a) hollow Researcher - when there is only a reseacherId field
    #   b) missing Author - situation where researcher includes
    # 3) Hollow Researcher - show where researcher has no version/home-inst-id or possibly 0 docs
    # 4) Add Schema validation functions
    #   a) check all nodes have primary key indexes
    #   b) check for nodes with missing keys/properties
    #   c) Add Relationship validations
    # 5) Extra UE's created since no filter exists on the Document_Set for pending completes.
    # this is growing the count on the document to Work relationships; this resets weekly but
    # for an incremental environment this should be further evaluated.
# Graph Rel Demographics
#CALL db.stats.retrieve('GRAPH COUNTS') YIELD data
#UNWIND [data IN data.relationships
#WHERE  exists(data.startLabel) or  exists(data.endLabel) ] as relCount
#RETURN max(relCount.startLabel),max(relCount.endLabel),coalesce(relCount.relationshipType, 'all') as relationshipType, relCount.count as count order by count;
