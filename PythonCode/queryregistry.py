from eventregistry import *
import psycopg2
import logging
import traceback
import requests
import pprint
import json
import eventregistry
from os import environ
from datetime import datetime, timedelta

endpoint=environ.get('ENDPOINT')
port=environ.get('PORT')
dbuser=environ.get('DBUSER')
password=environ.get('DBPASSWORD')
database=environ.get('DATABASE')

countSQL = "SELECT COUNT(*) FROM finnova.\"Art\";"
querySQL = "SELECT vp.\"ID\" as ID, vp.\"searchname\" as searchname " 
querySQL = querySQL + "FROM finnova.\"V_PEP\" vp;" 
addArticleSQL ="SELECT * FROM finnova.\"addArticle\"(%s, %s);"

logger=logging.getLogger()
logger.setLevel(logging.INFO)
        
def make_connection():
    conn_str="host={0} dbname={1} user={2} password={3} port={4}".format(
        endpoint,database,dbuser,password,port)
    conn = psycopg2.connect(conn_str)
    conn.autocommit=True
    return conn 


def log_err(errmsg):
    logger.error(errmsg)
    return {"body": errmsg , "headers": {}, "statusCode": 400,
        "isBase64Encoded":"false"}

logger.info("Cold start complete.") 

try:
            
    cnxQuery = make_connection()
    cursorQuery =cnxQuery.cursor()
    
    cnxInsert = make_connection()
    cursorInsert = cnxInsert.cursor()
    
    try:
        
        er =  EventRegistry(apiKey = "YourAPIKey")  
        
        cursorQuery.execute(countSQL)
        numArticles = cursorQuery.fetchone()
        print("Start Num Articles: ", numArticles,"\n")
        
        # Get list of PEPs
        cursorQuery.execute(querySQL)
        for result in cursorQuery:  
            # Get articles for current PEP searchname
            cq1 = ComplexArticleQuery(
                CombinedQuery.AND([
                    BaseQuery(dateStart = "2019-01-01",
                             # dateEnd = "2014-04-01",
                              lang = 'deu'),
                    CombinedQuery.OR([
                        BaseQuery(keyword = QueryItems.OR(["Volkspartei",
                                                           "Grünen", 
                                                           "CVP", "SVP",
                                                           "SP", "GLP", "FDP",
                                                           "BDP", "Bundesrat" ,
                                                           "Bundesrätin",
                                                           "Ständerräte",  
                                                           "Bundeshauses",
                                                           "Reierungsrat", 
                                                           "Nationalrat",
                                                           "Sozialdemokraten"  ])  )                           
                    ]),
                    BaseQuery(keyword = result[1], 
                              exclude= BaseQuery(keyword = 
                                                 QueryItems.OR(["Füßball", 
                                                                "Bundestrainer",
                                                                "Bundesliga",
                                                                "Bayern",
                                                                "Nationalmannschaft"])))
                ])
            )
            q = QueryArticlesIter.initWithComplexQuery(cq1)
            q.setRequestedResult(RequestArticlesInfo(
                returnInfo = ReturnInfo(
                articleInfo = ArticleInfoFlags(basicInfo = True, 
                                               socialScore = True,
                                               storyUri = True, 
                                               eventUri = True,  
                                               categories = True, 
                                               location = True, 
                                               image = True, concepts = True))))

            res = er.execQuery(q)
            
            art_page = res["articles"]["page"]
            art_pages = res["articles"]["pages"]
            art_count = res["articles"]["count"]
            art_totalResults = res["articles"]["totalResults"]
            
            print('PEP_ID: ' + str(result[0]) + ' Page: ' + str(art_page) +
                  ' of ' + str(art_pages) + ' Count: ' + str(art_count) +
                  ' of ' + str(art_totalResults) + ' Total Results' )
            # Loop through articles found for the current PEP and add them to the database.
            for indx in range(art_count):
               # print ("Article %d" % (indx))                
                art = res["articles"]["results"][indx]
                artdump = json.dumps(art)
                cursorInsert.execute(addArticleSQL, (result[0],    artdump)  ) 
                messg = cursorInsert.fetchone()
                #pprint.pprint (messg)
     
        cursorQuery.execute(countSQL)
        numArticles = cursorQuery.fetchone()
        print("End Num Articles: ", numArticles, "\n")         
    except:
        print ("ERROR: Cannot execute cursor.\n{}".format(
            traceback.format_exc()) )

except:
    print("ERROR: Cannot connect to database from handler.")
    print(format_exc())

finally:
    try:
        cursorQuery.execute(countSQL)
        numArticles = cursorQuery.fetchone()
        print("End Num Articles: ", numArticles, "\n")
        			
        cursorInsert.close()                    
        cursorQuery.close()            
        cnxInsert.close()
        cnxQuery.close()
    except:
        pass  
