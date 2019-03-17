#   pip3 install requests bs4 -t .
#  pip install html5lib -t .


import psycopg2
import logging
import traceback
import json  
import imp
import sys
sys.modules["sqlite"] = imp.new_module("sqlite")
sys.modules["sqlite3.dbapi2"] = imp.new_module("sqlite.dbapi2")
from os import environ

from textblob_de import TextBlobDE as TextBlob

  #  nltk.download('vader_lexicon')
endpoint=environ.get('ENDPOINT')
port=environ.get('PORT')
dbuser=environ.get('DBUSER')
password=environ.get('DBPASSWORD')
database=environ.get('DATABASE')

countSQL = "SELECT COUNT(*) FROM finnova.\"Art\" a WHERE a.\"polarity\" IS NULL;"
queryPolarity = "SELECT a.\"ID\", a.\"body\" FROM finnova.\"Art\" a WHERE a.\"polarity\" IS NULL;"
updatePolarity = "UPDATE finnova.\"Art\" a SET \"polarity\" = {0} WHERE a.\"ID\" = {1};"

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
    
    cnxUpdate = make_connection()
    cursorUpdate = cnxUpdate.cursor()
    
    try:
        # Get list of articles without a polarity score
        cursorQuery.execute(queryPolarity)                
        print("Start Num Articles without a polarity score: ", cursorQuery.rowcount,"\n")
		
        for article in cursorQuery:  
            # Get text body for current article  
            blob = TextBlob(str(article[1]))
            polarity = blob.polarity
            print (polarity)            
            sql = updatePolarity.format(polarity, str(article[0]) )
            cursorUpdate.execute(sql)  

            print("Updated polarity code for article id: " +  str(article[0]) + " Poloarity: " + str(blob.polarity) )
    except:
        print ("Error: Article_ID " +  str(article[0]))
        print (sql)
        log_err ("ERROR: Cannot execute cursor.\n{}".format(
            traceback.format_exc()) )

   
except:
    log_err("ERROR: Cannot connect to database from handler.\n{}".format(
        traceback.format_exc()))

finally:
    try:
        cursorQuery.execute(countSQL)
        numArticles = cursorQuery.fetchone()
        print("End Num Articles without polarity: ", numArticles,"\n")
                  			
        cursorUpdate.close()                    
        cursorQuery.close()            
    except:
        pass  



