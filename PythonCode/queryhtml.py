#   pip3 install requests bs4 -t .
#  pip install html5lib -t .

def htmlquery(event, context):
    import psycopg2
    import logging
    import traceback
    import requests
    import json  
    import imp
    import sys
    sys.modules["sqlite"] = imp.new_module("sqlite")
    sys.modules["sqlite3.dbapi2"] = imp.new_module("sqlite.dbapi2")
    import nltk
    from os import environ
    from bs4 import BeautifulSoup
    from textblob_de import TextBlobDE as TextBlob
    
  #  nltk.download('vader_lexicon')
  
    endpoint=environ.get('ENDPOINT')
    port=environ.get('PORT')
    dbuser=environ.get('DBUSER')
    password=environ.get('DBPASSWORD')
    database=environ.get('DATABASE')

    countSQL = "SELECT COUNT(*) FROM finnova.\"Articles\" a "
    countSQL = contSQL + " WHERE a.\"html\" IS NULL;"
    
    countSensitivity = "SELECT COUNT(*) FROM finnova.\"Articles\" a "
    countSensitivity = countSensitivity + " WHERE a.\"senisitivity\" IS NULL;"  
    
    querySQL ="SELECT a.\"ID\", (a.data ->> 'url'::TEXT) AS URL "
    querySQL = querySQL + " FROM finnova.\"Articles\" a WHERE a.\"html\" IS NULL;"
    
    querySensitivity = "SELECT a.\"ID\", a.\"html\" FROM finnova.\"Articles\" a"
    querySensitivity = querySensitivity + " WHERE a.\"sensitivity\" IS NULL;"
    
    updateSQL = "UPDATE finnova.\"Articles\" a SET \"html\" = '{0}' "
    updateSQL = updateSQL + " WHERE a.\"ID\" = {1};"
    
    updateSensitivity = "UPDATE finnova.\"Articles\" a SET \"sensitivity\" = '{0}'," 
    updateSensitivity = updateSensitivity + " \"polarity\" = {1} WHERE a.\"ID\" = {2};"

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
   
    def getTextFromURL(url):
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        text = ' '.join(map(lambda p: p.text, soup.find_all('p')))
        text = text.replace("'", " ") # strip out single auotes
        return text   
        
    def nltk_sentiment(sentence):
        from nltk.sentiment.vader import SentimentIntensityAnalyzer       
        nltk_sentiment = SentimentIntensityAnalyzer()
        score = nltk_sentiment.polarity_scores(sentence)
        return score    
    
    logger.info("Cold start complete.") 
    
    try:
                
        cnxQuery = make_connection()
        cursorQuery =cnxQuery.cursor()
        
        cnxUpdate = make_connection()
        cursorUpdate = cnxUpdate.cursor()
        
        try:
            # Get list of articles without html text 
            cursorQuery.execute(querySQL)                
            print("Start Num Articles without HtML: ", cursorQuery.rowcount,"\n")
		
            for article in cursorQuery:  
                # Get text from URL for current article  
                text = getTextFromURL(article[1])
                
                if  len(text) > 10  :
                    sql = updateSQL.format(text, str(article[0]) )
                    cursorUpdate.execute(sql)  

                    print("Updated HTML for article id: " +  str(article[0]))

            # Get list of articles without a senistivity score
            cursorQuery.execute(querySensitivity)                
            print("Start Num Articles without sensitivity score: ",
                  cursorQuery.rowcount,"\n")
		
            for article in cursorQuery:  
                # Get text from URL for current article  
                blob = TextBlob(str(article[1]))
                polarity = blob.polarity
                print (polarity)
                nltk_results = nltk_sentiment(article[1])                
                sql = updateSensitivity.format(json.dumps(nltk_results),
                                               polarity, str(article[0]) )
                cursorUpdate.execute(sql)  

                print("Updated sensitivity code for article id: " +  
                      str(article[0]) + " Poloarity: " + str(blob.polarity) )
        except:
            print ("Error: Article_ID " +  str(article[0]))
            print (sql)
            return log_err ("ERROR: Cannot execute cursor.\n{}".format(
                traceback.format_exc()) )

   
    except:
        return log_err("ERROR: Cannot connect to database from handler.\n{}".
                       format(traceback.format_exc()))


    finally:
        try:
            cursorQuery.execute(countSQL)
            numArticles = cursorQuery.fetchone()
            print("End Num Articles without HTML: ", numArticles,"\n")
            
            cursorQuery.execute(countSensitivity)
            numArticles = cursorQuery.fetchone()
            print("End Num Articles without sensitivity code: ", numArticles,"\n")
            			
            cursorUpdate.close()                    
            cursorQuery.close()            
        except:
            pass  
    
    
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

