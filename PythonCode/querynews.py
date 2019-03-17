def newsquery(event, context):
    import psycopg2
    import logging
    import traceback
    import requests
    import pprint
    import json
    from os import environ
    from datetime import datetime, timedelta

    # These parameters are loaded into the 
   #  into the environment variables with the 
   #  Serverless Framework YAMIL file.   
    endpoint=environ.get('ENDPOINT')
    port=environ.get('PORT')
    dbuser=environ.get('DBUSER')
    password=environ.get('DBPASSWORD')
    database=environ.get('DATABASE')

    countSQL = "SELECT COUNT(*) FROM finnova.\"Articles\";"
    querySQL = "SELECT vp.\"ID\" as ID, vp.\"searchname\" as searchname " 
    querySQL = querySQL + " FROM finnova.\"V_PEP\" vp WHERE vp.\"Language\" = 'd';"


    insertSQL = "INSERT INTO finnova.\"Articles\ " 
    insertSQL = insertSQL +  " VALUES (DEFAULT, %s, %s) ON CONFLICT DO NOTHING;"

    qFilter = 'AND ("SP" OR "GLP" OR "CVP" OR "FDP" OR "BDP" OR "Bundesrat" OR "Bundesrätin" '
    qFilter = qFilter + ' OR "Ständerräte" OR "Bundeshauses" OR "Reierungsrat" OR "Wahl" OR '
    qFilter = qFilter + ' "Wahlen" OR "Parlament" OR "Nationalrat" OR "Sozialdemokraten")'
    qFilter = qFilter + ' AND -"Füßball" AND -"Bayern" AND -"Bundestrainer"&'

	#NewsAPI.org string elements
    https = 'https://newsapi.org/v2/everything?'
    language = 'language=de&'  # Take only German articles
	# set time span for 3 days in the past to cover any articles that were added late.
    date = 'from=' + (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d&')
	# Take the max page size of 100
    pagesize =  'pagesize=100&'
    apiKey =    'apiKey=YourNewsAPIKey'

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
            cursorQuery.execute(countSQL)
            numArticles = cursorQuery.fetchone()
            print("Start Num Articles: ", numArticles,"\n")
    			
            # Get list of PEPs
            cursorQuery.execute(querySQL)
            for result in cursorQuery:  
                # Get articles for current PEP searchname
                q = 'q=\"%s\" ' % result[1]
                q = q + qFilter
                url = (https + q + language + date + pagesize + apiKey)    
                response = requests.get(url)
                if  response.json()['status'] == 'ok' :
                    article_list = response.json()['articles']
                    print('PEP: ' + result[1] + '   TotalResults: '
                          + str(response.json()['totalResults']) ) 
                    for article in article_list:
		 # Insert current PEP_ID and articles JSON for current search name into DB
                        cursorInsert.execute(insertSQL, (str(result[0]),
                                                         json.dumps(article) ) )                         
                else :# Not okay, log error
                    logger.error(response.json())                   

        except:
            return log_err ("ERROR: Cannot execute cursor.\n{}".format(
                traceback.format_exc()) )

   
    except:
        return log_err("ERROR: Cannot connect to database from handler.\n{}".format(
            traceback.format_exc()))


    finally:
        try:
            cursorQuery.execute(countSQL)
            numArticles = cursorQuery.fetchone()
            print("End Num Articles: ", numArticles,"\n")
            			
            cursorInsert.close()                    
            cursorQuery.close()            
            cnxInsert.close()
            cnxQuery.close()
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


