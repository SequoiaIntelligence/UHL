CREATE OR REPLACE FUNCTION finnova."addArticle"("PEP_ID" integer, "ArticleJSON" jsonb)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
   retMessg     CHARACTER VARYING;
   errMessg     CHARACTER VARYING;
   art_id       INTEGER;
   pep_art_id   INTEGER;
   uriInt       INTEGER;
   countDups    INTEGER;
   jsonURI      TEXT;
   jsonBody     TEXT;
   data         JSONB;
   concepts     JSONB;
BEGIN
   uriInt = ("ArticleJSON" ->> 'uri'::TEXT);
   jsonBody = ("ArticleJSON" ->> 'body'::TEXT);
   concepts = ("ArticleJSON" ->> 'concepts'::TEXT);
   retMessg = "ArticleJSON" -> 'title';

   -- see if any existing articles with the current PEP in the them are
   -- more than 80% similar to this new one,
   -- if so, consider it a duplicate
   -- get the art_id of the most similar article
	
	SELECT s."ID"
	INTO art_id
	FROM (
		SELECT "ID", similarity(body, jsonBody) 
		FROM finnova."Art" a
		WHERE a."body" LIKE (SELECT '%' || lastname || '%' 
							 FROM finnova."MV_PEP"
							 -- only check articles that have the current PEP in them.
							 WHERE "ID" = "PEP_ID") 					  
		ORDER BY similarity DESC -- order results descending to find the most similar article
	) s  
	WHERE similarity > 0.80
	LIMIT 1;	-- take the top, most similar article_id	
	
   IF (art_id IS NOT NULL) THEN
   		retMessg = 'DUPLICATE: ' || retMessg;
   ELSE -- New article to be added.

	   -- full original JSON object is too large for indexing
	   -- remove the 'body' and 'concepts' elements to reduce its size.
	   -- those elements will get saved in their own columns
	   SELECT "ArticleJSON"::JSONB - 'body' - 'concepts'
		 INTO data;
	
	   INSERT INTO finnova."Art" ("URI", "body", "concepts", "data")
			VALUES (uriInt::INTEGER, jsonBody::TEXT, concepts::JSONB, data::JSONB);
	
	   -- find the art_id of the article that was just inserted
	   SELECT a."ID"
		 INTO art_id
		 FROM finnova."Art" a
		WHERE a."URI" = uriInt;
	END IF;

   -- Make an entry in the PEP_ART table for the pep and article
   -- if it was an article that was already in the database
   -- there is a chance that there is already an entry for it and the pep
   -- so on conflict do nothing.
   INSERT INTO finnova."PEP_ART" ("PEP_ID", "ART_ID")
        VALUES ("PEP_ID", art_id)
   ON CONFLICT DO NOTHING;

   RETURN retMessg;
EXCEPTION
   WHEN OTHERS
   THEN
      RAISE INFO 'Error Name:%', SQLERRM;
      RAISE INFO 'Error State:%', SQLSTATE;
      RETURN 'Error Will Robinson! ' || SQLERRM;
END;
$function$
;
