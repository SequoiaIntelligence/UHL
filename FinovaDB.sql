-- =============================================================================
-- Diagram Name: Finnova Adverse Media Search DB
-- Created on: 3/17/2019 6:44:18 PM
-- Diagram Version: 
-- =============================================================================

CREATE SCHEMA "finnova" AUTHORIZATION "FreiburgBill";





SET CHECK_FUNCTION_BODIES TO FALSE;

CREATE OR REPLACE FUNCTION "finnova"."lexeme_occurrences" (IN _lexemes tsvector,
	IN _word text,
	IN _config int4,
	OUT lexeme_count int4,
	OUT lexeme_positions int4[]) 	
RETURNS record AS
$BODY$
DECLARE
--   _lexemes              tsvector := to_tsvector (_config, _document);
   _searched_lexeme      tsvector :=  strip (to_tsvector (_config, _word));
   _occurences_pattern   TEXT := _searched_lexeme::TEXT || ':([0-9,]+)';
   _occurences_list      TEXT
      := substring (_lexemes::TEXT, _occurences_pattern);
BEGIN
   SELECT count (a), array_agg (a::INT)
     FROM regexp_split_to_table (_occurences_list, ',') a
    WHERE _searched_lexeme::TEXT != ''           -- preventing false positives
     INTO lexeme_count, lexeme_positions;

   RETURN;
END
$BODY$
	LANGUAGE plpgsql
	CALLED ON NULL INPUT
	VOLATILE
	EXTERNAL SECURITY INVOKER
	COST 100;


ALTER FUNCTION "finnova"."lexeme_occurrences" (IN _lexemes tsvector,
	IN _word text,
	IN _config int4,
	OUT lexeme_count int4,
	OUT lexeme_positions int4[]) OWNER TO "FreiburgBill";



CREATE OR REPLACE FUNCTION "finnova"."addArticle" (IN PEP_ID int4, IN ArticleJSON jsonb) 	
RETURNS varchar AS
$BODY$
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
							 WHERE "ID" = "PEP_ID")		-- only check articles that have the current PEP in them.					  
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
$BODY$
	LANGUAGE plpgsql
	CALLED ON NULL INPUT
	VOLATILE
	EXTERNAL SECURITY INVOKER
	COST 100;


ALTER FUNCTION "finnova"."addArticle" (IN PEP_ID int4, IN ArticleJSON jsonb) OWNER TO "FreiburgBill";



CREATE OR REPLACE FUNCTION "finnova"."updatepep_art" (IN p_id int4) 	
RETURNS text AS
$BODY$
DECLARE
   pep                   RECORD;
   art_id                INTEGER;
   b_art_id              INTEGER;
   pep_id                INTEGER;
   lastname              TEXT;
   lexeme_count          INTEGER;
   lexeme_positions      INTEGER [];
   _lexemes              tsvector;
   _searched_lexeme      tsvector;
   _occurences_pattern   TEXT;
   _occurences_list      TEXT;
BEGIN
   FOR art_id IN   SELECT a."ID" AS art_id
                     FROM finnova."Art" a
                    WHERE a."ID" > p_id
                 ORDER BY art_id
   LOOP
      RAISE NOTICE 'Art_ID = % ', art_id;

      -- For every Article ID
      -- See if PEP_ID exists for it in PEP_ART
      FOR pep IN   SELECT p."ID" AS pep_id, p."lastname", p."searchname"
                     FROM finnova."MV_PEP" p
                 ORDER BY pep_id
      LOOP
         --
         IF NOT EXISTS
               (SELECT *
                  FROM finnova."PEP_ART" pa
                 WHERE pa."ART_ID" = art_id AND pa."PEP_ID" = pep.pep_id)
         THEN
            -- The current PEP_ID is not associated with the current ART_ID
            -- in the PEP_ART Table
            -- See if the current PEP name is in this article

            IF EXISTS
                  (SELECT *
                    FROM finnova."Art" a
                   WHERE     a."ID" = art_id
                         AND a.body LIKE ('%' || pep.searchname || '%'))
            THEN
               -- PEP exists in this article
               -- add a new PEP_ART entry for it

               INSERT INTO finnova."PEP_ART" ("PEP_ID", "ART_ID")
                    VALUES (pep.pep_id, art_id);

               RAISE NOTICE
               'SearchName = % Art_ID = % PEP_ID = %',
               pep.searchname, art_id, pep.pep_id;
            END IF;                              -- PEP exists in this article
         END IF;                 -- IF NOT EXISTS PEP_ID and ART_ID in PEP_ART
		       
		-- Now update the count of the current PEP  for the current article
        -- in the PEP_ART table.
		  SELECT a.vector
			INTO _lexemes
			FROM finnova."Art" a
		   WHERE "ID" = art_id;
			  
		  -- If the last name starts with 'de ', just take the part after the 'de '
		  -- Else try to split hyphinated names and just take the first part before 
		  -- the hyphination, if it is not hypinated it will take the whole thing.
		  SELECT 
	        CASE
	            WHEN substr(pep.lastname::text, 1, 3) = 'de '::text THEN substr(pep.lastname::text, 3)
	            ELSE split_part(pep.lastname::text, '-', 1)
	        END 
	        INTO lastname;
	       
		  _searched_lexeme := strip (to_tsvector ('german', lastname));
		  _occurences_pattern := _searched_lexeme::TEXT || ':([0-9,]+)';
		  _occurences_list := substring (_lexemes::TEXT, _occurences_pattern);
	
		  SELECT count (a), array_agg (a::INT)
			FROM regexp_split_to_table (_occurences_list, ',') a
		   WHERE _searched_lexeme::TEXT != ''        -- preventing false positives
			INTO lexeme_count, lexeme_positions;
	
		  UPDATE finnova."PEP_ART" pa
			 SET count = lexeme_count, positions = lexeme_positions
		   WHERE pa."ART_ID" = art_id AND pa."PEP_ID" = pep.pep_id;		 
		 		 
      END LOOP;                                                -- through PEPs


   END LOOP;                                               -- through Articles

   RETURN 'All done!';
END;
$BODY$
	LANGUAGE plpgsql
	CALLED ON NULL INPUT
	VOLATILE
	EXTERNAL SECURITY INVOKER
	COST 100;


ALTER FUNCTION "finnova"."updatepep_art" (IN p_id int4) OWNER TO "FreiburgBill";



CREATE OR REPLACE FUNCTION "finnova"."find_pep_art" (IN p_id int4) 	
RETURNS text AS
$BODY$
DECLARE
   aword                 RECORD;
   pep                   RECORD;
   pepart                RECORD;
   art                   RECORD;
   list                  RECORD;
   art_id                INTEGER;
   b_art_id              INTEGER;
   pep_id                INTEGER;
   closest_pep           INTEGER;
   closest_pos           INTEGER;
   aword_count           INTEGER;
   aword_positions       INTEGER [];
   search_name           TEXT;
   _lexemes              tsvector;
   _searched_lexeme      tsvector;
   _occurences_pattern   TEXT;
   _occurences_list      TEXT;
BEGIN
   -- get list of articles that have alert words in them
   FOR art IN   SELECT a."ID", a."vector"
                  FROM finnova."MV_ART" a
                 WHERE a."ID" > p_id AND a."alert" = TRUE
              ORDER BY art_id
   LOOP
      RAISE NOTICE 'Art_ID = % ', art."ID";

      -- For every Article ID
      -- Check every alert word
      FOR aword IN SELECT * 
                     FROM finnova."ALERT_WORDS" aw
      LOOP
         --
         SELECT finnova.lexeme_occurrences (art.vector, aword."word", 'german')
           INTO list;

         RAISE NOTICE '>>>>>>>>>>>>>> List = % %', list, aword."word";

         _lexemes := art.vector;
         _searched_lexeme := strip (to_tsvector ('german', aword."word"));
         _occurences_pattern := _searched_lexeme::TEXT || ':([0-9,]+)';
         _occurences_list := substring (_lexemes::TEXT, _occurences_pattern);

         SELECT count (a), array_agg (a::INT)
           FROM regexp_split_to_table (_occurences_list, ',') a
          WHERE _searched_lexeme::TEXT != ''     -- preventing false positives
           INTO aword_count, aword_positions;

          
         FOR counter IN 1 .. aword_count
         LOOP
            RAISE NOTICE 'Counter: % %', aword_positions[counter], aword_count;
           -- For the current alert word and its current position
           -- find the nearest PEP and its position.
           
          closest_pep := 0;  -- should end up with the ID of the closest PEP
          closest_pos := 0;  -- should end up with the position of the closest PEP
          
          -- Check all the PEPs that are associated with this article
          
         	FOR pepart IN SELECT * 
          				FROM finnova."PEP_ART" 
          				WHERE "ART_ID" = art."ID"
		 	LOOP
		 	    RAISE NOTICE 'PEP_ART PEP_ID: % %', pepart."PEP_ID", pepart."positions";
		 		FOR pep_pos IN 1..pepart."count" -- loop t 
          		LOOP
          			RAISE NOTICE 'PEP POS : %', pepart."positions"[pep_pos];
          			IF pepart."positions"[pep_pos] < aword_positions[counter]
          			AND pepart."positions"[pep_pos] > closest_pos
          			THEN
          				-- if the current pep position is before the alert word position
          				-- and it is after the last found closest position
          				-- update the closest postion and closest pep
          				RAISE NOTICE 'Before Closest PEP and POS: % %', closest_pep, closest_pos;
          				closest_pep := pepart."PEP_ID";
          			    closest_pos := pepart."positions"[pep_pos];
          			    RAISE NOTICE 'After Closest PEP and POS: % %', closest_pep, closest_pos;
          		 	END IF;          		
           		END LOOP; -- positions of pep in article        		
           		
            END LOOP; -- pep_arts 
            
	         IF closest_pep > 0 THEN
	            -- if a pep was found
	            -- Add an entry in the ALERTWORDS_ART table 
	            -- for the values found above.
	            
	            INSERT INTO finnova."ALERTWORDS_ART" ("AW_ID", "PEP_ID", "AW_POS", "PEP_POS", "ART_ID")
	            VALUES (aword."ID", closest_pep, aword_positions[counter], closest_pos, art."ID");
	        END IF;                
 
         END LOOP;  -- positions of alert words
      END LOOP;  -- alert words loop
   END LOOP;     -- article loop

   RETURN 'All done!';
END;
$BODY$
	LANGUAGE plpgsql
	CALLED ON NULL INPUT
	VOLATILE
	EXTERNAL SECURITY INVOKER
	COST 100;


ALTER FUNCTION "finnova"."find_pep_art" (IN p_id int4) OWNER TO "FreiburgBill";



SET CHECK_FUNCTION_BODIES TO TRUE;


CREATE TABLE "finnova"."ALERTWORDS_ART" (
	"ID" SERIAL NOT NULL,
	"AW_ID" int4,
	"PEP_ID" int4,
	"AW_POS" int4,
	"PEP_POS" int4,
	"ART_ID" int4,
	CONSTRAINT "ALERTWORDS_ART_pkey" PRIMARY KEY("ID")
)
WITH (
	OIDS = False
);

CREATE INDEX "alertwords_art_aw_id_idx" ON "finnova"."ALERTWORDS_ART" USING BTREE (
	"AW_ID"
);


CREATE INDEX "alertwords_art_pep_id_idx" ON "finnova"."ALERTWORDS_ART" USING BTREE (
	"PEP_ID"
);


CREATE INDEX "alertwords_art_art_id_idx" ON "finnova"."ALERTWORDS_ART" USING BTREE (
	"ART_ID"
);


ALTER TABLE "finnova"."ALERTWORDS_ART" OWNER TO "FreiburgBill";

COMMENT ON TABLE "finnova"."ALERTWORDS_ART" IS 'This table joins the Alert_Words table with the PEP table.
It identifies the position of an alert word in an article, the PEP that is closes to that word, and the position in the article of that PEP.';

COMMENT ON COLUMN "finnova"."ALERTWORDS_ART"."ID" IS 'Primiary Key ID';

COMMENT ON COLUMN "finnova"."ALERTWORDS_ART"."AW_ID" IS 'FK to Alert_Words ID';

COMMENT ON COLUMN "finnova"."ALERTWORDS_ART"."PEP_ID" IS 'FK to PEP ID';

COMMENT ON COLUMN "finnova"."ALERTWORDS_ART"."AW_POS" IS 'Position of the alert word in the article by word count.';

COMMENT ON COLUMN "finnova"."ALERTWORDS_ART"."PEP_POS" IS 'Position of the PEP''s last name in the article that is closes to the alert word.';

COMMENT ON COLUMN "finnova"."ALERTWORDS_ART"."ART_ID" IS 'FK on Art table ID field.';

CREATE TABLE "finnova"."ALERT_WORDS" (
	"ID" SERIAL NOT NULL,
	"word" varchar(30) NOT NULL,
	CONSTRAINT "ALERT_WORDS_pkey" PRIMARY KEY("ID")
)
WITH (
	OIDS = False
);

ALTER TABLE "finnova"."ALERT_WORDS" OWNER TO "FreiburgBill";

CREATE TABLE "finnova"."Art" (
	"ID" SERIAL NOT NULL,
	"data" jsonb,
	"URI" int4 NOT NULL,
	"body" text,
	"concepts" jsonb,
	"polarity" float4,
	"vector" tsvector,
	CONSTRAINT "Art_pkey" PRIMARY KEY("ID")
)
WITH (
	OIDS = False
);

CREATE INDEX "BODY_INDX" ON "finnova"."Art" USING GIST (
	"body"
)
WITH (
	BUFFERING = AUTO
);


CREATE UNIQUE INDEX "ART_ID_INDX" ON "finnova"."Art" USING BTREE (
	"ID"
);


CREATE INDEX "VECTOR_INDX" ON "finnova"."Art" USING GIN (
	"vector"
)
WITH (
	FASTUPDATE = ON
);


CREATE UNIQUE INDEX "URI_INDX" ON "finnova"."Art" USING BTREE (
	"URI"
);


ALTER TABLE "finnova"."Art" OWNER TO "FreiburgBill";

COMMENT ON TABLE "finnova"."Art" IS 'Table for EventRegistry Articles';

COMMENT ON COLUMN "finnova"."Art"."ID" IS 'Primary Key';

COMMENT ON COLUMN "finnova"."Art"."data" IS 'Contains returned JSON object from EventRegisty';

COMMENT ON COLUMN "finnova"."Art"."URI" IS 'Article unique URI';

COMMENT ON COLUMN "finnova"."Art"."body" IS 'The article text removed from the EventRegistry JSON';

COMMENT ON COLUMN "finnova"."Art"."concepts" IS 'JSON concepts object removed from EventRegistry JSON';

COMMENT ON COLUMN "finnova"."Art"."polarity" IS 'Sentiment Polarity ';

COMMENT ON COLUMN "finnova"."Art"."vector" IS 'TSVECTOR of the article body.';

COMMENT ON INDEX "finnova"."VECTOR_INDX" IS 'GIN Index on the tsvector column ''Vector2''';

COMMENT ON INDEX "finnova"."URI_INDX" IS 'Unique URI index.
Article URI should only exist once in the table.';

CREATE TABLE "finnova"."Articles" (
	"ID" SERIAL NOT NULL,
	"PEP_ID" int4,
	"data" jsonb,
	"sensitivity" jsonb,
	"html" text,
	"polarity" float4,
	CONSTRAINT "Articles_pkey" PRIMARY KEY("ID")
)
WITH (
	OIDS = False
);

CREATE UNIQUE INDEX "ArticlesUniqueIndex" ON "finnova"."Articles" USING BTREE (
	"PEP_ID", 
	"data"
);


CREATE INDEX "DataGin_Index" ON "finnova"."Articles" USING GIN (
	"data"
)
WITH (
	FASTUPDATE = ON
);


ALTER TABLE "finnova"."Articles" OWNER TO "FreiburgBill";

COMMENT ON COLUMN "finnova"."Articles"."polarity" IS '-1.0 Negative
 0.0 Neutral
 1.0 Positive';

COMMENT ON INDEX "finnova"."ArticlesUniqueIndex" IS 'Unique Index for FK PEP_ID and Data JSON field';

CREATE TABLE "finnova"."PEP" (
	"Name" varchar(50) NOT NULL,
	"Gender" char(1),
	"Language" char(1),
	"Year" int4,
	"ID" SERIAL NOT NULL,
	CONSTRAINT "PEP_pkey" PRIMARY KEY("ID")
)
WITH (
	OIDS = False
);

CREATE UNIQUE INDEX "PEP_ID_INDX" ON "finnova"."PEP" USING BTREE (
	"ID"
);


CREATE UNIQUE INDEX "pep_name_idx" ON "finnova"."PEP" USING BTREE (
	"Name"
);


ALTER TABLE "finnova"."PEP" OWNER TO "FreiburgBill";

COMMENT ON COLUMN "finnova"."PEP"."Name" IS 'LastName FirstName';

COMMENT ON COLUMN "finnova"."PEP"."Gender" IS 'M - Male
F- Female ';

COMMENT ON COLUMN "finnova"."PEP"."Language" IS 'd - Deutch
f -  French';

COMMENT ON COLUMN "finnova"."PEP"."Year" IS 'Year of birth';

COMMENT ON COLUMN "finnova"."PEP"."ID" IS 'Primary Key';

CREATE TABLE "finnova"."PEP_ART" (
	"ID" SERIAL NOT NULL,
	"PEP_ID" int4 NOT NULL,
	"ART_ID" int4 NOT NULL,
	"count" int4,
	"positions" int4[],
	CONSTRAINT "PEP_ART_pkey" PRIMARY KEY("ID")
)
WITH (
	OIDS = False
);

CREATE UNIQUE INDEX "PEP_ART_INDX" ON "finnova"."PEP_ART" USING BTREE (
	"PEP_ID", 
	"ART_ID"
);


ALTER TABLE "finnova"."PEP_ART" OWNER TO "FreiburgBill";

COMMENT ON TABLE "finnova"."PEP_ART" IS 'Table to join a PEP to Articles they are mentioned in.';

COMMENT ON COLUMN "finnova"."PEP_ART"."PEP_ID" IS 'Foreign Key to PEP table ID column.';

COMMENT ON COLUMN "finnova"."PEP_ART"."ART_ID" IS 'Foreign Key to ART table ID column.';

COMMENT ON COLUMN "finnova"."PEP_ART"."count" IS 'Number of times this PEP (PEP_ID) appears in this article.';

COMMENT ON COLUMN "finnova"."PEP_ART"."positions" IS 'Integer array of the positions (by word count) where this PEP''s last name appears.';


ALTER TABLE "finnova"."ALERTWORDS_ART" ADD CONSTRAINT "FK_ALERTWORDS_ART_AW" FOREIGN KEY ("AW_ID")
	REFERENCES "finnova"."ALERT_WORDS"("ID")
	MATCH SIMPLE
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	NOT DEFERRABLE;

ALTER TABLE "finnova"."ALERTWORDS_ART" ADD CONSTRAINT "FK_ALERTWORDS_ART_PEP" FOREIGN KEY ("PEP_ID")
	REFERENCES "finnova"."PEP"("ID")
	MATCH SIMPLE
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	NOT DEFERRABLE;

ALTER TABLE "finnova"."ALERTWORDS_ART" ADD CONSTRAINT "FK_ALERTWORDS_ART_ART" FOREIGN KEY ("ART_ID")
	REFERENCES "finnova"."Art"("ID")
	MATCH SIMPLE
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	NOT DEFERRABLE;

ALTER TABLE "finnova"."Articles" ADD CONSTRAINT "FK_Articles_1" FOREIGN KEY ("PEP_ID")
	REFERENCES "finnova"."PEP"("ID")
	MATCH SIMPLE
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	NOT DEFERRABLE;

ALTER TABLE "finnova"."PEP_ART" ADD CONSTRAINT "FK_PEP_ART_1" FOREIGN KEY ("PEP_ID")
	REFERENCES "finnova"."PEP"("ID")
	MATCH SIMPLE
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	NOT DEFERRABLE;

ALTER TABLE "finnova"."PEP_ART" ADD CONSTRAINT "FK_PEP_ART_2" FOREIGN KEY ("ART_ID")
	REFERENCES "finnova"."Art"("ID")
	MATCH SIMPLE
	ON DELETE NO ACTION
	ON UPDATE NO ACTION
	NOT DEFERRABLE;


CREATE MATERIALIZED VIEW "finnova"."MV_ALERT_WORDS" AS
	SELECT ts_stat.word,
    ts_stat.ndoc,
    ts_stat.nentry
   FROM ts_stat('SELECT to_tsvector(''german'', body) from finnova."Art"'::text) ts_stat(word, ndoc, nentry)
  WHERE (ts_stat.word IN ( SELECT "substring"(replace((to_tsvector('german'::regconfig, ("ALERT_WORDS".word)::text))::text, ''':1'::text, ''::text), 2) AS "substring"
           FROM finnova."ALERT_WORDS"));

COMMENT ON VIEW "finnova"."MV_ALERT_WORDS" IS 'List the number of documents and number of times the alert words appear.';


ALTER TABLE "finnova"."MV_ALERT_WORDS" OWNER TO "FreiburgBill";



CREATE MATERIALIZED VIEW "finnova"."MV_ART" AS
	SELECT (a.data ->> 'uri'::text) AS uri,
    (a.data ->> 'title'::text) AS title,
    (a.data ->> 'url'::text) AS url,
    (a.data ->> 'image'::text) AS urltoimage,
    (a.data ->> 'dateTime'::text) AS publishedat,
    ((a.data -> 'source'::text) ->> 'title'::text) AS source,
    (((a.data #>> '{categories,0}'::text[]))::json ->> 'uri'::text) AS category,
    (((a.data #>> '{authors,0}'::text[]))::json ->> 'name'::text) AS author,
    (a.data ->> 'shares'::text) AS shares,
    a."ID",
    (lower(a.body) ~ similar_escape((((('%(bestechung|geldwäsche|korruption|schmiergelder|waffenhandel|'::text || 'drogenhandel|steuerhinterziehung|umweltverschmutzung|blutdiamanten|'::text) || 'scheeballsysteme|insiderhandel|vorteilsnahme|ungetreue geschäftsführung|'::text) || 'käuflich|schwarze kasse|schwarzgeld|veruntreuung|unterschlagung|beeinflussung|'::text) || 'Ponzi|betrug)%'::text), NULL::text)) AS alert,
    a.polarity,
    (a.data ->> 'location'::text) AS location,
    (a.data ->> 'evetURI'::text) AS eventuri,
    (a.data ->> 'storyURI'::text) AS storyuri,
    a.body,
    array_length(regexp_split_to_array(a.body, '\s'::text), 1) AS word_count,
    to_tsvector('german'::regconfig, a.body) AS vector
   FROM finnova."Art" a;

COMMENT ON VIEW "finnova"."MV_ART" IS 'Materialized View of the Art (articles) table with the JSONB data column elements separated into their own columns and the body of the article as a tsvector column.';


ALTER TABLE "finnova"."MV_ART" OWNER TO "FreiburgBill";



CREATE MATERIALIZED VIEW "finnova"."MV_PEP" AS
	SELECT p."ID",
        CASE
            WHEN (substr((p."Name")::text, 1, 3) = 'de '::text) THEN btrim("substring"((p."Name")::text, 'de [^ ]*'::text))
            ELSE btrim(substr((p."Name")::text, 1, "position"((p."Name")::text, ' '::text)))
        END AS lastname,
        CASE
            WHEN (substr((p."Name")::text, 1, 3) = 'de '::text) THEN btrim("substring"((p."Name")::text, '(?:\S+ ){2}(.*)'::text))
            ELSE btrim(substr((p."Name")::text, "position"((p."Name")::text, ' '::text)))
        END AS firstname,
        CASE
            WHEN (substr((p."Name")::text, 1, 3) = 'de '::text) THEN (("substring"((p."Name")::text, '(?:\S+ ){2}(.*)'::text) || ' '::text) || "substring"((p."Name")::text, 'de [^ ]*'::text))
            ELSE ((btrim(substr((p."Name")::text, "position"((p."Name")::text, ' '::text))) || ' '::text) || btrim(substr((p."Name")::text, 1, "position"((p."Name")::text, ' '::text))))
        END AS searchname,
    p."Gender",
    p."Language",
    p."Year",
    (date_part('year'::text, CURRENT_DATE) - (p."Year")::double precision) AS age,
    s.num_articles,
    s.earliest,
    s.latest,
    s.min,
    s.max,
    s.avg,
    s.num_alerts
   FROM (finnova."PEP" p
     LEFT JOIN ( SELECT pa."PEP_ID",
            count(*) AS num_articles,
            ( SELECT count(*) AS count
                   FROM finnova."V_PEP_ART2" v
                  WHERE ((v.alert = true) AND (v."PEP_ID" = pa."PEP_ID"))) AS num_alerts,
            min((a.data ->> 'dateTime'::text)) AS earliest,
            max((a.data ->> 'dateTime'::text)) AS latest,
            min(a.polarity) AS min,
            max(a.polarity) AS max,
            avg(a.polarity) AS avg
           FROM (finnova."Art" a
             LEFT JOIN finnova."PEP_ART" pa ON ((a."ID" = pa."ART_ID")))
          GROUP BY pa."PEP_ID") s ON ((p."ID" = s."PEP_ID")));

COMMENT ON VIEW "finnova"."MV_PEP" IS 'Materialized View of the PEP table.
Calculates age from year of birth,
shows total number of articles per PEP,
shows earliest date of an article per PEP,
shows latest date of an article per PEP,
shows the min, man, and avg sentiment polarity per PEP.';


ALTER TABLE "finnova"."MV_PEP" OWNER TO "FreiburgBill";



CREATE MATERIALIZED VIEW "finnova"."MV_Sources" AS
	SELECT v.source,
    count(v.article_id) AS num_articles,
    min(v.publishedat) AS earliest,
    max(v.publishedat) AS latest,
    min(v.polarity) AS min,
    max(v.polarity) AS max,
    avg(v.polarity) AS avg,
    count(
        CASE
            WHEN v.alert THEN 1
            ELSE NULL::integer
        END) AS num_alerts
   FROM finnova."V_Art" v
  GROUP BY v.source;

ALTER TABLE "finnova"."MV_Sources" OWNER TO "FreiburgBill";



CREATE MATERIALIZED VIEW "finnova"."MV_WORD_PEP_ART" AS
	SELECT aw.word,
    awa."AW_POS",
    p."ID" AS pep_id,
    p.searchname,
    awa."PEP_POS",
    (awa."AW_POS" - awa."PEP_POS") AS score,
    a.word_count,
    (((100)::double precision * ((awa."PEP_POS")::real / (a.word_count)::real)))::integer AS pos_percent,
    a.title,
    a.publishedat,
    a.polarity,
    a."ID" AS art_id,
    a.url,
    a.category,
    a.author,
    a.shares
   FROM (((finnova."ALERTWORDS_ART" awa
     LEFT JOIN finnova."ALERT_WORDS" aw ON ((awa."AW_ID" = aw."ID")))
     LEFT JOIN finnova."MV_PEP" p ON ((awa."PEP_ID" = p."ID")))
     LEFT JOIN finnova."MV_ART" a ON ((awa."ART_ID" = a."ID")));

COMMENT ON VIEW "finnova"."MV_WORD_PEP_ART" IS 'Shows alert words and their associated PEPs position in an article along with the article title, polarity, and url.';


ALTER TABLE "finnova"."MV_WORD_PEP_ART" OWNER TO "FreiburgBill";



CREATE OR REPLACE VIEW "finnova"."V_Art" AS
	SELECT (a.data ->> 'uri'::text) AS uri,
    (a.data ->> 'title'::text) AS title,
    (a.data ->> 'url'::text) AS url,
    (a.data ->> 'image'::text) AS urltoimage,
    (a.data ->> 'dateTime'::text) AS publishedat,
    ((a.data -> 'source'::text) ->> 'title'::text) AS source,
    a."ID" AS article_id,
    (lower(a.body) ~ similar_escape((((('%(bestechung|geldwäsche|korruption|schmiergelder|waffenhandel|'::text || 'drogenhandel|steuerhinterziehung|umweltverschmutzung|blutdiamanten|'::text) || 'scheeballsysteme|insiderhandel|vorteilsnahme|ungetreue geschäftsführung|'::text) || 'käuflich|schwarze kasse|schwarzgeld|veruntreuung|unterschlagung|beeinflussung|'::text) || 'Ponzi|betrug)%'::text), NULL::text)) AS alert,
    a.polarity
   FROM finnova."Art" a;

ALTER TABLE "finnova"."V_Art" OWNER TO "FreiburgBill";



CREATE OR REPLACE VIEW "finnova"."V_Articles" AS
	SELECT p."Name" AS name,
    (a.data ->> 'author'::text) AS author,
    (a.data ->> 'title'::text) AS title,
    (a.data ->> 'description'::text) AS description,
    (a.data ->> 'url'::text) AS url,
    (a.data ->> 'urlToImage'::text) AS urltoimage,
    (a.data ->> 'publishedAt'::text) AS publishedat,
    (a.data ->> 'content'::text) AS content,
    ((a.data -> 'source'::text) ->> 'name'::text) AS source,
    a."PEP_ID",
    a."ID" AS article_id,
    a.polarity,
    (lower(a.html) ~ similar_escape((((('%(bestechung|geldwäsche|korruption|schmiergelder|waffenhandel|'::text || 'drogenhandel|steuerhinterziehung|umweltverschmutzung|blutdiamanten|'::text) || 'scheeballsysteme|insiderhandel|vorteilsnahme|ungetreue geschäftsführung|'::text) || 'käuflich|schwarze kasse|schwarzgeld|veruntreuung|unterschlagung|beeinflussung|'::text) || 'Ponzi|betrug)%'::text), NULL::text)) AS alert
   FROM (finnova."Articles" a
     LEFT JOIN finnova."PEP" p ON ((a."PEP_ID" = p."ID")));

ALTER TABLE "finnova"."V_Articles" OWNER TO "FreiburgBill";



CREATE OR REPLACE VIEW "finnova"."V_Articles_Summery" AS
	SELECT a.author,
    a.title,
    a.description,
    a.url,
    a.urltoimage,
    a.publishedat,
    a.content,
    a.source,
    count(a.name) AS numpeps,
    a.polarity,
    a.alert
   FROM finnova."V_Articles" a
  GROUP BY a.author, a.title, a.description, a.url, a.urltoimage, a.publishedat, a.content, a.source, a.polarity, a.alert;

ALTER TABLE "finnova"."V_Articles_Summery" OWNER TO "FreiburgBill";



CREATE OR REPLACE VIEW "finnova"."V_PEP" AS
	SELECT p."ID",
    p."Name",
    ((substr((p."Name")::text, "position"((p."Name")::text, ' '::text)) || ' '::text) || substr((p."Name")::text, 1, "position"((p."Name")::text, ' '::text))) AS searchname,
    p."Gender",
    p."Language",
    p."Year",
    (date_part('year'::text, CURRENT_DATE) - (p."Year")::double precision) AS age,
    s.num_articles,
    s.earliest,
    s.latest,
    s.min,
    s.max,
    s.avg,
    s.num_alerts
   FROM (finnova."PEP" p
     LEFT JOIN ( SELECT a."PEP_ID",
            count(*) AS num_articles,
            ( SELECT count(*) AS count
                   FROM finnova."V_Articles" v
                  WHERE ((v.alert = true) AND (v."PEP_ID" = a."PEP_ID"))) AS num_alerts,
            min((a.data ->> 'publishedAt'::text)) AS earliest,
            max((a.data ->> 'publishedAt'::text)) AS latest,
            min(a.polarity) AS min,
            max(a.polarity) AS max,
            avg(a.polarity) AS avg
           FROM finnova."Articles" a
          GROUP BY a."PEP_ID") s ON ((p."ID" = s."PEP_ID")));

ALTER TABLE "finnova"."V_PEP" OWNER TO "FreiburgBill";



CREATE OR REPLACE VIEW "finnova"."V_PEP_ART" AS
	SELECT p."Name",
    (a.data ->> 'title'::text) AS title,
    (lower(a.body) ~ similar_escape((((('%(bestechung|geldwäsche|korruption|schmiergelder|waffenhandel|'::text || 'drogenhandel|steuerhinterziehung|umweltverschmutzung|blutdiamanten|'::text) || 'scheeballsysteme|insiderhandel|vorteilsnahme|ungetreue geschäftsführung|'::text) || 'käuflich|schwarze kasse|schwarzgeld|veruntreuung|unterschlagung|beeinflussung|'::text) || 'Ponzi|betrug)%'::text), NULL::text)) AS alert,
    ((a.data -> 'source'::text) ->> 'title'::text) AS source,
    (a.data ->> 'dateTime'::text) AS publishedat,
    pa."PEP_ID",
    pa."ART_ID",
    a.data,
    (a.data -> 'url'::text) AS url
   FROM ((finnova."PEP" p
     LEFT JOIN finnova."PEP_ART" pa ON ((p."ID" = pa."PEP_ID")))
     LEFT JOIN finnova."Art" a ON ((pa."ART_ID" = a."ID")));

ALTER TABLE "finnova"."V_PEP_ART" OWNER TO "FreiburgBill";



CREATE OR REPLACE VIEW "finnova"."V_PEP_ART2" AS
	SELECT p."Name",
    (a.data ->> 'url'::text) AS url,
    (a.data ->> 'title'::text) AS title,
    (lower(a.body) ~ similar_escape((((('%(bestechung|geldwäsche|korruption|schmiergelder|waffenhandel|'::text || 'drogenhandel|steuerhinterziehung|umweltverschmutzung|blutdiamanten|'::text) || 'scheeballsysteme|insiderhandel|vorteilsnahme|ungetreue geschäftsführung|'::text) || 'käuflich|schwarze kasse|schwarzgeld|veruntreuung|unterschlagung|beeinflussung|'::text) || 'Ponzi|betrug)%'::text), NULL::text)) AS alert,
    ((a.data -> 'source'::text) ->> 'title'::text) AS source,
    (a.data ->> 'dateTime'::text) AS publishedat,
    pa."PEP_ID",
    pa."ART_ID",
    a.data
   FROM ((finnova."PEP" p
     LEFT JOIN finnova."PEP_ART" pa ON ((p."ID" = pa."PEP_ID")))
     LEFT JOIN finnova."Art" a ON ((pa."ART_ID" = a."ID")));

ALTER TABLE "finnova"."V_PEP_ART2" OWNER TO "FreiburgBill";



CREATE OR REPLACE VIEW "finnova"."V_PEP_Summery" AS
	SELECT p."ID",
    p."Name",
    ((substr((p."Name")::text, "position"((p."Name")::text, ' '::text)) || ' '::text) || substr((p."Name")::text, 1, "position"((p."Name")::text, ' '::text))) AS searchname,
    p."Gender",
    p."Language",
    p."Year",
    (date_part('year'::text, CURRENT_DATE) - (p."Year")::double precision) AS age,
    s.num_articles,
    s.earliest,
    s.latest,
    s.num_alerts
   FROM (finnova."PEP" p
     LEFT JOIN ( SELECT a."PEP_ID",
            count(*) AS num_articles,
            ( SELECT count(*) AS count
                   FROM finnova."V_PEP_ART" v
                  WHERE ((v.alert = true) AND (v."PEP_ID" = a."PEP_ID"))) AS num_alerts,
            min(a.publishedat) AS earliest,
            max(a.publishedat) AS latest
           FROM finnova."V_PEP_ART" a
          GROUP BY a."PEP_ID") s ON ((p."ID" = s."PEP_ID")));

ALTER TABLE "finnova"."V_PEP_Summery" OWNER TO "FreiburgBill";



CREATE OR REPLACE VIEW "finnova"."V_Sources" AS
	SELECT v.source,
    count(v.article_id) AS num_articles,
    min(v.publishedat) AS earliest,
    max(v.publishedat) AS latest,
    min(v.polarity) AS min,
    max(v.polarity) AS max,
    avg(v.polarity) AS avg
   FROM finnova."V_Articles" v
  GROUP BY v.source;

ALTER TABLE "finnova"."V_Sources" OWNER TO "FreiburgBill";



