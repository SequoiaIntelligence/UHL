CREATE OR REPLACE FUNCTION finnova.updatepep_art(p_id integer)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
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
	            WHEN substr(pep.lastname::text, 1, 3) = 'de '::text 
				THEN substr(pep.lastname::text, 3)
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
$function$