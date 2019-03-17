CREATE OR REPLACE FUNCTION finnova.find_pep_art(p_id integer)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
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
           -- For the current alert word and its current position
           -- find the nearest PEP and its position.
           
          closest_pep := 0;  -- should end up with the ID of the closest PEP
          closest_pos := 0;  -- should end up with the position of the closest PEP
          
          -- Check all the PEPs that are associated with this article
          
         	FOR pepart IN SELECT * 
          				FROM finnova."PEP_ART" 
          				WHERE "ART_ID" = art."ID"
		 	LOOP
		 		FOR pep_pos IN 1..pepart."count" -- loop t 
          		LOOP
          			RAISE NOTICE 'PEP POS : %', pepart."positions"[pep_pos];
          			IF pepart."positions"[pep_pos] < aword_positions[counter]
          			AND pepart."positions"[pep_pos] > closest_pos
          			THEN
          				-- if the current pep position is before the alert word position
          				-- and it is after the last found closest position
          				-- update the closest postion and closest pep
          				closest_pep := pepart."PEP_ID";
          			    closest_pos := pepart."positions"[pep_pos];
          		 	END IF;          		
           		END LOOP; -- positions of pep in article        		
           		
            END LOOP; -- pep_arts 
            
	         IF closest_pep > 0 THEN
	            -- if a pep was found
	            -- Add an entry in the ALERTWORDS_ART table 
	            -- for the values found above.
	            
	            INSERT INTO finnova."ALERTWORDS_ART" 
				("AW_ID", "PEP_ID", "AW_POS", "PEP_POS", "ART_ID")
	            VALUES (aword."ID", closest_pep, aword_positions[counter], 
						closest_pos, art."ID");
	        END IF;                
 
         END LOOP;  -- positions of alert words
      END LOOP;  -- alert words loop
   END LOOP;     -- article loop

   RETURN 'All done!';
END;
$function$