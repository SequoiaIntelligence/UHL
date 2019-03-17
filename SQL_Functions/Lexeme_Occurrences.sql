CREATE OR REPLACE FUNCTION finnova.lexeme_occurrences
(_lexemes tsvector, _word text, _config regconfig, 
OUT lexeme_count integer, OUT lexeme_positions integer[])
 RETURNS record
 LANGUAGE plpgsql
AS $function$DECLARE
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
$function$