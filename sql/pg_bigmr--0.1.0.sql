-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bigmr" to load this file. \quit

-- create the operator class for gin
CREATE OPERATOR CLASS gin_bigm_ops
FOR TYPE text USING gin
AS
        OPERATOR        1       pg_catalog.~~ (text, text),
        OPERATOR        2       =% (text, text),
        FUNCTION        1       bigmtextcmp (text, text),
        FUNCTION        2       gin_extract_value_bigm (text, internal),
        FUNCTION        3       gin_extract_query_bigm (text, internal, int2, internal, internal, internal, internal),
        FUNCTION        4       gin_bigm_consistent (internal, int2, text, int4, internal, internal, internal, internal),
        FUNCTION        5       gin_bigm_compare_partial (text, text, int2, internal),
        STORAGE         text;

ALTER OPERATOR FAMILY gin_bigm_ops USING gin ADD
        FUNCTION        6    (text, text) gin_bigm_triconsistent (internal, int2, text, int4, internal, internal, internal);
