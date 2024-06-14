/* gp_relaccess_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_relaccess_stats" to load this file. \quit

CREATE TABLE relaccess_stats (
    relid Oid,
    relname Name,
    last_user_id Oid,
    last_read timestamp,
    last_write timestamp,
    n_select_queries int,
    n_insert_queries int,
    n_update_queries int,
    n_delete_queries int,
    n_truncate_queries int
) DISTRIBUTED BY (relid);

CREATE FUNCTION relaccess_stats_dump()
RETURNS void
AS 'MODULE_PATHNAME', 'relaccess_stats_dump'
LANGUAGE C EXECUTE ON MASTER;

CREATE FUNCTION relaccess_stats_update()
RETURNS void
AS 'MODULE_PATHNAME', 'relaccess_stats_update'
LANGUAGE C EXECUTE ON MASTER;

CREATE FUNCTION relaccess_stats_fillfactor()
RETURNS INT2
AS 'MODULE_PATHNAME', 'relaccess_stats_fillfactor'
LANGUAGE C EXECUTE ON MASTER;

CREATE FUNCTION __relaccess_upsert_from_dump_file(path varchar) RETURNS VOID
LANGUAGE plpgsql VOLATILE EXECUTE ON MASTER AS
$func$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS relaccess_stats_tmp';
    EXECUTE 'CREATE TEMP TABLE relaccess_stats_tmp (LIKE relaccess_stats) distributed by (relid)';
    EXECUTE 'DROP TABLE IF EXISTS relaccess_stats_tmp_aggregated';
    EXECUTE 'CREATE TEMP TABLE relaccess_stats_tmp_aggregated (LIKE relaccess_stats) distributed by (relid)';
    EXECUTE format('COPY relaccess_stats_tmp FROM ''%s'' WITH (FORMAT ''csv'', DELIMITER '','')', $1);
    EXECUTE 'WITH aggregated_wo_relname_and_user AS (
        SELECT relid, max(last_read) AS last_read, max(last_write) AS last_write, sum(n_select_queries) AS n_select_queries,
            sum(n_insert_queries) AS n_insert_queries, sum(n_update_queries) AS n_update_queries, sum(n_delete_queries) AS n_delete_queries, sum(n_truncate_queries) AS n_truncate_queries
        FROM relaccess_stats_tmp GROUP BY relid
    )
    INSERT INTO relaccess_stats_tmp_aggregated
    SELECT relid,
        (SELECT relname FROM relaccess_stats_tmp w WHERE w.relid = wo.relid AND greatest(wo.last_read, wo.last_write) IN (w.last_read, w.last_write) LIMIT 1) AS relname,
        (SELECT last_user_id FROM relaccess_stats_tmp w WHERE w.relid = wo.relid AND greatest(wo.last_read, wo.last_write) IN (w.last_read, w.last_write) LIMIT 1) AS last_user_id,
        last_read,
        last_write,
        n_select_queries,
        n_insert_queries,
        n_update_queries,
        n_delete_queries,
        n_truncate_queries FROM aggregated_wo_relname_and_user AS wo';
    EXECUTE 'DROP TABLE IF EXISTS relaccess_stats_tmp';
    EXECUTE 'INSERT INTO relaccess_stats
        SELECT relid, relname, last_user_id, last_read, last_write, 0, 0, 0, 0, 0
        FROM relaccess_stats_tmp_aggregated stage
        WHERE NOT EXISTS (
            SELECT 1 FROM relaccess_stats orig WHERE orig.relid = stage.relid)';
    EXECUTE 'UPDATE relaccess_stats orig SET
        relname = stage.relname,
        last_user_id = stage.last_user_id,
        last_read = stage.last_read,
        last_write = stage.last_write,
        n_select_queries = orig.n_select_queries + stage.n_select_queries,
        n_insert_queries = orig.n_insert_queries + stage.n_insert_queries,
        n_update_queries = orig.n_update_queries + stage.n_update_queries,
        n_delete_queries = orig.n_delete_queries + stage.n_delete_queries,
        n_truncate_queries = orig.n_truncate_queries + stage.n_truncate_queries
    FROM relaccess_stats_tmp_aggregated stage
        WHERE orig.relid = stage.relid';
    EXECUTE 'DROP TABLE IF EXISTS relaccess_stats_tmp_aggregated';
END
$func$;

CREATE FUNCTION relaccess_stats_init() RETURNS VOID AS
$$
    WITH relations AS (
        SELECT oid as relid, relname, relowner FROM pg_catalog.pg_class WHERE relkind in ('r', 'v', 'm', 'f', 'p')
    )
    INSERT INTO relaccess_stats
        SELECT relid, relname, relowner, '2000-01-01 03:00:00', '2000-01-01 03:00:00', 0, 0, 0, 0, 0
        FROM relations AS all_rels WHERE NOT EXISTS(SELECT 1 FROM relaccess_stats orig WHERE orig.relid = all_rels.relid);
$$ LANGUAGE SQL VOLATILE EXECUTE ON MASTER;

-- This utility view shows **ONLY** stats on **EXISTING** partitioned tables in aggregated form
CREATE VIEW relaccess_stats_root_tables_aggregated AS (
    WITH RECURSIVE parents AS (
        SELECT inhrelid AS child, inhparent AS parent FROM pg_inherits
        UNION ALL
        SELECT prev.child, next.inhparent AS parent FROM parents AS prev JOIN pg_inherits AS next ON prev.parent = next.inhrelid
    ), part_to_root_mapping AS (
        SELECT DISTINCT child AS partid, min(parent) OVER (partition BY child) AS rootid FROM parents
    ), parts_including_roots AS (
        SELECT rootid as partid, rootid FROM (SELECT DISTINCT rootid FROM part_to_root_mapping) AS p
        UNION
        SELECT * FROM part_to_root_mapping
    ), with_root_id AS (
        SELECT part_tbl.rootid, stats.* FROM relaccess_stats stats JOIN parts_including_roots part_tbl ON (stats.relid = part_tbl.partid)
    ), without_last_user AS (
        SELECT rootid AS relid,
            rootid::regclass::text AS relname,
            max(last_read) AS last_read,
            max(last_write) AS last_write,
            sum(n_select_queries) AS n_select_queries,
            sum(n_insert_queries) AS n_insert_queries,
            sum(n_update_queries) AS n_update_queries,
            sum(n_delete_queries) AS n_delete_queries,
            sum(n_truncate_queries) AS n_truncate_queries
        FROM with_root_id outer_tbl GROUP BY rootid
    )
    SELECT relid,
        relname,
        (SELECT last_user_id FROM with_root_id w WHERE w.rootid = wo.relid AND greatest(wo.last_read, wo.last_write) IN (w.last_read, w.last_write) LIMIT 1) AS last_user_id,
        last_read,
        last_write,
        n_select_queries,
        n_insert_queries,
        n_update_queries,
        n_delete_queries,
        n_truncate_queries
    FROM without_last_user wo
);
