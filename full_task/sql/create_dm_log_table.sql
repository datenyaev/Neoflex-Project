-- Table: logs.dm_log_table

-- DROP TABLE IF EXISTS logs.dm_log_table;

CREATE TABLE IF NOT EXISTS logs.dm_log_table
(
    log_id integer NOT NULL DEFAULT nextval('logs.dm_log_table_log_id_seq'::regclass),
    process_name character varying(100) COLLATE pg_catalog."default",
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    description text COLLATE pg_catalog."default",
    rows_inserted integer,
    CONSTRAINT dm_log_table_pkey PRIMARY KEY (log_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS logs.dm_log_table
    OWNER to postgres;