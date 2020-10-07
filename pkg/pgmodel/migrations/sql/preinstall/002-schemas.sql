
CREATE SCHEMA IF NOT EXISTS SCHEMA_CATALOG;
GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT ON TABLES TO  prom_reader;
GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO  prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT, INSERT, UPDATE,  DELETE ON TABLES TO prom_writer;

CREATE SCHEMA IF NOT EXISTS SCHEMA_PROM;
GRANT USAGE ON SCHEMA SCHEMA_PROM TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_EXT;
GRANT USAGE ON SCHEMA SCHEMA_EXT TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_SERIES;
GRANT USAGE ON SCHEMA SCHEMA_SERIES TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_SERIES TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_SERIES GRANT SELECT ON TABLES TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_METRIC;
GRANT USAGE ON SCHEMA SCHEMA_METRIC TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_METRIC TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_METRIC GRANT SELECT ON TABLES TO  prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA;
GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT ON TABLES TO prom_reader;
GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;

CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_SERIES;
GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT ON TABLES TO prom_reader;
GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;


CREATE SCHEMA IF NOT EXISTS SCHEMA_INFO;
GRANT USAGE ON SCHEMA SCHEMA_INFO TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_INFO TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_INFO GRANT SELECT ON TABLES TO prom_reader;

--

SELECT distributed_exec($migration_file$ CREATE SCHEMA IF NOT EXISTS SCHEMA_CATALOG; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_reader;  $migration_file$);
SELECT distributed_exec($migration_file$ GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_reader;  $migration_file$);
SELECT distributed_exec($migration_file$ ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT ON TABLES TO  prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_writer;  $migration_file$);
SELECT distributed_exec($migration_file$ GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO  prom_writer; $migration_file$);
SELECT distributed_exec($migration_file$ ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT, INSERT, UPDATE,  DELETE ON TABLES TO prom_writer; $migration_file$);

SELECT distributed_exec($migration_file$ CREATE SCHEMA IF NOT EXISTS SCHEMA_PROM; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_PROM TO prom_reader; $migration_file$);

SELECT distributed_exec($migration_file$ CREATE SCHEMA IF NOT EXISTS SCHEMA_EXT; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_EXT TO prom_reader; $migration_file$);

SELECT distributed_exec($migration_file$ CREATE SCHEMA IF NOT EXISTS SCHEMA_SERIES; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_SERIES TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_SERIES TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_SERIES GRANT SELECT ON TABLES TO prom_reader; $migration_file$);

SELECT distributed_exec($migration_file$ CREATE SCHEMA IF NOT EXISTS SCHEMA_METRIC;  $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_METRIC TO prom_reader;  $migration_file$);
SELECT distributed_exec($migration_file$ GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_METRIC TO prom_reader;  $migration_file$);
SELECT distributed_exec($migration_file$ ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_METRIC GRANT SELECT ON TABLES TO  prom_reader; $migration_file$);

SELECT distributed_exec($migration_file$ CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT ON TABLES TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_writer; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_writer; $migration_file$);
SELECT distributed_exec($migration_file$ ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer; $migration_file$);

SELECT distributed_exec($migration_file$ CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_SERIES; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT ON TABLES TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_writer; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_writer; $migration_file$);
SELECT distributed_exec($migration_file$ ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer; $migration_file$);


SELECT distributed_exec($migration_file$ CREATE SCHEMA IF NOT EXISTS SCHEMA_INFO; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT USAGE ON SCHEMA SCHEMA_INFO TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_INFO TO prom_reader; $migration_file$);
SELECT distributed_exec($migration_file$ ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_INFO GRANT SELECT ON TABLES TO prom_reader; $migration_file$);


-- the promscale extension contains optimized version of some
-- of our functions and operators. To ensure the correct version of the are
-- used, SCHEMA_EXT must be before all of our other schemas in the search path
DO $$
DECLARE
   new_path text;
BEGIN
   new_path := current_setting('search_path') || format(',%L,%L,%L,%L', 'SCHEMA_EXT', 'SCHEMA_PROM', 'SCHEMA_METRIC', 'SCHEMA_CATALOG');
   execute format('ALTER DATABASE %I SET search_path = %s', current_database(), new_path);
   execute format('SET search_path = %s', new_path);
END
$$;
