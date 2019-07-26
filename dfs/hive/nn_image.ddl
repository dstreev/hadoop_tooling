CREATE DATABASE IF NOT EXISTS ${DB};

USE ${DB};

DROP TABLE IF EXISTS NN_FILE_DATA_EXT;

-- lsp call to extract data
-- lsp -R -d -1 -f permissions_long,replication,size,user,parent,file,datanode_info -o /warehouse/tablespace/external/hive/priv_dstreev.db/BM90-NN /
CREATE EXTERNAL TABLE IF NOT EXISTS NN_FILE_DATA_EXT (
    permissions_long STRING,
    replication      INTEGER,
    size             BIGINT,
    user_            STRING,
    parent           STRING,
    file             STRING,
    dn_ip            STRING,
    dn_hostname      STRING,
    block_name       STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '/warehouse/tablespace/external/hive/priv_dstreev.db/BM90-NN';

-- Check for content
SELECT *
FROM
    NN_FILE_DATA_EXT
LIMIT 2;

DROP TABLE IF EXISTS NN_FILE_DATA;

-- Create Managed Table for downstream
CREATE TABLE NN_FILE_DATA AS SELECT
                                 permissions_long
                               , replication
                               , size
                               , user_
                                 -- Trim the protocol off
                               , regexp_extract(parent, 'hdfs://([^/]+)(.*)', 2) AS parent
                               , CASE (file) WHEN (" ") THEN NULL ELSE file END  AS file
                               , dn_ip
                               , dn_hostname
                               , block_name
                             FROM
                                 NN_FILE_DATA_EXT;

-- List of Important directories to consider.
-- Because scans across all directories is wasteful.
DROP TABLE IF EXISTS IMPORTANT_DIRS;
CREATE TABLE IF NOT EXISTS IMPORTANT_DIRS (
    directory STRING
);

INSERT INTO TABLE
    IMPORTANT_DIRS (directory)
VALUES ("/apps")
     , ("/user")
     , ("/warehouse");

-- Rack Topology
DROP TABLE IF EXISTS RACK_TOPOLOGY;
CREATE TABLE IF NOT EXISTS RACK_TOPOLOGY (
    host_rack STRING,
    host_fqdn STRING,
    host_ip STRING
);

INSERT INTO TABLE RACK_TOPOLOGY ( host_rack, host_fqdn, host_ip) VALUES
("/A","os10.streever.local","10.0.1.20"),
("/A","os30.streever.local","10.0.1.40"),
("/C","os19.streever.local","10.0.1.29"),
("/B","os14.streever.local","10.0.1.24"),
("/B","os31.streever.local","10.0.1.41"),
("/A","os11.streever.local","10.0.1.21"),
("/C","os18.streever.local","10.0.1.28"),
("/B","os15.streever.local","10.0.1.25"),
("/C","os17.streever.local","10.0.1.27"),
("/C","os32.streever.local","10.0.1.42"),
("/A","os12.streever.local","10.0.1.22"),
("/B","os16.streever.local","10.0.1.26"),
("/A","os13.streever.local","10.0.1.23");

select * from RACK_TOPOLOGY;