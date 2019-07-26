USE ${DB};

-- Locate Files that are under replicated
-- Skip Directories and 0 byte files.
WITH REPL   AS (
               SELECT DISTINCT
                   parent
                 , file
                 , replication
               FROM
                   -- Join with important base directories to cut down on noise
                   NN_FILE_DATA n INNER JOIN IMPORTANT_DIRS i
                                             ON LOCATE(i.directory, n.parent) > 0
               WHERE
                     file IS NOT NULL -- skip directories
                 AND size > 0 -- zero byte files are NOT replicated
               )
   , ACTUAL AS (
               SELECT
                   parent
                 , file
                 , count(dn_ip) AS replicated
               FROM
                   -- Join with important base directories to cut down on noise
                   NN_FILE_DATA n INNER JOIN IMPORTANT_DIRS i
                                             ON LOCATE(i.directory, n.parent) > 0
               WHERE
                     FILE IS NOT NULL -- skip directories
                 AND size > 0 -- zero byte files are NOT replicated
               GROUP BY parent, file
               )
SELECT
    r.parent
  , r.file
  , r.replication
  , a.replicated
FROM
    REPL r INNER JOIN ACTUAL a
                      ON r.parent = a.parent AND r.file = a.file AND r.replication > a.replicated;
