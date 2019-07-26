USE ${DB};

-- Locate File that are only available on a single RACK.
-- These files need to be spread across more racks to support
-- Rack level resiliency.

WITH RACK_ASSOCIATION AS (
                         SELECT
                             parent
                           , file
                           , COLLECT_SET(r.host_rack) AS racks
                         FROM
                             NN_FILE_DATA n INNER JOIN IMPORTANT_DIRS i
                                                       ON LOCATE(i.directory, n.parent) > 0
                                 INNER JOIN            RACK_TOPOLOGY r
                                                       ON n.dn_hostname = r.host_fqdn
                         GROUP BY parent, FILE
                         HAVING
                             SIZE(racks) = 1 -- When only on one rack, you'll loose the file if the rack goes down.
                         )
SELECT *
FROM
    RACK_ASSOCIATION;
