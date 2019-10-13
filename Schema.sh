#!/bin/bash

#########################################################
#
# Schema.sh
#   Copyright 2019.10.12 konoar
#
#########################################################

# Drop
psql -U postgres <<EOF
DROP DATABASE IF EXISTS ${USER};
DROP ROLE     IF EXISTS ${USER};
EOF

# Create
psql -U postgres <<EOF
CREATE ROLE     ${USER} WITH LOGIN;
CREATE DATABASE ${USER} WITH OWNER ${USER};
EOF

psql -U ${USER}  <<EOF

CREATE SCHEMA   TestPQ;

CREATE SEQUENCE TestPQ.S_TEST START WITH 1 INCREMENT BY 1;

CREATE FUNCTION TestPQ.F_GET_DEFAULT_TID() RETURNS CHAR(8) AS
\$\$

    SELECT 'TID' || TO_CHAR(NEXTVAL('TestPQ.S_TEST'), 'FM00000');

\$\$ LANGUAGE SQL;

CREATE TABLE TestPQ.R_TEST
(

    tid     CHAR(8)     NOT NULL DEFAULT TestPQ.F_GET_DEFAULT_TID(),
    version INTEGER     NOT NULL DEFAULT 1,
    value   VARCHAR(16) NOT NULL,
    ctime   TIMESTAMP   NOT NULL DEFAULT CAST(CURRENT_DATE AS TIMESTAMP),

    PRIMARY KEY(tid, version)

)
WITH
(
    FILLFACTOR = 90
);

CREATE INDEX I_TEST_1 ON TestPQ.R_TEST
(
    value
);

CREATE FUNCTION TestPQ.F_TRIGGER_TEST() RETURNS TRIGGER AS
\$\$

    BEGIN

        INSERT INTO TestPQ.R_TEST(tid, version, value)
            VALUES (OLD.tid, OLD.version + 1, NEW.value);

        RETURN NULL;

    END

\$\$ LANGUAGE PLPGSQL;

CREATE TRIGGER T_TEST_UPDATE BEFORE UPDATE ON TestPQ.R_TEST
FOR EACH ROW EXECUTE PROCEDURE TestPQ.F_TRIGGER_TEST();

CREATE FUNCTION TestPQ.F_SELECT_TEST
(
    target TIMESTAMP DEFAULT LOCALTIMESTAMP
)
RETURNS TABLE
(
    rid     BIGINT,
    tid     CHAR(8),
    version INTEGER,
    value   VARCHAR(16)

) AS \$\$

    SELECT
        ROW_NUMBER() OVER w1    AS rid,
        r1.tid                  AS tid,
        MAX(r1.version)         AS version,
        (

            SELECT
                r2.value
            FROM
                TestPQ.R_TEST r2
            WHERE
                r2.tid      = r1.tid
            AND
                r2.version  = MAX(r1.version)

        )                       AS value
    FROM
        TestPQ.R_TEST r1
    WHERE
        (

            SELECT
                r2.ctime
            FROM
                TestPQ.R_TEST r2
            WHERE
                r2.tid      = r1.tid
            AND
                r2.version  = r1.version

        )
        <= target 
    GROUP BY
        r1.tid
    WINDOW
        w1 AS(ORDER BY (

            SELECT
                r2.value
            FROM
                TestPQ.R_TEST r2
            WHERE
                r2.tid      = r1.tid
            AND
                r2.version  = MAX(r1.version)
        
        ) ASC)

\$\$ LANGUAGE SQL;

CREATE FUNCTION TestPQ.F_UPDATE_TEST
(
    target_tid      CHAR(8),
    target_value    VARCHAR(16)

) RETURNS INTEGER AS \$\$

    DECLARE

        target_version INTEGER;

    BEGIN

        SELECT
            MAX(version)
        INTO
            target_version
        FROM
            TestPQ.R_TEST
        WHERE
            tid     = target_tid;

        UPDATE
            TestPQ.R_TEST
        SET
            value   = target_value
        WHERE
            tid     = target_tid
        AND
            version = target_version;

        RETURN 0;

    END

\$\$ LANGUAGE PLPGSQL;

INSERT INTO TestPQ.R_TEST
(
    value,
    ctime
)
VALUES
    ('bbb', CAST(CURRENT_DATE - 2 AS TIMESTAMP)),
    ('ccc', CAST(CURRENT_DATE - 1 AS TIMESTAMP)),
    ('aaa', CAST(CURRENT_DATE - 0 AS TIMESTAMP)),
    ('ddd', CAST(CURRENT_DATE + 1 AS TIMESTAMP));

EOF

