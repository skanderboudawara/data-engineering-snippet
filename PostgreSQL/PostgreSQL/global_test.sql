DROP SCHEMA IF EXISTS test_aws CASCADE;
CREATE SCHEMA IF NOT EXISTS test_aws AUTHORIZATION postgres;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------
DROP TABLE IF EXISTS test_aws.t00_test_table;
CREATE TABLE test_aws.t00_test_table (
    	father 		CHARACTER VARYING
   	, child       	CHARACTER VARYING
	, item_number 	CHARACTER VARYING
);

-- Insert data into the table
INSERT INTO test_aws.t00_test_table (father, child, item_number)
VALUES
   ('A', 'B', '0000')
   ,('B', 'C', '0001')
   ,('C', 'D', '0002')
   ,('D', 'E', '0003')
   ,('E', 'F', '0004');

------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS test_aws.f00_updow_light(v_pn_ref TEXT);
CREATE OR REPLACE FUNCTION test_aws.f00_updow_light(v_pn_ref TEXT) RETURNS TABLE(
    father                                       CHARACTER VARYING
    , child                                      CHARACTER VARYING
    , item_number                                CHARACTER VARYING
    , depth                                      INT    
) AS 
$BODY$
BEGIN

DROP TABLE IF EXISTS t00_result_union_temp;

CREATE TEMP TABLE t00_result_union_temp AS (
    -- up bom for all the fathers
    WITH RECURSIVE up_bom AS (
        SELECT
            AllLinks.*
            , -1                    AS depth
            , array[AllLinks.child] AS all_elements
        FROM
            test_aws.t00_test_table AllLinks
        WHERE
            AllLinks.child = v_pn_ref
        UNION
        SELECT
            AllLinks.*
            , up_bom.depth - 1                      AS depth
            , up_bom.all_elements || AllLinks.child AS all_children
        FROM
            test_aws.t00_test_table AllLinks
            INNER JOIN up_bom ON AllLinks.child = up_bom.father
        WHERE
            NOT AllLinks.father = ANY(up_bom.all_elements)
    ),
    -- lower bom for all the children
    down_bom AS (
        SELECT
            AllLinks.*
            , 1                      AS depth
            , array[AllLinks.father] AS all_elements
        FROM
            test_aws.t00_test_table AllLinks
        WHERE
            AllLinks.father = v_pn_ref
        UNION
        SELECT
            AllLinks.*
            , down_bom.depth + 1                       AS depth
            , down_bom.all_elements || AllLinks.father AS all_fathers
        FROM
            test_aws.t00_test_table AllLinks
            INNER JOIN down_bom ON AllLinks.father = down_bom.child
        WHERE
            NOT AllLinks.child = ANY(down_bom.all_elements)
    )
    -- Distinct is needed
    SELECT
        DISTINCT *
    FROM (
        SELECT
                *
        FROM
            up_bom
        UNION
        SELECT
            *
        FROM
            down_bom
    ) AS union_up_down
);

ALTER TABLE t00_result_union_temp
DROP COLUMN all_elements;

RETURN QUERY (
    SELECT
        *
    FROM 
        t00_result_union_temp
);
END;
$BODY$ 
LANGUAGE PLPGSQL;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS test_aws.p01_compute_path_test_aws(v_pn_ref TEXT);
CREATE OR REPLACE PROCEDURE test_aws.p01_compute_path_test_aws(v_pn_ref TEXT) LANGUAGE PLPGSQL AS $BODY$
BEGIN

DROP TABLE IF EXISTS t01_path_light_test_aws_temp;
CREATE TEMP TABLE t01_path_light_test_aws_temp (
    top_node                       CHARACTER VARYING
    , father                       CHARACTER VARYING
    , child                        CHARACTER VARYING
    , full_path                    CHARACTER VARYING
    , item_number                  CHARACTER VARYING
    , bom_level                    INTEGER
);

WITH RECURSIVE  path_test_aws_raw(
-- Definition of the temp view that will be used for recursivity
    top_node
    , father
    , child
    , all_children
    , full_path
    , last_father
    , item_number
    , bom_level
) AS (
    SELECT
        DISTINCT -- Definition of Level 0 and how it should be constructed
        v_pn_ref                                                            AS top_node
        , NULL ::                                   CHARACTER VARYING       AS father
        , v_pn_ref                                                          AS child
        , ARRAY[v_pn_ref]                                                   AS all_children
        , v_pn_ref                                                          AS full_path
        , v_pn_ref                                                          AS last_father
        , NULL ::                                   CHARACTER VARYING       AS item_number
        , 0                                                                 AS bom_level
    FROM
        t01_path_light_test_aws AS UpDownBomPathAwsTest 
    WHERE
        UpDownBomPathAwsTest.child = v_pn_ref OR
        UpDownBomPathAwsTest.father = v_pn_ref
    UNION
    ALL
    SELECT
        -- The new level from the inner join
        PathAwsTestRaw.top_node
        , UpDownBomPathAwsTest.father                                          AS father
        , UpDownBomPathAwsTest.child                                           AS child
        , PathAwsTestRaw.all_children || UpDownBomPathAwsTest.child            AS all_children
        , PathAwsTestRaw.full_path || ' / ' || UpDownBomPathAwsTest.child      AS full_path
        , UpDownBomPathAwsTest.child                                           AS last_father
        , UpDownBomPathAwsTest.item_number                                     AS item_number
        , PathAwsTestRaw.bom_level + 1                                         AS bom_level
    FROM
        path_test_aws_raw AS PathAwsTestRaw
        INNER JOIN t01_path_light_test_aws AS UpDownBomPathAwsTest 
        ON (PathAwsTestRaw.last_father = UpDownBomPathAwsTest.father)
    WHERE
        UpDownBomPathAwsTest.child IS NOT NULL AND
        NOT UpDownBomPathAwsTest.child = ANY(PathAwsTestRaw.all_children)
)

INSERT INTO t01_path_light_test_aws_temp
SELECT DISTINCT
    -- Final Selection 
    PathAwsTestRaw.top_node
    , PathAwsTestRaw.father
    , PathAwsTestRaw.child
    , PathAwsTestRaw.full_path
    , PathAwsTestRaw.item_number
    , PathAwsTestRaw.bom_level
FROM 
    path_test_aws_raw PathAwsTestRaw;

DROP TABLE t01_path_light_test_aws;
ALTER TABLE t01_path_light_test_aws_temp RENAME TO t01_path_light_test_aws;
END;
$BODY$;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS test_aws.p01_select_and_rename_test_aws();
CREATE OR REPLACE PROCEDURE test_aws.p01_select_and_rename_test_aws() LANGUAGE PLPGSQL AS $BODY$
BEGIN

DROP TABLE IF EXISTS t01_path_light_test_aws_temp;
CREATE TEMP TABLE t01_path_light_test_aws_temp AS 
    SELECT
    PathAwsTestRaw.top_node                       AS "TOP_NODE"  -- Not in US (must be deleted ?)
    , PathAwsTestRaw.full_path                    AS "PATH"
    , PathAwsTestRaw.bom_level                    AS "LEVEL"
    , PathAwsTestRaw.father                       AS "PARENT_ELEMENT"
    , PathAwsTestRaw.child                        AS "CURRENT_ELEMENT"
    , PathAwsTestRaw.item_number                  AS "ITEM_NUMBER"
FROM 

    t01_path_light_test_aws PathAwsTestRaw
ORDER BY
    PathAwsTestRaw.full_path;

DROP TABLE t01_path_light_test_aws;
ALTER TABLE t01_path_light_test_aws_temp RENAME TO t01_path_light_test_aws;
END;
$BODY$;


------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS test_aws.f01_path_light_test_aws(v_pn_ref TEXT);

CREATE OR REPLACE FUNCTION test_aws.f01_path_light_test_aws(v_pn_ref TEXT) RETURNS TABLE(
    "TOP_NODE"                       CHARACTER VARYING
    , "PATH"                         CHARACTER VARYING
    , "LEVEL"                        INTEGER
    , "PARENT_ELEMENT"               CHARACTER VARYING
    , "CURRENT_ELEMENT"              CHARACTER VARYING
    , "ITEM_NUMBER"                  CHARACTER VARYING
) AS 
$BODY$ 
BEGIN 

DROP TABLE IF EXISTS t01_path_light_test_aws;
CREATE TEMP TABLE t01_path_light_test_aws AS SELECT * FROM test_aws.f00_updow_light(v_pn_ref);

CALL test_aws.p01_compute_path_test_aws(v_pn_ref);

CALL test_aws.p01_select_and_rename_test_aws();

RETURN QUERY SELECT * FROM t01_path_light_test_aws;

DROP TABLE t01_path_light_test_aws;
END;
$BODY$ 
LANGUAGE PLPGSQL;

------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------

-- EG API : SELECT * FROM test_aws.f01_path_light_test_aws('C');
