-- Create the table with the specified schema
DROP TABLE IF EXISTS df_input_test_select_rows_by_priority;
CREATE TEMP TABLE df_input_test_select_rows_by_priority (
    field1 integer,
    field2 character varying,
    field3 character varying,
    field4 character varying,
    field5 character varying
);

-- Insert data into the table
INSERT INTO df_input_test_select_rows_by_priority (field1, field2, field3, field4, field5)
VALUES
    --  ('field1', 'field2', 'field3', 'field4', 'field5'),  # noqa
        (1       , 'PART1' , 'A'     , 'T'     , '1'     ),
        (2       , 'PART1' , 'A'     , 'T'     , '1'     ),
        (3       , 'PART1' , 'A'     , 'Y'     , '1'     ),
        (4       , 'PART1' , 'A'     , 'Z'     , '0'     ),
        (5       , 'PART1' , 'A'     , NULL    , '0'     ),
        (6       , 'PART1' , 'B'     , NULL    , '1'     ),
        (7       , 'PART1' , 'B'     , 'Y'     , '1'     ),
        (8       , 'PART1' , 'B'     , 'Z'     , '1'     ),
        (9       , 'PART1' , 'B'     , 'T'     , '1'     ),
        (10      , 'PART1' , 'C'     , 'T'     , '1'     ),
        (11      , 'PART1' , 'C'     , 'Z'     , '1'     ),
        (12      , 'PART2' , 'B'     , 'Y'     , '1'     ),
        (13      , 'PART2' , 'B'     , NULL    , '1'     ),
        (14      , 'PART2' , 'B'     , 'Y'     , '0'     ),
        (15      , 'PART2' , 'B'     , 'Y'     , '1'     ),
        (16      , 'PART2' , 'C'     , NULL    , '1'     ),
        (17      , 'PART2' , 'C'     , NULL    , '1'     ),
        (18      , 'PART3' , 'A'     , NULL    , '1'     ),
        (19      , 'PART3' , NULL    , 'T'     , '1'     );
		
-- select * from df_input_test_select_rows_by_priority;

call select_rows_by_priority('df_input_test_select_rows_by_priority', '{"field2"}', '{"field3": "A,B,C", "field4": "T,Z,Y"}');

-- Create the table with the specified schema
DROP TABLE IF EXISTS df_output_select_rows_by_priority;
CREATE TEMP TABLE df_output_select_rows_by_priority (
    field1 integer,
    field2 character varying,
    field3 character varying,
    field4 character varying,
    field5 character varying
);

-- Insert data into the table
INSERT INTO df_output_select_rows_by_priority (field1, field2, field3, field4, field5)
VALUES
    --  ('field1', 'field2', 'field3', 'field4', 'field5'),  # noqa
        (1       , 'PART1' , 'A'     , 'T'     , '1'     ),
        (2       , 'PART1' , 'A'     , 'T'     , '1'     ),
        (12      , 'PART2' , 'B'     , 'Y'     , '1'     ),
        (14      , 'PART2' , 'B'     , 'Y'     , '0'     ),
        (15      , 'PART2' , 'B'     , 'Y'     , '1'     ),
        (18      , 'PART3' , 'A'     , NULL    , '1'     );



-- Check if the data in table1 is the same as in table2
SELECT f0002_test_equals('df_input_test_select_rows_by_priority', 'df_output_select_rows_by_priority');

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- Create the table with the specified schema
DROP TABLE IF EXISTS df_input_test_select_rows_by_priority;
CREATE TEMP TABLE df_input_test_select_rows_by_priority (
    field1 integer,
    field2 character varying,
    field3 character varying,
    field4 character varying,
    field5 character varying
);

-- Insert data into the table
INSERT INTO df_input_test_select_rows_by_priority (field1, field2, field3, field4, field5)
VALUES
    --  ('field1', 'field2', 'field3', 'field4', 'field5'),  # noqa
        (1       , 'PART1' , 'A'     , 'T'     , '1'     ),
        (2       , 'PART1' , 'A'     , 'T'     , '1'     ),
        (3       , 'PART1' , 'A'     , 'Y'     , '1'     ),
        (4       , 'PART1' , 'A'     , 'Z'     , '0'     ),
        (5       , 'PART1' , 'A'     , NULL    , '0'     ),
        (6       , 'PART1' , 'B'     , NULL    , '1'     ),
        (7       , 'PART1' , 'B'     , 'Y'     , '1'     ),
        (8       , 'PART1' , 'B'     , 'Z'     , '1'     ),
        (9       , 'PART1' , 'B'     , 'T'     , '1'     ),
        (10      , 'PART1' , 'C'     , 'T'     , '1'     ),
        (11      , 'PART1' , 'C'     , 'Z'     , '1'     ),
        (12      , 'PART2' , 'B'     , 'Y'     , '1'     ),
        (13      , 'PART2' , 'B'     , NULL    , '1'     ),
        (14      , 'PART2' , 'B'     , 'Y'     , '0'     ),
        (15      , 'PART2' , 'B'     , 'Y'     , '1'     ),
        (16      , 'PART2' , 'C'     , NULL    , '1'     ),
        (17      , 'PART2' , 'C'     , NULL    , '1'     ),
        (18      , 'PART3' , 'A'     , NULL    , '1'     ),
        (19      , 'PART3' , NULL    , 'T'     , '1'     );
		
-- select * from df_input_test_select_rows_by_priority;


call select_rows_by_priority(
	'df_input_test_select_rows_by_priority',
	'{"field2"}',
	'{"field3": "A,B,C", "field4": "not_null", "field5": "max"}'
);

-- Create the table with the specified schema
DROP TABLE IF EXISTS df_output_select_rows_by_priority;
CREATE TEMP TABLE df_output_select_rows_by_priority (
    field1 integer,
    field2 character varying,
    field3 character varying,
    field4 character varying,
    field5 character varying
);

-- Insert data into the table
INSERT INTO df_output_select_rows_by_priority (field1, field2, field3, field4, field5)
VALUES
    --  ('field1', 'field2', 'field3', 'field4', 'field5'),  # noqa
		(1       , 'PART1' , 'A'     , 'T'     , '1'     ),
		(2       , 'PART1' , 'A'     , 'T'     , '1'     ),
		(3       , 'PART1' , 'A'     , 'Y'     , '1'     ),
		(12      , 'PART2' , 'B'     , 'Y'     , '1'     ),
		(15      , 'PART2' , 'B'     , 'Y'     , '1'     ),
		(18      , 'PART3' , 'A'     , NULL    , '1'     );



-- Check if the data in table1 is the same as in table2
SELECT f0002_test_equals('df_input_test_select_rows_by_priority', 'df_output_select_rows_by_priority');
