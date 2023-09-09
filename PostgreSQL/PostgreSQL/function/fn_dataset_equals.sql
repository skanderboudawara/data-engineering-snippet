/**
Compare two database tables for equality.

This function takes the names of two database tables, `v_table_1_name` and `v_table_2_name`,
and compares them to determine if they are equal or different. The comparison is based on the
contents of the tables, and the function returns a Boolean value to indicate the result.

:param v_table_1_name: (TEXT) - The name of the first table to compare.
:param v_table_2_name: (TEXT) - The name of the second table to compare.

The function performs the following steps:

1. Builds a dynamic SQL query to calculate the count of rows that differ between the two tables.
   - The `EXCEPT` operator is used to find rows that exist in the first table but not in the second.
2. Stores the result count in the `result_count` variable.
3. Checks the result count and performs the following actions:
   - If `result_count` is 0, it raises a notice indicating that the tables are equal and returns TRUE.
   - If `result_count` is 1, it raises an exception indicating that the tables differ and returns FALSE.
   - If `result_count` is neither 0 nor 1, it raises an exception indicating an unexpected result count
     and returns FALSE.

:returns: (BOOLEAN) - TRUE if the tables are equal, FALSE if they differ.

@throws EXCEPTION - If an unexpected result count is encountered or if other errors occur during the operation.
 */ -- Drop the function if it already exists to avoid conflicts

DROP FUNCTION IF EXISTS f0005_test_equals(v_table_1_name text, v_table_2_name text);

-- Create or replace a new PostgreSQL function named project.f0005_test_equals
-- It takes two text parameters: v_table_1_name and v_table_2_name

CREATE OR REPLACE FUNCTION xf0005_test_equals(v_table_1_name text, v_table_2_name text) RETURNS BOOLEAN AS $$
DECLARE
    result_count INTEGER; -- Declare a variable to hold the result_count
BEGIN
    -- Build a dynamic SQL query to compare two tables for equality
    -- The query calculates the count of rows that are different between the two tables.
    -- It uses the EXCEPT operator to find rows that exist in the first table but not in the second.
    EXECUTE '
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM ' || v_table_1_name ||'
            EXCEPT
            SELECT *
            FROM ' || v_table_2_name ||'
        ) AS test_table'
    INTO result_count; -- Store the result count in the result_count variable

    -- Check the result count
    IF result_count = 0 THEN
        RAISE NOTICE 'Tables are equal.'; -- If there are no differing rows, raise a notice
        RETURN TRUE; -- Return TRUE to indicate that the tables are equal
    ELSIF result_count = 1 THEN
        RAISE EXCEPTION 'Tables differ.'; -- If there is exactly one differing row, raise an exception
        RETURN FALSE; -- Return FALSE to indicate that the tables differ
    ELSE
        RAISE EXCEPTION 'Unexpected result count: %', result_count; -- If result_count is neither 0 nor 1, raise an exception
        RETURN FALSE; -- Return FALSE to indicate an unexpected result count
    END IF;
END;
$$ LANGUAGE plpgsql;

