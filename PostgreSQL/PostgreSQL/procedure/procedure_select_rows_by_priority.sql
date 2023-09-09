/*
This procedure selects rows from a given table based on priority rules specified in a JSONB object.

:param v_table_name: (TEXT) - The name of the table to select rows from.
:param v_col_partitions: (TEXT[]) - An array of column names to use for partitioning rows.
:param v_col_prio: (JSONB) - A JSONB object specifying priority rules for columns.

The procedure performs the following steps:

1. Creates a temporary table for manipulation.
2. Loops through the JSONB object to identify priority rules:
   - For "max" values, it adds columns to the query for descending sorting.
   - For "min" values, it adds columns to the query for ascending sorting.
   - For "not_null" values, it adds a new column for priority, updating it based on null values.
   - For other values, it adds a new column for priority, mapping values to priorities.
3. Performs the final selection of rows by priority, saving the result in another temporary table.
4. Drops the original table and renames the temporary table to replace it.
5. Drops any temporary tables created during the process.

@throws EXCEPTION - If unexpected errors occur during the operation.
 */
DROP PROCEDURE IF EXISTS select_rows_by_priority(v_table_name TEXT, v_col_partitions TEXT[], v_col_prio JSONB);


CREATE OR REPLACE PROCEDURE select_rows_by_priority(v_table_name TEXT, v_col_partitions TEXT[], v_col_prio JSONB) LANGUAGE PLPGSQL AS $$
DECLARE
    key_col_prio                            TEXT;
    value_col_prio                          JSONB;
    query_full                              TEXT[] = ARRAY[null::TEXT];
    array_of_prio                           TEXT[] = ARRAY[null::TEXT];
    all_column_priority_created_to_drop     TEXT[] = ARRAY['DROP COLUMN row_bnb'];
BEGIN
    -- Create a temporary table for manipulation
    DROP TABLE IF EXISTS temp_select_rows_by_priority_result;
    EXECUTE 'CREATE TEMP TABLE temp_select_rows_by_priority_result AS SELECT * FROM ' || v_table_name;

    -- Loop through the JSONB object
    FOR key_col_prio, value_col_prio IN SELECT * FROM JSONB_EACH(v_col_prio) LOOP
        RAISE NOTICE '%: %', key_col_prio, value_col_prio;

        -- Check for "max" value in JSONB
        IF value_col_prio = '"max"' THEN
            query_full := query_full || ARRAY[key_col_prio || ' DESC'];

        -- Check for "min" value in JSONB
        ELSIF value_col_prio = '"min"' THEN
            query_full := query_full || ARRAY[key_col_prio || ' ASC'];

        -- Handle "not_null" value in JSONB
        ELSIF value_col_prio = '"not_null"' THEN
            -- Add a new column for priority
            EXECUTE '
                ALTER TABLE temp_select_rows_by_priority_result
                ADD COLUMN ' || key_col_prio || '_priority BIGINT;
            ';

            -- Save the column for later drop
            all_column_priority_created_to_drop := all_column_priority_created_to_drop || ARRAY['DROP COLUMN ' || key_col_prio || '_priority'];

            -- Update priority column based on null values
            EXECUTE '
                UPDATE
                    temp_select_rows_by_priority_result
                SET ' || key_col_prio || '_priority = CASE
                    WHEN ' || key_col_prio || ' IS NOT NULL THEN 0
                    ELSE 1
                END;
            ';

            query_full := query_full || ARRAY[key_col_prio || '_priority ASC'];

        -- Handle other JSONB values
        ELSE
            -- Add a new column for priority
            EXECUTE '
                ALTER TABLE temp_select_rows_by_priority_result
                ADD COLUMN ' || key_col_prio || '_priority BIGINT;
            ';

            -- Save the column for later drop
            all_column_priority_created_to_drop := all_column_priority_created_to_drop || ARRAY['DROP COLUMN ' || key_col_prio || '_priority'];

            -- Extract an array from JSONB value
            array_of_prio := ARRAY(SELECT jsonb_array_elements_text(value_col_prio));

            -- Map values to priorities
            FOR i IN 1..ARRAY_LENGTH(array_of_prio, 1) LOOP
                EXECUTE '
                    UPDATE
                        temp_select_rows_by_priority_result
                    SET ' || key_col_prio || '_priority = CASE
                        WHEN CAST(' || key_col_prio || ' AS TEXT) = ''' || CAST(array_of_prio[i] AS TEXT) || ''' THEN ' || i || '
                        ELSE ' || key_col_prio || '_priority 
                    END;
                ';
            END LOOP;

            -- Handle NULL values after mapping
            EXECUTE '
                UPDATE
                    temp_select_rows_by_priority_result
                SET ' || key_col_prio || '_priority = COALESCE(' || key_col_prio || '_priority, ' || ARRAY_LENGTH(array_of_prio, 1) + 1 || ');
            ';

            query_full := query_full || ARRAY[key_col_prio || '_priority ASC'];
        END IF;
    END LOOP;

    -- Perform the final selection of rows by priority
    EXECUTE '
        DROP TABLE IF EXISTS temp_select_rows_by_priority_final_selection;
        CREATE TEMP TABLE temp_select_rows_by_priority_final_selection AS
        SELECT
            *
        FROM (
            SELECT *,
                DENSE_RANK() OVER (PARTITION BY ' || ARRAY_TO_STRING(v_col_partitions, ' , ') || ' ORDER BY ' || ARRAY_TO_STRING(query_full, ' , ') || ' ) as row_bnb 
            FROM 
                temp_select_rows_by_priority_result    
        ) AS T
        WHERE T.row_bnb = 1;
    ';

    -- Drop columns used for priority
    EXECUTE '
        ALTER TABLE temp_select_rows_by_priority_final_selection
        ' || ARRAY_TO_STRING(all_column_priority_created_to_drop, ' , ') || ';
    ';

    -- Replace the original table with the final selection
    EXECUTE 'DROP TABLE ' || v_table_name;
    EXECUTE 'ALTER TABLE temp_select_rows_by_priority_final_selection RENAME TO ' || v_table_name;
    DROP TABLE IF EXISTS temp_select_rows_by_priority_result;
END;
$$;

