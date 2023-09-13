DROP TABLE IF EXISTS your_table;
CREATE TABLE your_table (
    father character varying,
    child character varying
);

-- Insert data into the table
INSERT INTO your_table (father, child)
VALUES
   ('A', 'B'),
   ('B', 'C'),
   ('C', 'D'),
   ('D', 'E'),
   ('E', 'C'),
   ('E', 'F');

-- to avoid infinite loops
WITH RECURSIVE all_links(
    father,
    child,
    depth,
    all_fathers
) AS (
    SELECT
        *,
        0 as depth,
        array[father] as all_fathers
    FROM 
        your_table
    WHERE
        father = 'C'
    UNION
    ALL
    SELECT
        YourTable.*,
        AllLinks.depth + 1 as depth,
        AllLinks.all_fathers || YourTable.child as all_fathers
    FROM
        all_links as AllLinks,
        your_table as YourTable
    WHERE
        YourTable.father = AllLinks.child 
		AND
        NOT YourTable.child = ANY(AllLinks.all_fathers)
)
SELECT *
FROM all_links

-- https://www.postgresql.org/docs/current/ddl-constraints.html
ALTER TABLE your_table
  ADD CONSTRAINT "father" CHECK (father <> 'C');
