-- View: db.path_generated
ALTER DATABASE db;

DROP VIEW IF EXISTS db.path_generated CASCADE;

CREATE
OR REPLACE VIEW db.path_generated AS WITH RECURSIVE path_generated(
    -- Definition of the temp view that will be used for recursivity
    top_node
    , father
    , father_type
    , child
    , child_type
    , full_path
    , full_path_technical_key
    , last_father
    , quantity_link
    , quantity_link_unit
    , cumulated_quantity
    , cumulated_quantity_unit
    , depth_level
) AS (
    SELECT
        DISTINCT -- Definition of Level 0 and how it should be constructed
        TreeLinks.father as top_node
        , NULL :: text as father
        , NULL :: text as father_type
        , TreeLinks.father as child
        , TreeLinks.father_type as child_type
        , TreeLinks.father as full_path
        , TreeLinks.father as full_path_technical_key
        , TreeLinks.father AS last_father
        , CAST(1 as double precision) AS quantity_link
        , 'EA' :: text as quantity_link_unit
        , CAST(1 as double precision) as cumulated_quantity
        , 'EA' :: text AS cumulated_quantity_unit
        , 0 AS depth_level
    FROM
        db.tree_links TreeLinks
    WHERE
        TreeLinks.father = 'test_father'
    UNION
    ALL
    SELECT
        -- The new level from the inner join
        TreePath.top_node
        , TreeLinks.father as father
        , TreeLinks.father_type as father_type
        , TreeLinks.child as child
        , TreeLinks.child_type as child_type
        , TreePath.full_path || ' / ' || TreeLinks.child as full_path
        , (
            TreePath.full_path_technical_key || ' / ' || CONCAT_WS(
                '_',
                coalesce(TreeLinks.child, ''),
                coalesce(TreeLinks.quantity_link :: text, '')
                coalesce(TreeLinks.quantity_link_unit :: text, '')
            )
        ) as full_path_technical_key
        , TreeLinks.child AS last_father
        , TreeLinks.quantity_link as quantity_link
        , TreeLinks.quantity_link_unit as quantity_link_unit
        , TreePath.cumulated_quantity * TreeLinks.quantity_link as cumulated_quantity
        , TreePath.quantity_link_unit as cumulated_quantity_unit
        , TreePath.depth_level + 1
    FROM
        path_generated as TreePath
        INNER JOIN db.tree_links TreeLinks ON (TreePath.last_father = TreeLinks.father)
    WHERE
        TreeLinks.child IS NOT NULL
)
SELECT
    -- Final Selection 
    TreePath.top_node
    , TreePath.father
    , TreePath.father_type
    , TreePath.child                     AS part_number
    , TreePath.child_type                AS part_number_type
    , TreePath.full_path
    , TreePath.full_path_technical_key
    , TreePath.quantity_link
    , TreePath.quantity_link_unit
    , TreePath.cumulated_quantity
    , TreePath.cumulated_quantity_unit
    , TreePath.depth_level
FROM
    path_generated as TreePath
ORDER BY
    TreePath.top_node,
    TreePath.depth_level;

ALTER TABLE
    db.path_generated OWNER TO postgres;

COMMENT ON VIEW db.path_generated IS 'This view will allow the consultation of specified view regarding part attributes for Digital Costing topic';

SELECT
    *
FROM
    db.path_generated
