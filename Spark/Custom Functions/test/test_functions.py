import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import ColumnIsMissing, add_order_columns, array_max, array_min, arrays_zip, col, expr, filter_, lit, sort_array, transform_, when
from pyspark_custom_functions.sql.functions import IllegalArgumentError, add_order_columns, extract_array_occurence, unpivot_table


@pytest.fixture
def test_data(spark):
    data = [(1, "A"), (2, "B"), (3, "C")]
    schema = StructType([StructField("id", StringType()), StructField("col", StringType())])
    return spark.createDataFrame(data, schema)


@pytest.mark.parametrize(
    "priority_cols, expected_columns",
    [
        ({"col": ["B", "A", "C"]}, ["id", "col", "col_priority"]),
        ({"col": ["min"]}, ["id", "col", "col_priority"]),
        ({"col": ["max"]}, ["id", "col", "col_priority"]),
        ({"col": ["not_null"]}, ["id", "col", "col_priority"]),
        ({"col": ["B", "A", "C"], "id": ["min", "max"]}, ["id", "col", "col_priority", "id_priority"]),
    ],
)
def test_add_order_columns(test_data, priority_cols, expected_columns):
    """
    Test for add_order_columns function.

    Arrange:
        test_data (DataFrame): The test DataFrame.
        priority_cols (dict): The priority order for each column.
        expected_columns (list): The expected columns in the resulting DataFrame.

    Act:
        result = add_order_columns(test_data, priority_cols)

    Assert:
        assert result.columns == expected_columns
    """
    result = add_order_columns(test_data, priority_cols)
    assert result.columns == expected_columns


def test_add_order_columns_missing_column(test_data):
    """
    Test for add_order_columns function with missing column.

    Arrange:
        test_data (DataFrame): The test DataFrame.

    Act/Assert:
        Verify that ColumnIsMissing is raised when a column specified in priority_cols is not present in the DataFrame.
    """
    with pytest.raises(ColumnIsMissing):
        priority_cols = {"missing_col": ["A", "B", "C"]}
        add_order_columns(test_data, priority_cols)


@pytest.mark.parametrize(
    "column_array, tolerance_user, most_frequent, expected",
    [
        (col("array_col"), 0.0, True, col("array_col")),
        (col("array_col"), 0.5, True, col("array_col")),
        (col("array_col"), 1.0, True, col("array_col")),
        (col("array_col"), 0.0, False, col("array_col")),
        (col("array_col"), 0.5, False, col("array_col")),
        (col("array_col"), 1.0, False, col("array_col")),
    ],
)
def test_extract_array_occurence(column_array, tolerance_user, most_frequent, expected):
    result = extract_array_occurence(column_array, tolerance_user, most_frequent)
    assert result == expected


@pytest.mark.parametrize(
    "column_array, tolerance_user, most_frequent",
    [
        ("array_col", 0.0, True),
        (col("array_col"), 1.5, True),
        (col("array_col"), -0.5, True),
        (col("array_col"), 0.0, "True"),
        (col("array_col"), 0.5, 1),
        (col("array_col"), 1.0, None),
    ],
)
def test_extract_array_occurence_invalid_input(column_array, tolerance_user, most_frequent):
    with pytest.raises((TypeError, ValueError)):
        extract_array_occurence(column_array, tolerance_user, most_frequent)


@pytest.fixture
def dataframe(spark):
    data = [
        ("A", 1, 10, 100),
        ("B", 2, 20, 200),
        ("C", 3, 30, 300),
    ]
    columns = ["key", "col1", "col2", "col3"]
    return spark.createDataFrame(data, columns)


@pytest.mark.parametrize(
    "pivot_key, column_to_unpivot, col_key_name, col_value_name, expected_columns",
    [
        (["key"], ["col1", "col2", "col3"], "key", "value", ["key", "value"]),
        (["key"], ["col1"], "key", "value", ["key", "value"]),
    ],
)
def test_unpivot_table_positive(dataframe, pivot_key, column_to_unpivot, col_key_name, col_value_name, expected_columns):
    # Arrange

    # Act
    result = dataframe.unpivot_table(pivot_key, column_to_unpivot, col_key_name, col_value_name)

    # Assert
    assert isinstance(result, DataFrame)
    assert result.columns == expected_columns


@pytest.mark.parametrize(
    "pivot_key, column_to_unpivot, col_key_name, col_value_name, expected_error",
    [
        (["key"], "col1", "key", "value", TypeError),
        (["key"], ["col1", "col2"], "key", "value", IllegalArgumentError),
        (["key"], ["col1", "col2", "col4"], "key", "value", ColumnIsMissing),
        (["key"], ["col1", "col2", "col3"], 123, "value", TypeError),
        (["key"], ["col1", "col2", "col3"], "key", 123, TypeError),
    ],
)
def test_unpivot_table_negative(dataframe, pivot_key, column_to_unpivot, col_key_name, col_value_name, expected_error):
    # Arrange

    # Act & Assert
    with pytest.raises(expected_error):
        dataframe.unpivot_table(pivot_key, column_to_unpivot, col_key_name, col_value_name)
