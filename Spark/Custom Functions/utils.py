from typing import Union
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import expr, col, sort_array, transform_, when, lit, array_max, array_min, filter_, arrays_zip, broadcast, dense_rank
from pyspark.sql.window import Window

class IllegalArgumentError(ValueError):
    """IllegalArgumentError"""

class ColumnIsMissing(ValueError):
    """ColumnIsMissing"""

def remove_left_zeros(
    column: Union[str, Column],
    number: int = 1
) -> Column:
    """
    Remove the leading zeros for the given column

    :param column: (str or Column), source column.

    :param number: (int, optional), Delete 0s only if there are at least "number" (or more) 0. Default to 1.

    :returns: (Column), Column w/o N left zeros.
    """
    if not isinstance(column, (str, Column)):
        raise TypeError("column must be a str or a Column")
    if not isinstance(number, int):
        raise TypeError("number must be an Integer")

    column = col(column) if isinstance(column, str) else column

    # Finds left leading zeros (at least "number") -> https://regex101.com/r/OZBFUP/1
    left_zeros_re = "^0{number,}".replace("number", str(number))

    return regexp_replace(column, left_zeros_re, "")


def remove_non_alphanumeric(
    column: Union[str, Column],
) -> Column:
    """
    Remove the non alphanumeric characters for the given column.

    :param column: (str or Column), source column.

    :returns: (Column), Column w/o alphanumeric values.
    """
    if not isinstance(column, (str, Column)):
        raise TypeError("column must be a str or a Column")

    column = col(column) if isinstance(column, str) else column

    return regexp_replace(column, C.SPECIAL_CHARS, "")
    
def unpivot_table(
    self: DataFrame,
    pivot_key: Union[str, Column],
    column_to_unpivot: list,
    col_key_name: str = "key",
    col_value_name: str = "value",
) -> DataFrame:
    """
    Unpivots a DataFrame by transforming multiple columns into key-value pairs.

    Args:
        self (DataFrame): The target DataFrame.
        pivot_key (Union[str, Column]): The pivot key(s) to keep in the select.
        column_to_unpivot (list): The list of columns to unpivot.
        col_key_name (str, optional): The name of the column containing keys. Defaults to "key".
        col_value_name (str, optional): The name of the column containing values. Defaults to "value".

    Returns:
        DataFrame: The DataFrame with the specified columns unpivoted.

    Raises:
        TypeError: If pivot_key is not a str or Column, column_to_unpivot is not a list, col_key_name or col_value_name is not a str.
        IllegalArgumentError: If column_to_unpivot has less than 2 columns.
        ColumnIsMissing: If a column specified in column_to_unpivot is not present in the DataFrame.

    Example:
        ```python
        df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["col1", "col2", "col3"])
        pivot_key = "col1"
        column_to_unpivot = ["col2", "col3"]
        result = df.unpivot_table(pivot_key, column_to_unpivot)
        result.show()
        ```
    """
    if isinstance(pivot_key, str):
        pivot_key = [pivot_key]

    if not isinstance(pivot_key, list):
        raise TypeError("pivot_key must be a list")

    if not isinstance(column_to_unpivot, list):
        raise TypeError("column_to_unpivot must be a list")

    if not isinstance(col_key_name, str):
        raise TypeError("col_key_name must be a str")

    if not isinstance(col_value_name, str):
        raise TypeError("col_value_name must be a str")

    df_columns = self.columns

    if len(column_to_unpivot) <= 1:
        raise IllegalArgumentError("column_to_unpivot must have at least 2 columns to unpivot")

    for col_name in column_to_unpivot:
        if col_name not in df_columns:
            raise ColumnIsMissing(f"Wrong Value: {col_name} is not in dataframe")

    all_columns = ", ".join([f"'{col_name}', {col_name}" for col_name in column_to_unpivot])
    unpivot_expr = f"stack({len(column_to_unpivot)}, {all_columns}) as ({col_key_name},{col_value_name})"

    self = self.select(
        *pivot_key,
        transform_(sort_array(transform_(col(column_to_unpivot), lambda y: transform_(col(column_to_unpivot), lambda x: when(x.between(y*(1-0.0), y*(1+0.0)), lit(1.0)).otherwise(lit(0.0))))), 
        expr(unpivot_expr)
    )

    return self


def extract_array_occurence(
    column_array: Union[str, Column],
    tolerance_user : float = 0.0,
    most_frequent : bool = True,
) -> Column:
    if not isinstance(column_array, (str, Column)):
        raise TypeError("column_array must be a string or Column")

    if not isinstance(tolerance_user, float):
        raise TypeError("tolerance_user must be a float")

    if not (0.0 <= tolerance_user <= 1.0):
        raise ValueError("argument tolerance_user must be between 0 & 1 in float")

    if not isinstance(most_frequent, bool):
        raise TypeError("most_frequent must be a boolean")

    column_array = column_array if isinstance(column_array, Column) else col(column_array)
    column_array = sort_array(column_array, False)

    compute_occurences = transform_(
        column_array,
        lambda y : array_max(transform_(
            column_array,
            lambda x: when(x.between(y*(1-tolerance_user), y*(1+tolerance_user)), lit(1.0)).otherwise(lit(0.0))
        ))
    )

    max_or_min_occurence = array_max(compute_occurences) if most_frequent else array_min(compute_occurences)

    return filter_(
        arrays_zip(
            column_array.alias("tmp_array_of_working_columns"),
            compute_occurences.alias("compute_occurences")
        ),
        lambda x: x["compute_occurences"] == max_or_min_occurence
    ).tmp_array_of_working_columns


def add_order_columns(
    df: DataFrame,
    priority_cols: dict
) -> DataFrame:
    """
    Adds columns with the priority order provided by the user to the DataFrame.

    Args:
        df (DataFrame): The target DataFrame.
        priority_cols (dict): A dictionary specifying the priority order for each column. The keys are the column names, and the values can be either a list of priorities, "min" or "max" to prioritize the minimum or maximum value in the column, or "not_null" to prioritize non-null values.

    Returns:
        DataFrame: The DataFrame with the added priority columns.

    Raises:
        TypeError: If df is not a DataFrame or priority_cols is not a dict.
        ColumnIsMissing: If a column specified in priority_cols is not present in the DataFrame.

    Example:
        ```python
        df = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "col"])
        priority_cols = {"col": ["B", "A", "C"]}
        result = add_order_columns(df, priority_cols)
        result.show()
        ```
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a DataFrame")

    if not isinstance(priority_cols, dict):
        raise TypeError("priority_cols must be a dict")

    columns_prio = priority_cols.keys()

    for col_index in columns_prio:
        if col_index not in df.columns:
            raise ColumnIsMissing(f" {col_index} is not in df columns")

    new_priority_cols = {}

    for column, priorities in priority_cols.items():
        if (priorities == "min") | (priorities == "max"):
            df = df.withColumn(
                f"{column}_priority",
                col(column)
            )
        elif priorities == "not_null":
            df = df.withColumn(
                f"{column}_priority",
                when(
                    col(column).isNotNull(),
                    lit("0").cast("string")
                ).otherwise(lit("1").cast("string"))
            )
        else:
            new_priority_cols[column] = priorities

    cols_priority = []

    for column, priority_list in new_priority_cols.items():

        col_priority = f"{column}_priority"
        cols_priority.append(col_priority)

        priorities = [
            (value, priority) for priority, value in enumerate(priority_list)
        ]

        schema_priority = StructType([
            StructField("_value_"   , StringType()),
            StructField(col_priority, StringType()),
        ])

        data_priority = df.sql_ctx.createDataFrame(data=priorities, schema=schema_priority).cache()

        df = df.join(
            broadcast(data_priority),
            df[column].eqNullSafe(data_priority["_value_"]),
            "left"
        ).drop("_value_")

    for column, col_priority in zip(new_priority_cols, cols_priority):
        df = df.withColumn(
            col_priority,
            when(
                col(col_priority).isNull(),
                len(new_priority_cols[column])
            ).otherwise(col(col_priority))
        )

    return df


def select_rows_by_priority(
    df: DataFrame,
    partition_cols: Union[str, list],
    priority_cols: dict,
    keep: bool = True
) -> DataFrame:
    """
    Selects rows from a DataFrame based on the specified priority order within each partition.

    Args:
        df (DataFrame): The target DataFrame.
        partition_cols (Union[str, list]): The column(s) to apply partitionBy in a window. Can be a single column name or a list of column names.
        priority_cols (dict): A dictionary specifying the priority order for each column. The keys are the column names, and the values can be either a list of priorities, "min" or "max" to prioritize the minimum or maximum value in the column, or "not_null" to prioritize non-null values.
        keep (bool, optional): Whether to keep the selected rows (True) or delete them (False). Defaults to True.

    Returns:
        DataFrame: The DataFrame with the selected rows based on the priority order.

    Raises:
        TypeError: If df is not a DataFrame, partition_cols is not a str or list, or priority_cols is not a dict.
        ColumnIsMissing: If a column specified in partition_cols or priority_cols is not present in the DataFrame.
        ValueError: If a value in priority_cols is not one of "min", "max", or "not_null", or if a list in priority_cols is empty.

    Example:
        ```python
        df = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "col"])
        partition_cols = "id"
        priority_cols = {"col": ["B", "A", "C"]}
        result = select_rows_by_priority(df, partition_cols, priority_cols)
        result.show()
        ```
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a DataFrame")

    if not isinstance(priority_cols, dict):
        raise TypeError("priority_cols must be a dict")

    partition_cols = [partition_cols] if isinstance(partition_cols, str) else partition_cols

    if not isinstance(partition_cols, list):
        raise TypeError("partition_cols must be a list")

    if not isinstance(keep, bool):
        raise TypeError("keep must be a bool")

    df_cols = df.columns

    for col_index, prio_val in priority_cols.items():
        if col_index not in df_cols:
            raise ColumnIsMissing(f"Wrong Value: {col_index} in priority_cols is not in df columns")

        if not isinstance(prio_val, (str, list)):
            raise TypeError("prio_val must be a string or a list")

        if isinstance(prio_val, str) and (prio_val not in ["min", "max", "not_null"]):
            raise ValueError("only 'max, min, not_null' are accepted as string in prio_val")

        if isinstance(prio_val, list) and (len(prio_val) <= 0):
            raise ValueError("your list in priorisation must not be empty")

    for col_index in partition_cols:
        if col_index not in df_cols:
            raise ColumnIsMissing(f"{col_index} in partition_cols is not in df columns")

    df = add_order_columns(df, priority_cols)

    prio_cols = []

    for column, priority in priority_cols.items():
        col_priority = f"{column}_priority"

        if priority == "max":
            prio_cols.append(col(col_priority).desc())
        else:
            prio_cols.append(col(col_priority).asc())

    w = Window.partitionBy(*partition_cols).orderBy(*prio_cols)

    mask_prio = col("_keep_") == lit(1) if keep else col("_keep_") > lit(1)

    df = (
        df.withColumn("_keep_", dense_rank().over(w))
        .filter(mask_prio)
        .drop(*[f"{c}_priority" for c in priority_cols], "_keep_")
    )

    return df

def array_regexp_extract(
    column_array: Union[str, Column],
    regexp_pattern : str,
    group_pattern : int = 0,
    remove_none_and_empty_string : bool = True
) -> Column:
    """
    This function performs an array regexp extract on each item of the array.
    Default It will remove empty strings and extract group 0

    :param column_array: (str or Column), column name that contains the array information.

    :param regexp_pattern: (str), regexp expression in string

    :param group_pattern: (int), group to extract. Default 0

    :param remove_none_and_empty_string: (bool), boolean to remove None and empty strings. Default to True

    :returns: (Column), array with extracted regexp
    """
    if not isinstance(column_array, (str, Column)):
        raise TypeError("argument column_array must be a str or a Column")

    if not isinstance(regexp_pattern, str):
        raise TypeError("regexp_pattern must be a string")

    if not isinstance(group_pattern, int):
        raise TypeError("group_pattern must be a integer")

    if not isinstance(remove_none_and_empty_string, bool):
        raise TypeError("remove_none_and_empty_string must be a boolean")

    column_array = column_array if isinstance(column_array, Column) else col(column_array)

    array_regexp_extracted = transform_(
        column_array,
        lambda x : regexp_extract(x, regexp_pattern, group_pattern)
    )

    if remove_none_and_empty_string:
        array_regexp_extracted = filter_(
            array_regexp_extracted,
            lambda y : (y.isNotNull() & (y != lit("")))
        )

    return when(
        size(array_regexp_extracted) > lit(0),
        array_regexp_extracted
    ).otherwise(
        lit(None)
    )


def array_regexp_filter(
    column_array: Union[str, Column],
    regexp_pattern : str,
    remove_none_and_empty_string : bool = True,
    rlike_ : bool = True
) -> Column:
    """
    This function performs a filter on array to keep items respecting the regexp .
    Default It will remove empty strings and extract group 0

    :param column_array: (str or Column), column name that contains the array information.

    :param regexp_pattern: (str), regexp expression in string

    :param rlike_: (bool), if True will do an rlike if false will do not rlike. Default to True

    :param remove_none_and_empty_string: (bool), boolean to remove None and empty strings. Default to True

    :returns: (Column), array filtred with the regex
    """

    if not isinstance(column_array, (str, Column)):
        raise TypeError("column_array must be a string or Column")

    if not isinstance(regexp_pattern, str):
        raise TypeError("regexp_pattern must be a string")

    if not isinstance(remove_none_and_empty_string, bool):
        raise TypeError("remove_none_and_empty_string must be a boolean")

    if not isinstance(rlike_, bool):
        raise TypeError("rlike_ must be a boolean")

    column_array = column_array if isinstance(column_array, Column) else col(column_array)

    def _custom_filter(col_w: Column, rlike_: bool, remove_none_and_empty_string: bool) -> Column:
        """
        This function will create the custom_filter

        :param col_w: (Column), will contain the item of the array

        :param rlike_: (bool), if True will do an rlike if false will do not rlike

        :param remove_none_and_empty_string: (bool), boolean to remove None and empty strings

        :returns: (Column), custom filter
        """
        mask_filter = col_w.rlike(regexp_pattern)
        mask_filter = mask_filter if rlike_ else ~mask_filter
        if remove_none_and_empty_string:
            mask_filter = mask_filter & col_w.isNotNull() & (col_w != lit(""))
        return mask_filter

    array_regexp_extracted = filter_(
        column_array,
        lambda x : _custom_filter(x, rlike_, remove_none_and_empty_string)
    )

    return when(
        size(array_regexp_extracted) > lit(0),
        array_regexp_extracted
    ).otherwise(
        lit(None)
    )


def array_mean(
    column_array: Union[str, Column],
    apply_distinct_on_array : bool = False
) -> Column:
    """
    This function will compute the mean of an array

    :param column_array: (str or Column), column name that contains the array information.

    :param apply_distinct_on_array: (bool), boolean to apply a distinct on an array or not Default to False

    :returns: (Column), a value of the mean of the array
    """

    if not isinstance(column_array, (str, Column)):
        raise TypeError("column_array must be a string or Column")

    if not isinstance(apply_distinct_on_array, bool):
        raise TypeError("apply_distinct_on_array must be a boolean")

    column_array = column_array if isinstance(column_array, Column) else col(column_array)
    column_of_work = array_distinct(column_array) if apply_distinct_on_array else column_array

    def _count_and_sum(acc: Column, sum_: Column) -> Column:
        """
        This funciton will performs a sum_ and and an accumulation

        :param acc: (Column(Struct)) containing count_ and sum_

        :param sum_ : (Column) containing the sum

        :returns: (Column(Struct))
        """
        count_ = acc.count_ + 1
        sum_ = acc.sum_ + sum_
        return struct(count_.alias("count_"), sum_.alias("sum_"))

    return aggregate_(
        column_of_work,
        struct(lit(0).alias("count_"), lit(0.0).alias("sum_")),
        _count_and_sum,
        lambda acc: acc.sum_ / acc.count_,
    )
