from operator import and_, gt as gt_, lt as lt_, ge as ge_, le as le_, eq as eq_
from typing import Union, Callable
from functools import reduce
from logging import info
import warnings
from pyspark.sql.functions import (
    col, array, lit, array_except, coalesce, concat_ws, md5 as md5_, forall,
    struct, length, transform as transform_, sort_array, concat, lpad, expr,
    broadcast, when, upper, regexp_replace, dense_rank, size, filter as filter_
)
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, ArrayType
from pyspark.sql.window import Window

class IllegalArgumentError(ValueError):
    """IllegalArgumentError"""


class ArgumentMissingError(ValueError):
    """ArgumentMissingError"""


class ColumnIsMissing(ValueError):
    """ColumnIsMissing"""


def unpivot_table(
    self: DataFrame,
    pivot_key: Union[str, Column],
    column_to_unpivot: list,
    col_key_name: str = "key",
    col_value_name: str = "value",
) -> DataFrame:
    """
    This function aims to unpivot tables

    :param pivot_key: (list), pivot_keys to keep in select

    :param column_to_unpivot: (list), list of column to unpivot

    :param col_key_name: (str), name of the column containing keys

    :param col_value_name: (str), name of the column containing values

    :returns: (DataFrame), dataframe with modifications
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
    df_types = dict(self.dtypes)

    if len(column_to_unpivot) <= 1:
        raise IllegalArgumentError("column_to_unpivot must have at least 2 columns to unpivot")

    for col_name in column_to_unpivot:
        if col_name not in df_columns:
            raise ColumnIsMissing(f"Wrong Value: {col_name} is not in dataframe")

    column_to_unpivot_type = [df_types[col_name] for col_name in column_to_unpivot]
    first_type = column_to_unpivot_type[0]
    column_to_unpivot_type = [(col_name == first_type) for col_name in column_to_unpivot_type]

    if not all(column_to_unpivot_type):
        raise IllegalArgumentError("all unpivot columns must have the same type")

    all_columns = ", ".join([f"'{col_name}', {col_name}" for col_name in column_to_unpivot])
    unpivot_expr = f"stack({len(column_to_unpivot)}, {all_columns}) as ({col_key_name},{col_value_name})"

    self = self.select(
        *pivot_key,
        expr(unpivot_expr)
    )

    return self


def extract_array_occurence(
    column_array: Union[str, Column],
    tolerance_user : float = 0.0,
    most_frequent : bool = True,
) -> Column:
    """
    This function will look for a cluster with most (or least) occurences with user tolerance
    eg:
        For an array [1, 1, 1, 2, 3] tolerance_user 0 and most_frequent True
        this function will return [1, 1, 1]

    eg2:
        For an array [1, 1, 1, 2, 2, 2, 3] tolerance_user 0 and most_frequent True
        this function will return [1, 1, 1, 2, 2, 2]

    :param column_array: (str or Column), column name that contains the array information.

    :param tolerance_user: (float), a percentage of tolerance between values. Must be between [0.0-1.0]. Default to 0.0

    :param most_frequent: (bool), True to extract most frequent, False to extract the least frequent. Default True

    :returns: (Column), array containing the occurrence
    """

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
        lambda y : aggregate_(
            column_array,
            lit(0.0),
            lambda acc, x: when(x.between(y*(1-tolerance_user), y*(1+tolerance_user)), acc+lit(1.0)).otherwise(acc)
        )
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
    - This function adds columns with the priority order of provided by the user (from higher to lower priority).
    - For example: with priority_cols = {"col": [P, T, Z]} it will add a new column (named "{column_name}_priority") with 0 when "P", 1 when "T", and 2 when "Z".
    - There can be more than one priority_cols.
    - Istead of [priorisation_list] there can be the following string values:
        - "min" or "max": create an auxiliary column (named "{column_name}_priority") with the same content as the column.
        - "not_null": It creates a new column (named "{column_name}_priority") with 0 when not null, 1 when null.

    :param df: (DataFrame), dataframe target

    :param priority_cols: (dict(str)), {"column": [priorisation_list], "column": "max",...}. [priorisation_list] provides the priorty of the elements in "column".

    :returns: (DataFrame), dataframe with modifications
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
    # Find priority_cols with empty lists
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
                    lit("0").cast(StringType())
                ).otherwise(lit("1").cast(StringType()))
            )
        else:
            new_priority_cols[column] = priorities

    cols_priority = []
    # Treat each priority_col independently
    for column, priority_list in new_priority_cols.items():

        col_priority = f"{column}_priority"
        cols_priority.append(col_priority)

        # Create df with priorisation list
        priorities = [
            (value, priority) for priority, value in enumerate(priority_list)
        ]

        schema_priority = StructType([
            StructField("_value_"   , StringType()),
            StructField(col_priority, StringType()),
        ])

        data_priority = df.sql_ctx.createDataFrame(data=priorities, schema=schema_priority).cache()

        # Join df to add new columns with priorisation
        df = df.join(
            broadcast(data_priority),
            df[column].eqNullSafe(data_priority["_value_"]),
            "left"
        ).drop("_value_")

    # The elements that are not part of the priorisation list are considered to be at the end of the priorisation
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
    - Select the row with max priorisation (based on priority in priority_cols provied by the user) in a partition based on partition_cols.
    - The priority_cols input shall be {"column": [priorisation_list], "column": "max",...}.
    - The algorithm will consider the max of the combination of the differnt "column" in priority_cols.
    - In case instead of [priorisation_list] there can be the following strings:
        - "min" or "max": It will priorize min or max based on "column" content. It works with boths numbers and letters.
        - "not_null": Priorizes non null values.

    :param df: (DataFrame), dataframe target.

    :param partition_cols: (list(str) or str), that contains the columns to apply partitionBy in a window.

    :param priority_cols: (dict(str)), {"column": [priorisation_list], "column": "max",...}.. [priorisation_list] provides the priorty of the elements in "column".

    :param keep: (bool, optional), True to keep selected lines and False to delete them. Default to True.

    :returns: (DataFrame), dataframe with modifications
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

    # Create new columns with priority provided in priority_cols
    df = add_order_columns(df, priority_cols)

    prio_cols = []

    # Create new columns with maximum of each "column" in priority_cols per partition
    for column, priority in priority_cols.items():
        col_priority = f"{column}_priority"

        if priority == "max":
            prio_cols.append(col(col_priority).desc())
        else:  # We enter here if [priorisation_list] == "min" or "non_null" or when [priorisation_list].isNotNull()
            prio_cols.append(col(col_priority).asc())

    w = Window.partitionBy(*partition_cols).orderBy(*prio_cols)

    mask_prio = col("_keep_") == lit(1) if keep else col("_keep_") > lit(1)

    df = df.withColumn(
        "_keep_",
        dense_rank().over(w)
    ).filter(
        mask_prio
    ).drop(
        *[f"{c}_priority" for c in priority_cols.keys()],
        "_keep_",
    )

    return df
