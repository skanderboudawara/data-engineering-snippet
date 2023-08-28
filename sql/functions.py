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
