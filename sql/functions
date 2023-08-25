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
