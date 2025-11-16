from pathlib import Path
from typing import Union

import polars as pl

# Import the Rust extension
from polars_rpsl._rpsl_reader import read_rpsl as _read_rpsl_rs


def read_rpsl(
    path: Union[str, Path],
    schema: Union[pl.Schema, pl.DataFrame, None] = None,
) -> pl.DataFrame:
    """
    Read RPSL data from a file into a Polars DataFrame.

    Parameters
    ----------
    path : str or Path
        Path to the RPSL file. Gzip-compressed files (.gz) are automatically detected.
    schema : pl.Schema, pl.DataFrame, or None, optional
        Schema to use for reading the data. If provided, the data will be read into
        columns matching the schema. Only pl.String and pl.List(pl.String) types are
        supported. If None (default), returns a single column with all attributes as
        a list of structs.

    Returns
    -------
    pl.DataFrame
        DataFrame containing the RPSL data. If schema is None, contains a single
        'attributes' column with List[Struct{name: String, value: String}].
        If schema is provided, contains one column per schema field.

    Examples
    --------
    Read without schema (returns nested structure):

    >>> df = read_rpsl("data.txt")
    >>> df.schema
    Schema({'attributes': List(Struct({'name': String, 'value': String}))})

    Read with schema (returns flat structure):

    >>> schema = pl.Schema({
    ...     'aut-num': pl.String,
    ...     'mnt-by': pl.List(pl.String),
    ... })
    >>> df = read_rpsl("data.txt", schema=schema)
    >>> df.schema
    Schema({'aut-num': String, 'mnt-by': List(String)})
    """
    path_str = str(path)

    if schema is None:
        return _read_rpsl_rs(path_str, None)
    elif isinstance(schema, pl.Schema):
        # Convert Schema to empty DataFrame
        schema_df = pl.DataFrame(schema=schema)
        return _read_rpsl_rs(path_str, schema_df)
    elif isinstance(schema, pl.DataFrame):
        # Use DataFrame's schema directly
        return _read_rpsl_rs(path_str, schema)
    else:
        raise TypeError(
            f"schema must be pl.Schema, pl.DataFrame, or None, got {type(schema).__name__}"
        )


__all__ = ["read_rpsl"]
