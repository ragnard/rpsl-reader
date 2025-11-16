from pathlib import Path
from typing import IO, Union

import polars as pl

# Import the Rust extension
from polars_rpsl._rpsl_reader import read_rpsl as _read_rpsl_rs
from polars_rpsl._rpsl_reader import read_rpsl_bytes as _read_rpsl_bytes_rs


def read_rpsl(
    source: Union[str, Path, bytes, "IO[bytes]"],
    schema: Union[pl.Schema, pl.DataFrame, None] = None,
) -> pl.DataFrame:
    """
    Read RPSL data from a file, bytes, or binary file-like object into a Polars DataFrame.

    Parameters
    ----------
    source : str, Path, bytes, or binary file-like object
        Source of RPSL data. Can be:
        - A file path (str or Path). Gzip-compressed files (.gz) are automatically detected.
        - Raw bytes containing RPSL data.
        - A binary file-like object with a read() method (e.g., open(path, 'rb'), io.BytesIO).
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
    Read from a file path:

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

    Read from bytes:

    >>> data = b"aut-num: AS123\\nmnt-by: EXAMPLE-MNT\\n\\n"
    >>> df = read_rpsl(data)

    Read from a binary file-like object:

    >>> with open("data.txt", "rb") as f:
    ...     df = read_rpsl(f)
    """
    # Prepare schema argument
    schema_arg = None
    if schema is not None:
        if isinstance(schema, pl.Schema):
            # Convert Schema to empty DataFrame
            schema_arg = pl.DataFrame(schema=schema)
        elif isinstance(schema, pl.DataFrame):
            # Use DataFrame's schema directly
            schema_arg = schema
        else:
            raise TypeError(
                f"schema must be pl.Schema, pl.DataFrame, or None, got {type(schema).__name__}"
            )

    # Handle different source types
    if isinstance(source, bytes):
        return _read_rpsl_bytes_rs(source, schema_arg)
    elif hasattr(source, "read"):
        # It's a file-like object - read all bytes
        data = source.read()
        if not isinstance(data, bytes):
            raise TypeError(
                f"file-like object must return bytes from read(), got {type(data).__name__}"
            )
        return _read_rpsl_bytes_rs(data, schema_arg)
    else:
        # Assume it's a path
        return _read_rpsl_rs(str(source), schema_arg)


__all__ = ["read_rpsl"]
