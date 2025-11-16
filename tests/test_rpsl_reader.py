import tempfile
from pathlib import Path

import polars as pl
import pytest

from polars_rpsl import read_rpsl


def test_read_rpsl():
    """Test parsing a single RPSL object."""
    content = b"""route:          192.0.2.0/24
origin:         AS65000
descr:          Example route

route:          198.51.100.0/24
origin:         AS65001
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(content)
        f.flush()
        
        df = read_rpsl(f.name)
        
        assert isinstance(df, pl.DataFrame)
        assert "attributes" in df.columns
        assert df.shape[0] == 2  # Two objects
        
        # Check the first object has 3 attributes
        objects = df["attributes"].to_list()
        assert len(objects[0]) == 3
        
        # Check attribute names and values
        attrs = {item["name"]: item["value"] for item in objects[0]}
        assert attrs["route"] == "192.0.2.0/24"
        assert attrs["origin"] == "AS65000"
        assert attrs["descr"] == "Example route"
        
        Path(f.name).unlink()


# =============================================================================
# Schema-based reading tests
# =============================================================================


def test_read_with_schema_string_columns():
    """Test reading with a schema containing only String columns."""
    content = b"""route:          192.0.2.0/24
origin:         AS65000

route:          198.51.100.0/24
origin:         AS65001
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(content)
        f.flush()
        
        schema = pl.Schema({"route": pl.String, "origin": pl.String})
        df = read_rpsl(f.name, schema=schema)
        
        assert df.shape == (2, 2)
        assert df.columns == ["route", "origin"]
        assert df["route"].to_list() == ["192.0.2.0/24", "198.51.100.0/24"]
        assert df["origin"].to_list() == ["AS65000", "AS65001"]
        
        Path(f.name).unlink()


def test_read_with_schema_list_columns():
    """Test reading with a schema containing List[String] columns."""
    content = b"""aut-num:        AS65000
mnt-by:         MAINT-AS65000
mnt-by:         RIPE-NCC-END-MNT

aut-num:        AS65001
mnt-by:         MAINT-AS65001
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(content)
        f.flush()
        
        schema = pl.Schema({
            "aut-num": pl.String,
            "mnt-by": pl.List(pl.String),
        })
        df = read_rpsl(f.name, schema=schema)
        
        assert df.shape == (2, 2)
        assert df.columns == ["aut-num", "mnt-by"]
        assert df["aut-num"].to_list() == ["AS65000", "AS65001"]
        assert df["mnt-by"].to_list() == [
            ["MAINT-AS65000", "RIPE-NCC-END-MNT"],
            ["MAINT-AS65001"],
        ]
        
        Path(f.name).unlink()


def test_read_with_schema_missing_attributes():
    """Test that missing attributes result in null values."""
    content = b"""route:          192.0.2.0/24
origin:         AS65000

route:          198.51.100.0/24
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(content)
        f.flush()
        
        schema = pl.Schema({"route": pl.String, "origin": pl.String})
        df = read_rpsl(f.name, schema=schema)
        
        assert df.shape == (2, 2)
        assert df["route"].to_list() == ["192.0.2.0/24", "198.51.100.0/24"]
        assert df["origin"].to_list() == ["AS65000", None]
        
        Path(f.name).unlink()


def test_read_with_schema_empty_list():
    """Test that missing list attributes result in empty lists."""
    content = b"""aut-num:        AS65000
mnt-by:         MAINT-AS65000

aut-num:        AS65001
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(content)
        f.flush()
        
        schema = pl.Schema({
            "aut-num": pl.String,
            "mnt-by": pl.List(pl.String),
        })
        df = read_rpsl(f.name, schema=schema)
        
        assert df["aut-num"].to_list() == ["AS65000", "AS65001"]
        assert df["mnt-by"].to_list() == [["MAINT-AS65000"], []]
        
        Path(f.name).unlink()


def test_read_with_schema_ignores_extra_attributes():
    """Test that attributes not in schema are ignored."""
    content = b"""route:          192.0.2.0/24
origin:         AS65000
descr:          This should be ignored
admin-c:        ADMIN-CONTACT
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(content)
        f.flush()
        
        schema = pl.Schema({"route": pl.String, "origin": pl.String})
        df = read_rpsl(f.name, schema=schema)
        
        assert df.columns == ["route", "origin"]
        assert df["route"].to_list() == ["192.0.2.0/24"]
        assert df["origin"].to_list() == ["AS65000"]
        
        Path(f.name).unlink()


def test_read_with_schema_duplicate_single_value_error():
    """Test that duplicate values for String columns raise an error."""
    content = b"""route:          192.0.2.0/24
origin:         AS65000
origin:         AS65001
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(content)
        f.flush()
        
        schema = pl.Schema({"route": pl.String, "origin": pl.String})
        
        with pytest.raises(Exception, match="Duplicate value"):
            read_rpsl(f.name, schema=schema)
        
        Path(f.name).unlink()


def test_read_with_schema_from_dataframe():
    """Test passing a DataFrame instead of Schema."""
    content = b"""route:          192.0.2.0/24
origin:         AS65000
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(content)
        f.flush()
        
        # Create an empty DataFrame with the desired schema
        schema_df = pl.DataFrame(schema={"route": pl.String, "origin": pl.String})
        df = read_rpsl(f.name, schema=schema_df)
        
        assert df.columns == ["route", "origin"]
        assert df["route"].to_list() == ["192.0.2.0/24"]
        assert df["origin"].to_list() == ["AS65000"]
        
        Path(f.name).unlink()


def test_read_with_schema_gzip():
    """Test reading gzip file with schema."""
    import gzip
    
    content = b"""route:          192.0.2.0/24
origin:         AS65000
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".gz") as f:
        with gzip.open(f.name, "wb") as gz:
            gz.write(content)
        
        schema = pl.Schema({"route": pl.String, "origin": pl.String})
        df = read_rpsl(f.name, schema=schema)
        
        assert df["route"].to_list() == ["192.0.2.0/24"]
        assert df["origin"].to_list() == ["AS65000"]
        
        Path(f.name).unlink()


def test_read_with_schema_preserves_column_order():
    """Test that schema column order is preserved."""
    content = b"""origin:         AS65000
route:          192.0.2.0/24
descr:          Test
"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(content)
        f.flush()
        
        # Schema order should be preserved, not file order
        schema = pl.Schema({
            "descr": pl.String,
            "route": pl.String,
            "origin": pl.String,
        })
        df = read_rpsl(f.name, schema=schema)
        
        assert df.columns == ["descr", "route", "origin"]
        
        Path(f.name).unlink()


