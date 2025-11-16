# polars-rpsl

Fast RPSL (Routing Policy Specification Language) parser with Polars DataFrame output.

## Installation

```bash
pip install polars-rpsl
```

## Usage

### Schema-less reading

Read RPSL data into a nested structure where each object is a list of attribute name/value pairs:

```python
from polars_rpsl import read_rpsl

df = read_rpsl("ripe.db.route.gz")
print(df)
```

Output:
```
shape: (3, 1)
┌─────────────────────────────────────────────────┐
│ attributes                                      │
│ ---                                             │
│ list[struct[2]]                                 │
╞═════════════════════════════════════════════════╡
│ [{"route","192.0.2.0/24"}, {"origin","AS65000"}│
│ , {"mnt-by","MAINT-AS65000"}]                   │
│ [{"route","198.51.100.0/24"}, {"origin","AS650…│
│ [{"route","203.0.113.0/24"}, {"origin","AS6500…│
└─────────────────────────────────────────────────┘
```

### Schema-based reading

Read RPSL data into a flat DataFrame with typed columns:

```python
import polars as pl
from polars_rpsl import read_rpsl

schema = pl.Schema({
    "route": pl.String,
    "origin": pl.String,
    "mnt-by": pl.List(pl.String),  # Multi-valued attribute
})

df = read_rpsl("ripe.db.route.gz", schema=schema)
print(df)
```

Output:
```
shape: (3, 3)
┌─────────────────┬──────────┬──────────────────────────────┐
│ route           ┆ origin   ┆ mnt-by                       │
│ ---             ┆ ---      ┆ ---                          │
│ str             ┆ str      ┆ list[str]                    │
╞═════════════════╪══════════╪══════════════════════════════╡
│ 192.0.2.0/24    ┆ AS65000  ┆ ["MAINT-AS65000"]            │
│ 198.51.100.0/24 ┆ AS65001  ┆ ["MAINT-AS65001", "RIPE-NCC… │
│ 203.0.113.0/24  ┆ AS65002  ┆ ["MAINT-AS65002"]            │
└─────────────────┴──────────┴──────────────────────────────┘
```

Schema-based reading:
- Supports `pl.String` for single-valued attributes
- Supports `pl.List(pl.String)` for multi-valued attributes
- Returns `None` for missing single-valued attributes
- Returns empty list `[]` for missing multi-valued attributes
- Raises an error if a single-valued attribute appears multiple times
- Ignores attributes not defined in the schema

### Reading gzip files

Gzip-compressed files (`.gz`) are automatically detected and decompressed:

```python
df = read_rpsl("ripe.db.route.gz")
```

## Development

```bash
# Install dependencies
uv sync

# Build and install in development mode
uv run maturin develop

# Run tests
uv run pytest tests/ -v

# Build wheel
uv run maturin build --release --out dist/
```

## Project Structure

```
rpsl-reader/
├── crates/
│   ├── rpsl-parser/     # Core parser (minimal dependencies)
│   └── polars-rpsl/     # Polars + Python bindings
├── python/
│   └── polars_rpsl/     # Python wrapper
└── tests/               # Python tests
```

## License

MIT
