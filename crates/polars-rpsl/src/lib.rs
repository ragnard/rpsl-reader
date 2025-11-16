use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use flate2::read::GzDecoder;
use polars::{frame::DataFrame, prelude::Schema};
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rpsl_parser::{ParseError, RpslParser};
use thiserror::Error;

mod schema;
mod schemaless;

use schema::SchemaPolarsBuilder;
use schemaless::PolarsBuilder;

#[derive(Error, Debug)]
pub enum RpslError {
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("Duplicate value for single-valued attribute '{attr}' in object at row {row}")]
    DuplicateSingleValue { attr: String, row: usize },

    #[error("Unsupported schema type for column '{column}': {dtype}. Only String and List(String) are supported.")]
    UnsupportedType { column: String, dtype: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

// =============================================================================
// Schema-less reading
// =============================================================================

/// Read RPSL data from a buffered reader into a Polars DataFrame (schema-less)
pub fn read_rpsl_from_reader<R: BufRead>(reader: R) -> Result<DataFrame, ParseError> {
    let mut parser = RpslParser::new(PolarsBuilder::new());
    parser.parse(reader)?;
    let polars_builder = parser.into_callbacks();
    Ok(polars_builder.build())
}

/// Read RPSL data from a file path into a Polars DataFrame (schema-less)
pub fn read_rpsl_from_path<P: AsRef<Path>>(
    path: P,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let path = path.as_ref();
    let file = File::open(path)?;

    let df = if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        let reader = BufReader::new(GzDecoder::new(file));
        read_rpsl_from_reader(reader)?
    } else {
        let reader = BufReader::new(file);
        read_rpsl_from_reader(reader)?
    };

    Ok(df)
}

// =============================================================================
// Schema-based reading
// =============================================================================

/// Read RPSL data with a specific schema
pub fn read_rpsl_with_schema_from_reader<R: BufRead>(
    reader: R,
    schema: &Schema,
) -> Result<DataFrame, RpslError> {
    let builder = SchemaPolarsBuilder::new(schema)?;
    let mut parser = RpslParser::new(builder);
    parser.parse(reader)?;
    let builder = parser.into_callbacks();
    builder.build()
}

/// Read RPSL data from a file path with a specific schema
pub fn read_rpsl_with_schema_from_path<P: AsRef<Path>>(
    path: P,
    schema: &Schema,
) -> Result<DataFrame, RpslError> {
    let path = path.as_ref();
    let file = File::open(path)?;

    let df = if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        let reader = BufReader::new(GzDecoder::new(file));
        read_rpsl_with_schema_from_reader(reader, schema)?
    } else {
        let reader = BufReader::new(file);
        read_rpsl_with_schema_from_reader(reader, schema)?
    };

    Ok(df)
}

// =============================================================================
// Python bindings
// =============================================================================

#[pyfunction]
#[pyo3(name = "read_rpsl", signature = (path, schema=None))]
fn py_read_rpsl(path: &str, schema: Option<PyDataFrame>) -> PyResult<PyDataFrame> {
    match schema {
        None => {
            let df = read_rpsl_from_path(path)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(PyDataFrame(df))
        }
        Some(schema_df) => {
            let polars_schema = schema_df.0.schema();
            let df = read_rpsl_with_schema_from_path(path, &polars_schema)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(PyDataFrame(df))
        }
    }
}

#[pymodule]
fn _rpsl_reader(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_read_rpsl, m)?)?;
    Ok(())
}
