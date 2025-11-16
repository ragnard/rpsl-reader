use std::collections::HashMap;

use indexmap::IndexMap;
use polars::{
    frame::DataFrame,
    prelude::{ArrowField, DataType, LargeListArray, Schema, Series},
};
use polars_arrow::{
    array::MutableUtf8Array,
    datatypes::ArrowDataType,
    offset::OffsetsBuffer,
};
use rpsl_parser::Callbacks;

use crate::RpslError;

#[derive(Debug, Clone, Copy, PartialEq)]
enum ColumnType {
    String,
    ListString,
}

pub(crate) struct SchemaPolarsBuilder {
    /// Column names in schema order
    columns: IndexMap<String, ColumnType>,

    /// Builders for String columns
    string_builders: HashMap<String, MutableUtf8Array<i64>>,

    /// Builders for List[String] columns (values array + offsets)
    list_builders: HashMap<String, (MutableUtf8Array<i64>, Vec<i64>)>,

    /// Current object's accumulated values
    current_object: HashMap<String, Vec<String>>,

    /// Current row number (for error reporting)
    row_count: usize,

    /// Error that occurred during parsing (if any)
    error: Option<RpslError>,
}

impl SchemaPolarsBuilder {
    pub fn new(schema: &Schema) -> Result<Self, RpslError> {
        let mut columns = IndexMap::new();
        let mut string_builders = HashMap::new();
        let mut list_builders = HashMap::new();

        for (name, dtype) in schema.iter() {
            let col_type = match dtype {
                DataType::String => ColumnType::String,
                DataType::List(inner) if matches!(inner.as_ref(), DataType::String) => {
                    ColumnType::ListString
                }
                _ => {
                    return Err(RpslError::UnsupportedType {
                        column: name.to_string(),
                        dtype: format!("{:?}", dtype),
                    });
                }
            };

            columns.insert(name.to_string(), col_type);

            match col_type {
                ColumnType::String => {
                    string_builders.insert(name.to_string(), MutableUtf8Array::<i64>::new());
                }
                ColumnType::ListString => {
                    list_builders.insert(
                        name.to_string(),
                        (MutableUtf8Array::<i64>::new(), vec![0i64]),
                    );
                }
            }
        }

        Ok(Self {
            columns,
            string_builders,
            list_builders,
            current_object: HashMap::new(),
            row_count: 0,
            error: None,
        })
    }

    pub fn build(self) -> Result<DataFrame, RpslError> {
        if let Some(err) = self.error {
            return Err(err);
        }

        let mut series_vec = Vec::new();

        for (name, col_type) in &self.columns {
            let series = match col_type {
                ColumnType::String => {
                    let array = self.string_builders.get(name).unwrap().clone();
                    let utf8_array: polars_arrow::array::Utf8Array<i64> = array.into();
                    Series::from_arrow(name.as_str().into(), Box::new(utf8_array))
                        .expect("Failed to create string series")
                }
                ColumnType::ListString => {
                    let (values_array, offsets) = self.list_builders.get(name).unwrap().clone();
                    let utf8_array: polars_arrow::array::Utf8Array<i64> = values_array.into();

                    let offsets_buffer = unsafe { OffsetsBuffer::new_unchecked(offsets.into()) };
                    let list_array = LargeListArray::new(
                        ArrowDataType::LargeList(Box::new(ArrowField::new(
                            "item".into(),
                            ArrowDataType::LargeUtf8,
                            true,
                        ))),
                        offsets_buffer,
                        Box::new(utf8_array),
                        None,
                    );

                    Series::from_arrow(name.as_str().into(), Box::new(list_array))
                        .expect("Failed to create list series")
                }
            };
            series_vec.push(series.into());
        }

        Ok(DataFrame::new(series_vec).expect("Failed to create DataFrame"))
    }
}

impl Callbacks for SchemaPolarsBuilder {
    fn start_object(&mut self) {
        self.current_object.clear();
    }

    fn attribute(&mut self, name: &[u8], value: &[u8]) {
        if self.error.is_some() {
            return;
        }

        let name_str = String::from_utf8_lossy(name).to_string();
        let value_str = String::from_utf8_lossy(value).to_string();

        // Only collect attributes that are in the schema
        if self.columns.contains_key(&name_str) {
            self.current_object
                .entry(name_str)
                .or_insert_with(Vec::new)
                .push(value_str);
        }
    }

    fn end_object(&mut self) {
        if self.error.is_some() {
            return;
        }

        // Process each column in schema order
        for (name, col_type) in &self.columns {
            let values = self.current_object.get(name);

            match col_type {
                ColumnType::String => {
                    let builder = self.string_builders.get_mut(name).unwrap();
                    match values {
                        None => builder.push::<&str>(None),
                        Some(vals) if vals.is_empty() => builder.push::<&str>(None),
                        Some(vals) if vals.len() == 1 => builder.push(Some(&vals[0])),
                        Some(_) => {
                            self.error = Some(RpslError::DuplicateSingleValue {
                                attr: name.clone(),
                                row: self.row_count,
                            });
                            return;
                        }
                    }
                }
                ColumnType::ListString => {
                    let (values_builder, offsets) = self.list_builders.get_mut(name).unwrap();
                    match values {
                        None => {
                            // Empty list - just update offset
                            offsets.push(values_builder.len() as i64);
                        }
                        Some(vals) if vals.is_empty() => {
                            // Empty list - just update offset
                            offsets.push(values_builder.len() as i64);
                        }
                        Some(vals) => {
                            for val in vals {
                                values_builder.push(Some(val.as_str()));
                            }
                            offsets.push(values_builder.len() as i64);
                        }
                    }
                }
            }
        }

        self.row_count += 1;
    }
}
