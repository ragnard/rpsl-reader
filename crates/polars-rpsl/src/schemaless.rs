use polars::{
    frame::DataFrame,
    prelude::{ArrowField, LargeListArray, Series},
};
use polars_arrow::{
    array::{Array, MutableUtf8Array, StructArray},
    datatypes::ArrowDataType,
    offset::OffsetsBuffer,
};
use rpsl_parser::Callbacks;

pub(crate) struct PolarsBuilder {
    names: MutableUtf8Array<i32>,
    values: MutableUtf8Array<i64>,
    object_starts: Vec<i64>,
}

impl PolarsBuilder {
    pub fn new() -> PolarsBuilder {
        PolarsBuilder {
            names: MutableUtf8Array::<i32>::new(),
            values: MutableUtf8Array::<i64>::new(),
            object_starts: vec![0],
        }
    }

    pub fn build(self) -> DataFrame {
        let names_array: polars_arrow::array::Utf8Array<i32> = self.names.into();
        let values_array: polars_arrow::array::Utf8Array<i64> = self.values.into();

        let struct_fields = vec![
            ArrowField::new("name".into(), ArrowDataType::Utf8, false),
            ArrowField::new("value".into(), ArrowDataType::LargeUtf8, false),
        ];
        let struct_array = StructArray::new(
            ArrowDataType::Struct(struct_fields),
            names_array.len(),
            vec![Box::new(names_array), Box::new(values_array)],
            None,
        );

        let offsets = unsafe { OffsetsBuffer::new_unchecked(self.object_starts.into()) };
        let list_array = LargeListArray::new(
            ArrowDataType::LargeList(Box::new(ArrowField::new(
                "item".into(),
                struct_array.dtype().clone(),
                true,
            ))),
            offsets,
            Box::new(struct_array),
            None,
        );

        let series = Series::from_arrow("attributes".into(), Box::new(list_array))
            .expect("Failed to create list series");

        DataFrame::new(vec![series.into()]).expect("Failed to create DataFrame")
    }
}

impl Callbacks for PolarsBuilder {
    fn start_object(&mut self) {}

    fn attribute(&mut self, name: &[u8], value: &[u8]) {
        self.names
            .push(Some(String::from_utf8_lossy(name).as_ref()));
        self.values
            .push(Some(String::from_utf8_lossy(value).as_ref()));
    }

    fn end_object(&mut self) {
        self.object_starts.push(self.names.len() as i64);
    }
}
