use anyhow::{ensure, Result};
use generic_csv::{read_csv_rows, PreparedSourceFile};
use serde::{Deserialize, Serialize};

pub const PARSER_VERSION: &str = "jp-postal-v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PostalCodeRecord {
    pub postal_code: String,
    pub prefecture_name: String,
    pub city_name: String,
    pub town_name: String,
}

pub fn parse_postal_codes(files: &[PreparedSourceFile]) -> Result<Vec<PostalCodeRecord>> {
    let mut rows = Vec::new();
    for file in files
        .iter()
        .filter(|file| file.logical_name == "postal_codes")
    {
        rows.extend(
            read_csv_rows::<PostalCodeCsvRow>(file)?
                .into_iter()
                .map(|row| PostalCodeRecord {
                    postal_code: row.postal_code,
                    prefecture_name: row.prefecture_name,
                    city_name: row.city_name,
                    town_name: row.town_name,
                }),
        );
    }
    ensure!(
        !rows.is_empty(),
        "manifest did not provide any postal_codes CSV rows"
    );
    Ok(rows)
}

#[derive(Debug, Deserialize)]
struct PostalCodeCsvRow {
    postal_code: String,
    prefecture_name: String,
    city_name: String,
    town_name: String,
}
