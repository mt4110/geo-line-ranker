use anyhow::{ensure, Result};
use generic_csv::{read_csv_rows, PreparedSourceFile};
use serde::{Deserialize, Serialize};

pub const SCHOOL_CODES_PARSER_VERSION: &str = "jp-school-codes-v1";
pub const SCHOOL_GEODATA_PARSER_VERSION: &str = "jp-school-geodata-v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchoolCodeRecord {
    pub school_code: String,
    pub school_id: String,
    pub name: String,
    pub prefecture_name: String,
    pub city_name: String,
    pub school_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchoolGeodataRecord {
    pub school_code: String,
    pub school_id: String,
    pub name: String,
    pub prefecture_name: String,
    pub city_name: String,
    pub address: String,
    pub school_type: String,
    pub latitude: f64,
    pub longitude: f64,
}

pub fn parse_school_codes(files: &[PreparedSourceFile]) -> Result<Vec<SchoolCodeRecord>> {
    let mut rows = Vec::new();
    for file in files
        .iter()
        .filter(|file| file.logical_name == "school_codes")
    {
        rows.extend(
            read_csv_rows::<SchoolCodeCsvRow>(file)?
                .into_iter()
                .map(|row| SchoolCodeRecord {
                    school_id: school_id_from_code(&row.school_code),
                    school_code: row.school_code,
                    name: row.name,
                    prefecture_name: row.prefecture_name,
                    city_name: row.city_name,
                    school_type: row.school_type,
                }),
        );
    }
    ensure!(
        !rows.is_empty(),
        "manifest did not provide any school_codes CSV rows"
    );
    Ok(rows)
}

pub fn parse_school_geodata(files: &[PreparedSourceFile]) -> Result<Vec<SchoolGeodataRecord>> {
    let mut rows = Vec::new();
    for file in files
        .iter()
        .filter(|file| file.logical_name == "school_geodata")
    {
        rows.extend(
            read_csv_rows::<SchoolGeodataCsvRow>(file)?
                .into_iter()
                .map(|row| SchoolGeodataRecord {
                    school_id: school_id_from_code(&row.school_code),
                    school_code: row.school_code,
                    name: row.name,
                    prefecture_name: row.prefecture_name,
                    city_name: row.city_name,
                    address: row.address,
                    school_type: row.school_type,
                    latitude: row.latitude,
                    longitude: row.longitude,
                }),
        );
    }
    ensure!(
        !rows.is_empty(),
        "manifest did not provide any school_geodata CSV rows"
    );
    Ok(rows)
}

pub fn school_id_from_code(school_code: &str) -> String {
    format!("jp_school_{}", school_code)
}

#[derive(Debug, Deserialize)]
struct SchoolCodeCsvRow {
    school_code: String,
    name: String,
    prefecture_name: String,
    city_name: String,
    school_type: String,
}

#[derive(Debug, Deserialize)]
struct SchoolGeodataCsvRow {
    school_code: String,
    name: String,
    prefecture_name: String,
    city_name: String,
    address: String,
    school_type: String,
    latitude: f64,
    longitude: f64,
}

#[cfg(test)]
mod tests {
    use super::school_id_from_code;

    #[test]
    fn builds_stable_school_ids() {
        assert_eq!(school_id_from_code("13101A"), "jp_school_13101A");
    }
}
