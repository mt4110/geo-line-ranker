use anyhow::{ensure, Result};
use generic_csv::{read_csv_rows, PreparedSourceFile};
use serde::{Deserialize, Serialize};

pub const PARSER_VERSION: &str = "jp-rail-v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RailStationRecord {
    pub station_code: String,
    pub station_id: String,
    pub station_name: String,
    pub line_name: String,
    pub prefecture_name: String,
    pub latitude: f64,
    pub longitude: f64,
}

pub fn parse_rail_stations(files: &[PreparedSourceFile]) -> Result<Vec<RailStationRecord>> {
    let mut rows = Vec::new();
    for file in files
        .iter()
        .filter(|file| file.logical_name == "rail_stations")
    {
        rows.extend(
            read_csv_rows::<RailStationCsvRow>(file)?
                .into_iter()
                .map(|row| RailStationRecord {
                    station_id: station_id_from_code(&row.station_code),
                    station_code: row.station_code,
                    station_name: row.station_name,
                    line_name: row.line_name,
                    prefecture_name: row.prefecture_name,
                    latitude: row.latitude,
                    longitude: row.longitude,
                }),
        );
    }
    ensure!(
        !rows.is_empty(),
        "manifest did not provide any rail_stations CSV rows"
    );
    Ok(rows)
}

pub fn station_id_from_code(station_code: &str) -> String {
    format!("jp_station_{}", station_code)
}

#[derive(Debug, Deserialize)]
struct RailStationCsvRow {
    station_code: String,
    station_name: String,
    line_name: String,
    prefecture_name: String,
    latitude: f64,
    longitude: f64,
}

#[cfg(test)]
mod tests {
    use super::station_id_from_code;

    #[test]
    fn builds_stable_station_ids() {
        assert_eq!(station_id_from_code("1130217"), "jp_station_1130217");
    }
}
