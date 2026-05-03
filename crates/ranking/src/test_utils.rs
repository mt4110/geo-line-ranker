use std::path::PathBuf;

use domain::{PlacementKind, RankingQuery};

pub(crate) fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../storage/fixtures/minimal")
}

pub(crate) fn config_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs/ranking")
}

pub(crate) fn query(target_station_id: &str, placement: PlacementKind) -> RankingQuery {
    RankingQuery {
        target_station_id: target_station_id.to_string(),
        limit: Some(3),
        user_id: None,
        placement,
        debug: false,
        context: None,
    }
}
