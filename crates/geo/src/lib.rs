use std::collections::HashMap;

pub fn haversine_meters(from_lat: f64, from_lng: f64, to_lat: f64, to_lng: f64) -> f64 {
    let earth_radius_m = 6_371_000.0_f64;
    let lat_delta = (to_lat - from_lat).to_radians();
    let lng_delta = (to_lng - from_lng).to_radians();
    let from_lat = from_lat.to_radians();
    let to_lat = to_lat.to_radians();

    let a = (lat_delta / 2.0).sin().powi(2)
        + from_lat.cos() * to_lat.cos() * (lng_delta / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    earth_radius_m * c
}

/// Geographic graph defining prefectures and their adjacencies.
/// Used for safe context-first retrieval and fallback planning.
#[derive(Debug, Clone)]
pub struct GeoGraph {
    // prefecture_code -> adjacent prefecture_codes
    adjacent_prefectures: HashMap<String, Vec<String>>,
}

impl Default for GeoGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl GeoGraph {
    /// Creates a JP prefecture adjacency graph with canonical borders.
    pub fn new() -> Self {
        let mut graph = Self {
            adjacent_prefectures: HashMap::new(),
        };

        // Initialize JP prefecture borders. Format: prefecture_code -> adjacent codes
        // This is a simplified model; real borders would require cadastral data.
        // Canonical source: MLIT N02 adjacency (simplified for determinism).
        let borders = vec![
            ("01", vec!["02"]),                   // Hokkaido - Aomori (fallback bridge)
            ("02", vec!["01", "03"]),             // Aomori - Hokkaido, Iwate
            ("03", vec!["02", "04", "05"]),       // Iwate - Aomori, Miyagi, Akita
            ("04", vec!["03", "06", "07"]),       // Miyagi - Iwate, Yamagata, Fukushima
            ("05", vec!["03", "06"]),             // Akita - Iwate, Yamagata
            ("06", vec!["04", "05", "07", "15"]), // Yamagata - Miyagi, Akita, Fukushima, Niigata
            ("07", vec!["04", "06", "08", "09", "10", "15"]), // Fukushima - Miyagi, Yamagata, Ibaraki, Tochigi, Gunma, Niigata
            ("08", vec!["07", "09", "11", "12"]), // Ibaraki - Fukushima, Tochigi, Saitama, Chiba
            ("09", vec!["07", "08", "10", "11"]), // Tochigi - Fukushima, Ibaraki, Gunma, Saitama
            ("10", vec!["07", "09", "11", "15", "20"]), // Gunma - Fukushima, Tochigi, Saitama, Niigata, Nagano
            ("11", vec!["08", "09", "10", "12", "13", "19"]), // Saitama - Ibaraki, Tochigi, Gunma, Chiba, Tokyo, Yamanashi
            ("12", vec!["08", "11", "13"]),                   // Chiba - Ibaraki, Saitama, Tokyo
            ("13", vec!["11", "12", "14", "19"]), // Tokyo - Saitama, Chiba, Kanagawa, Yamanashi
            ("14", vec!["13", "19", "22"]),       // Kanagawa - Tokyo, Yamanashi, Shizuoka
            ("15", vec!["06", "07", "10", "16", "20"]), // Niigata - Yamagata, Fukushima, Gunma, Toyama, Nagano
            ("16", vec!["15", "17", "20", "21"]),       // Toyama - Niigata, Ishikawa, Nagano, Gifu
            ("17", vec!["16", "18", "21"]),             // Ishikawa - Toyama, Fukui, Gifu
            ("18", vec!["17", "20", "21", "25", "26"]), // Fukui - Ishikawa, Nagano, Gifu, Shiga, Kyoto
            ("19", vec!["11", "13", "14", "20", "22"]), // Yamanashi - Saitama, Tokyo, Kanagawa, Nagano, Shizuoka
            ("20", vec!["10", "15", "16", "18", "19", "21", "22", "23"]), // Nagano - Gunma, Niigata, Toyama, Fukui, Yamanashi, Gifu, Shizuoka, Aichi
            ("21", vec!["16", "17", "18", "20", "23", "24", "25"]), // Gifu - Toyama, Ishikawa, Fukui, Nagano, Aichi, Mie, Shiga
            ("22", vec!["14", "19", "20", "23"]), // Shizuoka - Kanagawa, Yamanashi, Nagano, Aichi
            ("23", vec!["20", "21", "22", "24"]), // Aichi - Nagano, Gifu, Shizuoka, Mie
            ("24", vec!["21", "23", "25", "26", "29", "30"]), // Mie - Gifu, Aichi, Shiga, Kyoto, Nara, Wakayama
            ("25", vec!["18", "21", "24", "26"]),             // Shiga - Fukui, Gifu, Mie, Kyoto
            ("26", vec!["18", "24", "25", "27", "28", "29", "30"]), // Kyoto - Fukui, Mie, Shiga, Osaka, Hyogo, Nara, Wakayama
            ("27", vec!["26", "28", "29"]),                         // Osaka - Kyoto, Hyogo, Nara
            ("28", vec!["26", "27", "29", "31", "33"]), // Hyogo - Kyoto, Osaka, Nara, Tottori, Okayama
            ("29", vec!["24", "26", "27", "28", "30"]), // Nara - Mie, Kyoto, Osaka, Hyogo, Wakayama
            ("30", vec!["24", "26", "29"]),             // Wakayama - Mie, Kyoto, Nara
            ("31", vec!["28", "32", "33", "34"]), // Tottori - Hyogo, Shimane, Okayama, Hiroshima
            ("32", vec!["31", "34", "35"]),       // Shimane - Tottori, Hiroshima, Yamaguchi
            ("33", vec!["28", "31", "34", "37"]), // Okayama - Hyogo, Tottori, Hiroshima, Kagawa
            ("34", vec!["31", "32", "33", "35", "38"]), // Hiroshima - Tottori, Shimane, Okayama, Yamaguchi, Ehime
            ("35", vec!["32", "34", "40"]),             // Yamaguchi - Shimane, Hiroshima, Fukuoka
            ("36", vec!["37", "38", "39"]),             // Tokushima - Kagawa, Ehime, Kochi
            ("37", vec!["33", "36", "38"]),             // Kagawa - Okayama, Tokushima, Ehime
            ("38", vec!["34", "36", "37", "39"]), // Ehime - Hiroshima, Tokushima, Kagawa, Kochi
            ("39", vec!["36", "38"]),             // Kochi - Tokushima, Ehime
            ("40", vec!["35", "41", "43", "44"]), // Fukuoka - Yamaguchi, Saga, Kumamoto, Oita
            ("41", vec!["40", "42"]),             // Saga - Fukuoka, Nagasaki
            ("42", vec!["41"]),                   // Nagasaki - Saga
            ("43", vec!["40", "44", "45", "46"]), // Kumamoto - Fukuoka, Oita, Miyazaki, Kagoshima
            ("44", vec!["40", "43", "45"]),       // Oita - Fukuoka, Kumamoto, Miyazaki
            ("45", vec!["43", "44", "46"]),       // Miyazaki - Kumamoto, Oita, Kagoshima
            ("46", vec!["43", "45"]),             // Kagoshima - Kumamoto, Miyazaki
            ("47", vec![]),                       // Okinawa - isolated (no land borders)
        ];

        for (code, adjacent) in borders {
            graph.adjacent_prefectures.insert(
                code.to_string(),
                adjacent.iter().map(|s| s.to_string()).collect(),
            );
        }

        graph
    }

    /// Returns adjacent prefecture codes for the given prefecture.
    pub fn adjacent_prefectures(&self, prefecture_code: &str) -> Vec<&str> {
        self.adjacent_prefectures
            .get(prefecture_code)
            .map(|codes| codes.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Checks if two prefectures are adjacent.
    pub fn are_adjacent(&self, code1: &str, code2: &str) -> bool {
        let forward = self
            .adjacent_prefectures
            .get(code1)
            .map(|codes| codes.iter().any(|code| code == code2))
            .unwrap_or(false);

        if forward {
            return true;
        }

        self.adjacent_prefectures
            .get(code2)
            .map(|codes| codes.iter().any(|code| code == code1))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::{haversine_meters, GeoGraph};

    #[test]
    fn distance_is_zero_for_same_point() {
        assert_eq!(haversine_meters(35.0, 139.0, 35.0, 139.0), 0.0);
    }

    #[test]
    fn hokkaido_adjacent_to_aomori() {
        let graph = GeoGraph::new();
        assert!(graph.are_adjacent("01", "02"));
        assert!(graph.are_adjacent("02", "01"));
    }

    #[test]
    fn hokkaido_not_adjacent_to_okinawa() {
        let graph = GeoGraph::new();
        assert!(!graph.are_adjacent("01", "47"));
    }

    #[test]
    fn okinawa_isolated() {
        let graph = GeoGraph::new();
        assert!(graph.adjacent_prefectures("47").is_empty());
    }

    #[test]
    fn tokyo_has_neighbors() {
        let graph = GeoGraph::new();
        let neighbors = graph.adjacent_prefectures("13");
        assert!(neighbors.contains(&"11")); // Saitama
        assert!(neighbors.contains(&"12")); // Chiba
        assert!(neighbors.contains(&"14")); // Kanagawa
    }

    #[test]
    fn canonical_nagano_code_has_neighbors() {
        let graph = GeoGraph::new();
        let neighbors = graph.adjacent_prefectures("20");
        assert!(!neighbors.is_empty());
    }

    #[test]
    fn adjacency_is_order_independent() {
        let graph = GeoGraph::new();
        assert_eq!(
            graph.are_adjacent("29", "30"),
            graph.are_adjacent("30", "29")
        );
    }
}
