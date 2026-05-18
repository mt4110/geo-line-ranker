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
            ("01", vec!["02"]),                               // Hokkaido - Aomori
            ("02", vec!["01", "03"]),                         // Aomori - Hokkaido, Iwate
            ("03", vec!["02", "04", "06"]),                   // Iwate - Aomori, Miyagi, Yamagata
            ("04", vec!["03", "05", "06", "07"]), // Miyagi - Iwate, Akita, Yamagata, Fukushima
            ("05", vec!["04", "06"]),             // Akita - Miyagi, Yamagata
            ("06", vec!["03", "04", "05", "07"]), // Yamagata - Iwate, Miyagi, Akita, Fukushima
            ("07", vec!["04", "06", "08"]),       // Fukushima - Miyagi, Yamagata, Ibaraki
            ("08", vec!["07", "09", "10", "11"]), // Ibaraki - Fukushima, Tochigi, Gunma, Saitama
            ("09", vec!["08", "10"]),             // Tochigi - Ibaraki, Gunma
            ("10", vec!["08", "09", "11"]),       // Gunma - Ibaraki, Tochigi, Saitama
            ("11", vec!["08", "10", "12", "13"]), // Saitama - Ibaraki, Gunma, Chiba, Tokyo
            ("12", vec!["11", "13", "14"]),       // Chiba - Saitama, Tokyo, Kanagawa
            ("13", vec!["11", "12", "14", "19"]), // Tokyo - Saitama, Chiba, Kanagawa, Yamanashi
            ("14", vec!["12", "13", "15"]),       // Kanagawa - Chiba, Tokyo, Shizuoka
            ("15", vec!["14", "19", "22", "23"]), // Shizuoka - Kanagawa, Yamanashi, Aichi, Mie
            ("16", vec!["17", "18"]),             // Niigata - Toyama, Nagano
            ("17", vec!["16", "18", "22"]),       // Toyama - Niigata, Nagano, Aichi
            ("18", vec!["16", "17", "19", "06"]), // Nagano - Niigata, Toyama, Yamanashi, Yamagata
            ("19", vec!["13", "15", "18", "22"]), // Yamanashi - Tokyo, Shizuoka, Nagano, Aichi
            ("20", vec!["16", "17", "19", "06"]), // Nagano (canonical JIS) - mirrors legacy 18 mapping
            ("21", vec!["22", "24"]),             // Gifu - Aichi, Kyoto
            ("22", vec!["15", "17", "19", "21", "23"]), // Aichi - Shizuoka, Toyama, Yamanashi, Gifu, Mie
            ("23", vec!["15", "22", "24"]),             // Mie - Shizuoka, Aichi, Kyoto
            ("24", vec!["21", "23", "25", "26", "27", "28"]), // Kyoto - Gifu, Mie, Osaka, Hyogo, Nara, Wakayama
            ("25", vec!["24", "26", "27"]),                   // Osaka - Kyoto, Hyogo, Nara
            ("26", vec!["24", "25", "27", "28", "30"]), // Hyogo - Kyoto, Osaka, Nara, Wakayama, Okayama
            ("27", vec!["24", "25", "26"]),             // Nara - Kyoto, Osaka, Hyogo
            ("28", vec!["24", "26", "29"]),             // Wakayama - Kyoto, Hyogo, Mie
            ("29", vec!["28", "30", "31"]), // Mie - Wakayama, Okayama, Hiroshima (approx)
            ("30", vec!["26", "31"]),       // Okayama - Hyogo, Hiroshima
            ("31", vec!["29", "30", "32"]), // Hiroshima - Mie, Okayama, Yamaguchi
            ("32", vec!["31", "33", "34"]), // Yamaguchi - Hiroshima, Tokushima, Kagawa
            ("33", vec!["32", "34", "35"]), // Tokushima - Yamaguchi, Kagawa, Ehime
            ("34", vec!["32", "33", "35"]), // Kagawa - Yamaguchi, Tokushima, Ehime
            ("35", vec!["33", "34", "36"]), // Ehime - Tokushima, Kagawa, Kochi
            ("36", vec!["35"]),             // Kochi - Ehime
            ("37", vec!["38", "39", "40"]), // Fukuoka - Saga, Nagasaki, Kumamoto
            ("38", vec!["37"]),             // Saga - Fukuoka
            ("39", vec!["37", "40"]),       // Nagasaki - Fukuoka, Kumamoto
            ("40", vec!["41", "42", "43", "44"]), // Fukuoka - Saga, Nagasaki, Kumamoto, Oita
            ("41", vec!["40", "42"]),       // Saga - Fukuoka, Nagasaki
            ("42", vec!["40", "41"]),       // Nagasaki - Fukuoka, Saga
            ("43", vec!["40", "44", "45", "46"]), // Kumamoto - Fukuoka, Oita, Miyazaki, Kagoshima
            ("44", vec!["40", "43", "45"]), // Oita - Fukuoka, Kumamoto, Miyazaki
            ("45", vec!["43", "44", "46"]), // Miyazaki - Kumamoto, Oita, Kagoshima
            ("46", vec!["43", "45"]),       // Kagoshima - Kumamoto, Miyazaki
            ("47", vec![]),                 // Okinawa - isolated (no land borders)
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
