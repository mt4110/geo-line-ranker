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

#[cfg(test)]
mod tests {
    use super::haversine_meters;

    #[test]
    fn distance_is_zero_for_same_point() {
        assert_eq!(haversine_meters(35.0, 139.0, 35.0, 139.0), 0.0);
    }
}
