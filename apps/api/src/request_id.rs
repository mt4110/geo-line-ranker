pub(crate) fn resolve_request_id(request_id: Option<&str>) -> Result<String, &'static str> {
    const MAX_REQUEST_ID_LEN: usize = 128;

    match request_id.map(str::trim) {
        None => Ok(generate_request_id()),
        Some("") => Err("request_id must not be empty when provided"),
        Some(request_id) if request_id.len() > MAX_REQUEST_ID_LEN => {
            Err("request_id must be at most 128 characters")
        }
        Some(request_id)
            if !request_id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')) =>
        {
            Err("request_id may contain only ASCII letters, digits, '_' or '-'")
        }
        Some(request_id) => Ok(request_id.to_string()),
    }
}

fn generate_request_id() -> String {
    format!("req_{}", uuid::Uuid::new_v4().simple())
}
