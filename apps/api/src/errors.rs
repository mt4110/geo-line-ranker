use api_contracts::ErrorResponse;
use axum::{http::StatusCode, response::IntoResponse, Json};

pub(crate) fn error_response(status: StatusCode, message: String) -> axum::response::Response {
    (
        status,
        Json(ErrorResponse {
            error: message,
            code: error_code(status).to_string(),
        }),
    )
        .into_response()
}

fn error_code(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "bad_request",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::SERVICE_UNAVAILABLE => "service_unavailable",
        StatusCode::INTERNAL_SERVER_ERROR => "internal_server_error",
        _ => "http_error",
    }
}

pub(crate) fn context_resolution_error_status(error: &anyhow::Error) -> StatusCode {
    if is_context_resolution_validation_error(error) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

pub(crate) fn context_resolution_error_message(
    error: &anyhow::Error,
    context_input: &context::ContextInput,
) -> String {
    let error_message = error.to_string();
    if is_blank_station_error(error) {
        return "target_station_id or context.station_id must not be blank".to_string();
    }
    if is_unknown_station_error(error) {
        return context_input
            .station_id
            .as_deref()
            .map(str::trim)
            .map(|station_id| format!("unknown target_station_id: {station_id}"))
            .unwrap_or(error_message);
    }
    error_message
}

fn is_context_resolution_validation_error(error: &anyhow::Error) -> bool {
    is_unknown_station_error(error)
        || is_unknown_line_error(error)
        || is_blank_station_error(error)
        || error
            .chain()
            .any(|cause| cause.to_string() == "line context requires line_id or line_name")
}

fn is_blank_station_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string() == "station_id must not be blank")
}

fn is_unknown_station_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().starts_with("unknown station:"))
}

fn is_unknown_line_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().starts_with("unknown line_id:"))
}
