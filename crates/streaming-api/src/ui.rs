//! The embedded web UI (`ui/` folder). In debug builds the files are read
//! from disk, so the UI can be edited without rebuilding.

use axum::{
    http::{HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;

use crate::routes::ApiError;

#[derive(RustEmbed)]
#[folder = "ui/"]
struct Ui;

fn file_response(path: &str) -> Option<Response> {
    let file = Ui::get(path)?;
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let mut response = ([(header::CONTENT_TYPE, mime.to_string())], file.data.into_owned()).into_response();
    if mime.subtype() == mime_guess::mime::HTML {
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    }
    Some(response)
}

/// Fallback of the router: embedded files, `index.html` for unknown paths
/// (hash routing), JSON 404 under `/api`.
pub async fn serve_ui(method: Method, uri: Uri) -> Response {
    let path = uri.path();
    if path == "/api" || path.starts_with("/api/") {
        return ApiError::new(StatusCode::NOT_FOUND, "not found").into_response();
    }
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let wanted = path.trim_start_matches('/');
    let wanted = if wanted.is_empty() { "index.html" } else { wanted };
    if !wanted.split('/').any(|part| part == "..")
        && let Some(response) = file_response(wanted)
    {
        return response;
    }
    file_response("index.html")
        .unwrap_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "the web UI is not embedded").into_response())
}
