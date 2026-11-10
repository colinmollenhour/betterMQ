//! Request correlation IDs (`x-request-id`).
//!
//! [`assign_request_id`] keeps a well-formed inbound ID or generates one,
//! echoes it on the response, and scopes it for the request task so internal
//! calls made through [`crate::cluster_auth::apply_cluster_secret`] forward it
//! to the next node.

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;

pub const REQUEST_ID_HEADER: &str = "x-request-id";
const MAX_REQUEST_ID_LEN: usize = 128;

tokio::task_local! {
    static CURRENT: HeaderValue;
}

/// The request ID of the request this task is serving, if any.
pub fn current() -> Option<HeaderValue> {
    CURRENT.try_with(HeaderValue::clone).ok()
}

fn well_formed(value: &HeaderValue) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_REQUEST_ID_LEN
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

/// Middleware: adopt or mint the request ID before tracing sees the request.
pub async fn assign_request_id(mut request: Request, next: Next) -> Response {
    let id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .filter(|value| well_formed(value))
        .cloned()
        .unwrap_or_else(|| {
            HeaderValue::from_str(&uuid::Uuid::new_v4().to_string())
                .expect("UUIDs are valid header values")
        });
    request.headers_mut().insert(REQUEST_ID_HEADER, id.clone());
    let mut response = CURRENT.scope(id.clone(), next.run(request)).await;
    response.headers_mut().insert(REQUEST_ID_HEADER, id);
    response
}

/// Attach the current request ID to an outbound internal request.
pub fn forward(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match current() {
        Some(id) => builder.header(REQUEST_ID_HEADER, id.as_bytes()),
        None => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    fn app() -> Router {
        Router::new()
            .route(
                "/echo",
                get(|| async {
                    current()
                        .map(|id| id.to_str().unwrap().to_string())
                        .unwrap_or_default()
                }),
            )
            .layer(axum::middleware::from_fn(assign_request_id))
    }

    async fn call(id: Option<&str>) -> (String, String) {
        let mut request = axum::http::Request::builder().uri("/echo");
        if let Some(id) = id {
            request = request.header(REQUEST_ID_HEADER, id);
        }
        let response = app()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let header = response.headers()[REQUEST_ID_HEADER]
            .to_str()
            .unwrap()
            .to_string();
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        (header, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn inbound_id_is_kept_scoped_and_echoed() {
        let (header, scoped) = call(Some("gw-1234.abc")).await;
        assert_eq!(header, "gw-1234.abc");
        assert_eq!(scoped, "gw-1234.abc");
    }

    #[tokio::test]
    async fn missing_or_malformed_ids_are_replaced() {
        let (header, scoped) = call(None).await;
        assert!(uuid::Uuid::parse_str(&header).is_ok());
        assert_eq!(header, scoped);
        let long = "a".repeat(MAX_REQUEST_ID_LEN + 1);
        for bad in ["has space", "quote\"", long.as_str()] {
            let (header, _) = call(Some(bad)).await;
            assert!(uuid::Uuid::parse_str(&header).is_ok(), "{bad}");
        }
    }

    #[test]
    fn forward_outside_a_request_adds_nothing() {
        let request = forward(reqwest::Client::new().get("http://example.invalid"))
            .build()
            .unwrap();
        assert!(request.headers().get(REQUEST_ID_HEADER).is_none());
    }
}
