//! OpenAPI spec + Scalar UI (embedded, served at `/docs` and `/api-reference`).

use axum::{
    http::{header, HeaderValue},
    middleware,
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
    Router,
};

#[cfg(feature = "cloud")]
const OPENAPI_JSON: &str = include_str!("../openapi/bettermq.cloud.openapi.json");
#[cfg(not(feature = "cloud"))]
const OPENAPI_JSON: &str = include_str!("../openapi/bettermq.openapi.json");
const SCALAR_HTML: &str = include_str!("../embedded/scalar.html");
const SCALAR_JS: &[u8] = include_bytes!("../embedded/scalar-standalone.js");

pub fn router() -> Router {
    Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(scalar))
        .route("/docs/scalar.js", get(scalar_js))
        .route("/api-reference", get(scalar))
        .route(
            "/api-reference/",
            get(|| async { Redirect::permanent("/api-reference") }),
        )
        .route("/docs/", get(|| async { Redirect::permanent("/docs") }))
        .layer(middleware::from_fn(crate::security::docs_security_headers))
}

async fn openapi_json() -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/json; charset=utf-8"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        OPENAPI_JSON,
    )
        .into_response()
}

async fn scalar() -> Html<&'static str> {
    Html(SCALAR_HTML)
}

async fn scalar_js() -> Response {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/javascript; charset=utf-8"),
        )],
        SCALAR_JS,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_public_router_path_has_a_published_contract() {
        let spec: serde_json::Value = serde_json::from_str(OPENAPI_JSON).unwrap();
        let paths = spec["paths"].as_object().unwrap();
        let source_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../broker-api/src");
        for entry in std::fs::read_dir(source_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|extension| extension != "rs") {
                continue;
            }
            let source = std::fs::read_to_string(path).unwrap();
            // Routes built inside unit-test modules are fixtures, not the published API.
            let source = source.split("#[cfg(test)]").next().unwrap_or_default();
            for route in source.split(".route(").skip(1) {
                let Some(literal) = route.trim_start().strip_prefix('"') else {
                    continue;
                };
                let path = literal.split('"').next().unwrap();
                // Explicit publication policy: broker-to-broker protocols are
                // private; all management, enrollment and data-plane routes are public.
                if path.starts_with("/internal/v1/") {
                    continue;
                }
                // Axum catch-all syntax is represented by an ordinary OpenAPI
                // path parameter; the handler accepts the complete destination.
                let path = path.replace("{*", "{");
                assert!(
                    paths.contains_key(&path),
                    "undocumented public router path: {path}"
                );
            }
        }
    }

    #[test]
    fn all_openapi_references_resolve() {
        let spec: serde_json::Value = serde_json::from_str(OPENAPI_JSON).unwrap();
        fn visit(value: &serde_json::Value, root: &serde_json::Value) {
            match value {
                serde_json::Value::Object(object) => {
                    if let Some(reference) = object.get("$ref").and_then(|value| value.as_str()) {
                        assert!(
                            root.pointer(reference.strip_prefix('#').expect("local reference"))
                                .is_some(),
                            "unresolved reference: {reference}"
                        );
                    }
                    for value in object.values() {
                        visit(value, root);
                    }
                }
                serde_json::Value::Array(array) => {
                    for value in array {
                        visit(value, root);
                    }
                }
                _ => {}
            }
        }
        visit(&spec, &spec);
    }
}
