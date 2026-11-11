//! Thin client for the broker's control-plane endpoints used by scenarios.

use anyhow::{bail, Context};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct Api {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct QueueSpec {
    pub name: String,
    pub url: String,
    pub parallelism: u32,
    pub max_retries: u32,
    /// `(initial_ms, max_ms, multiplier)`; `None` keeps the broker default.
    pub backoff: Option<(u64, u64, f64)>,
}

#[derive(Clone, Debug)]
pub struct CronCreated {
    pub next_run_at_ms: i64,
}

impl Api {
    pub fn new(http: reqwest::Client, base: &str, token: Option<String>) -> Self {
        Self {
            http,
            base: base.trim_end_matches('/').to_string(),
            token,
        }
    }

    pub fn with_token(&self, base: &str, token: Option<String>) -> Self {
        Self::new(self.http.clone(), base, token)
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let r = self.http.request(method, format!("{}{path}", self.base));
        match &self.token {
            Some(t) => r.bearer_auth(t),
            None => r,
        }
    }

    async fn send(&self, rb: reqwest::RequestBuilder, what: &str) -> anyhow::Result<Value> {
        let resp = rb.send().await.with_context(|| what.to_string())?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "{what}: {status}: {}",
                text.chars().take(300).collect::<String>()
            );
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    pub async fn create_queue(&self, spec: &QueueSpec, secret: &str) -> anyhow::Result<String> {
        let mut body = json!({
            "queue": spec.name,
            "url": spec.url,
            "secret": secret,
            "parallelism": spec.parallelism,
            "max_retries": spec.max_retries,
        });
        if let Some((initial, max, mult)) = spec.backoff {
            body["retry_backoff"] = json!({
                "kind": "exponential", "initialMs": initial, "maxMs": max, "multiplier": mult
            });
        }
        let v = self
            .send(
                self.req(reqwest::Method::POST, "/v1/queues").json(&body),
                &format!("create queue {}", spec.name),
            )
            .await?;
        Ok(v["queue_id"].as_str().unwrap_or_default().to_string())
    }

    pub async fn create_cron(&self, body: Value) -> anyhow::Result<CronCreated> {
        let v = self
            .send(
                self.req(reqwest::Method::POST, "/v1/crons").json(&body),
                "create cron",
            )
            .await?;
        Ok(CronCreated {
            next_run_at_ms: v["next_run_at_ms"].as_i64().unwrap_or(0),
        })
    }

    pub async fn create_group(&self, name: &str) -> anyhow::Result<String> {
        let v = self
            .send(
                self.req(reqwest::Method::POST, "/v1/groups")
                    .json(&json!({ "name": name })),
                "create group",
            )
            .await?;
        Ok(v["group_id"].as_str().unwrap_or_default().to_string())
    }

    pub async fn add_member(
        &self,
        group_id: &str,
        name: &str,
        url: &str,
        parallelism: u32,
    ) -> anyhow::Result<String> {
        let v = self
            .send(
                self.req(
                    reqwest::Method::POST,
                    &format!("/v1/groups/{group_id}/members"),
                )
                .json(&json!({
                    "name": name, "url": url, "secret": "s3cret", "parallelism": parallelism
                })),
                "add group member",
            )
            .await?;
        Ok(v["member_id"].as_str().unwrap_or_default().to_string())
    }

    pub async fn get(&self, path: &str) -> anyhow::Result<Value> {
        self.send(self.req(reqwest::Method::GET, path), path).await
    }

    /// Total messages currently parked in dead-letter topics.
    pub async fn dlq_total(&self) -> u64 {
        match self.get("/v1/dlq/sources").await {
            Ok(Value::Array(items)) => items.iter().filter_map(|i| i["count"].as_u64()).sum(),
            Ok(v) => v["sources"]
                .as_array()
                .map(|a| a.iter().filter_map(|i| i["count"].as_u64()).sum())
                .unwrap_or(0),
            Err(_) => 0,
        }
    }

    pub async fn metrics_text(&self) -> Option<String> {
        let resp = self
            .req(reqwest::Method::GET, "/metrics/prometheus")
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.text().await.ok()
    }
}
