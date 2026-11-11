//! Request-scoped tenant id for multi-tenant cloud brokers.
//!
//! Self-host keeps `BrokerConfig.tenant_id` (`"default"`). Cloud auth sets the
//! authenticated tenant for the duration of each request via `scope_tenant`.

use std::borrow::Cow;

tokio::task_local! {
    static REQUEST_TENANT: String;
}

/// Active tenant for broker catalog / publish paths.
pub fn effective_tenant<'a>(config_tenant: &'a str) -> Cow<'a, str> {
    match REQUEST_TENANT.try_with(|t| t.clone()) {
        Ok(t) if !t.is_empty() => Cow::Owned(t),
        _ => Cow::Borrowed(config_tenant),
    }
}

/// Run `fut` with `tenant_id` bound for all nested broker calls.
pub async fn scope_tenant<F, R>(tenant_id: String, fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    REQUEST_TENANT.scope(tenant_id, fut).await
}

/// Capture request identity before a future is handed to another task/thread.
/// `sync_scope` restores the blocking thread's previous value on return/panic.
pub(crate) fn inherit_request_tenant<R: Send + 'static>(
    operation: impl FnOnce() -> R + Send + 'static,
) -> impl FnOnce() -> R + Send + 'static {
    let tenant = REQUEST_TENANT.try_with(Clone::clone).ok();
    move || match tenant {
        Some(tenant) => REQUEST_TENANT.sync_scope(tenant, operation),
        None => operation(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn falls_back_to_config_outside_scope() {
        assert_eq!(effective_tenant("default").as_ref(), "default");
        let got = scope_tenant("tenant-a".into(), async {
            effective_tenant("default").into_owned()
        })
        .await;
        assert_eq!(got, "tenant-a");
        assert_eq!(effective_tenant("default").as_ref(), "default");
    }

    #[test]
    fn blocking_io_preserves_request_scope_without_leaking_to_the_next_call() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            for tenant in ["tenant-a", "tenant-b"] {
                let work = scope_tenant(tenant.into(), async {
                    crate::run_blocking_io(|| effective_tenant("default").into_owned())
                })
                .await;
                // The scope is gone before this future is spawned or polled.
                let inherited = tokio::spawn(work).await.unwrap().unwrap();
                assert_eq!(inherited, tenant);
                assert_eq!(
                    crate::run_blocking_io(|| effective_tenant("default").into_owned())
                        .await
                        .unwrap(),
                    "default"
                );
            }
        });
    }
}
