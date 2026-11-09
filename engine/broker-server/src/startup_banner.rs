//! Colored startup banner for interactive terminals (brand wordmark + URLs, then logs below).

use broker_config::{Component, PanelMode, ResolvedServeSettings};
use broker_storage::StorageMode;
use std::io::{IsTerminal, Write};

const BRAND: &str = "\x1b[38;2;5;53;190m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

pub fn print(settings: &ResolvedServeSettings, storage: StorageMode) {
    if !std::io::stderr().is_terminal() {
        return;
    }

    let (base, panel, docs) = endpoint_urls(
        &settings.listeners.public.to_string(),
        &settings.listeners.admin.to_string(),
        settings.panel_mode != PanelMode::Disabled && settings.has(Component::Panel),
    );
    let data = settings.data_dir.display().to_string();
    let storage_label = format!("{storage:?}");
    let cluster = if settings.cluster_enabled {
        "on"
    } else {
        "off"
    };

    let mut out = std::io::stderr().lock();
    let _ = writeln!(out);
    print_logo(&mut out);
    let _ = writeln!(out);
    let _ = writeln!(out, "{DIM}{BOLD}     Http Messaging & Scheduling{RESET}");
    let _ = writeln!(out);
    let _ = writeln!(out, "  {BRAND}●{RESET} {BOLD}RUNNING{RESET}");
    let _ = writeln!(out);
    let _ = writeln!(out, "  {DIM}API{RESET}      {base}");
    if let Some(panel) = panel {
        let _ = writeln!(out, "  {DIM}Panel{RESET}    {panel}");
    }
    let _ = writeln!(out, "  {DIM}Docs{RESET}     {docs}");
    let _ = writeln!(out, "  {DIM}Data{RESET}     {data}");
    let _ = writeln!(
        out,
        "  {DIM}Storage{RESET}  {storage_label}  ·  {DIM}cluster{RESET} {cluster}"
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "{DIM}────────────────────────────────────────{RESET}");
    let _ = writeln!(out);
}

fn print_logo(out: &mut impl Write) {
    let _ = writeln!(out, "  {BOLD}{BRAND}            betterMQ{RESET}");
}

fn endpoint_urls(
    public: &str,
    admin: &str,
    include_panel: bool,
) -> (String, Option<String>, String) {
    let base = public_base_url(public);
    let management = public_base_url(admin);
    let panel = include_panel.then(|| format!("{management}/panel/"));
    (base, panel, format!("{management}/docs"))
}

fn public_base_url(listen: &str) -> String {
    let (host, port) = match listen.rsplit_once(':') {
        Some((h, p)) => (h.trim_matches(['[', ']']), p),
        None => (listen, "8080"),
    };
    let host = match host {
        "0.0.0.0" | "::" | "" => "127.0.0.1",
        h if h.starts_with("::ffff:") => &h[7..],
        h => h,
    };
    if host.contains(':') {
        format!("http://[{host}]:{port}")
    } else {
        format!("http://{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_wildcard_bind_to_localhost() {
        assert_eq!(public_base_url("0.0.0.0:8080"), "http://127.0.0.1:8080");
        assert_eq!(public_base_url("127.0.0.1:9090"), "http://127.0.0.1:9090");
        assert_eq!(
            public_base_url("[2001:db8::1]:9090"),
            "http://[2001:db8::1]:9090"
        );
    }

    #[test]
    fn banner_hides_disabled_panel_and_uses_management_listener() {
        let (api, panel, docs) = endpoint_urls("0.0.0.0:8080", "127.0.0.1:9090", true);
        assert_eq!(api, "http://127.0.0.1:8080");
        assert_eq!(panel.as_deref(), Some("http://127.0.0.1:9090/panel/"));
        assert_eq!(docs, "http://127.0.0.1:9090/docs");
        assert!(endpoint_urls("0.0.0.0:8080", "127.0.0.1:9090", false)
            .1
            .is_none());
        assert_eq!(
            endpoint_urls("0.0.0.0:8080", "0.0.0.0:8080", true)
                .1
                .as_deref(),
            Some("http://127.0.0.1:8080/panel/")
        );
    }
}
