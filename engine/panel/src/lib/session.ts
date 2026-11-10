import { reactive } from "vue";
import { isExternalAuth, isStandalone } from "@/lib/config";
import { ApiError, adminUrl, apiGet, apiPost, isAuthRejection, onAuthRejected, publicUrl, setBrokerAccessGuard } from "@/lib/api";
import { isLocalCell } from "@/lib/targets";
import { serverVersion } from "@/lib/health";
import { refreshFlows, refreshGroups, refreshQueues } from "@/lib/catalog";
import {
  applyTheme,
  loadSavedApiBase,
  loadSavedCellId,
  loadSavedToken,
  readThemeMode,
  saveApiBase,
  saveCellId,
  saveToken,
  type ThemeMode,
} from "@/lib/storage";

export type AuthPhase = "loading" | "unavailable" | "setup" | "gate" | "reveal" | "app";

export type CellRecord = {
  id: string;
  region: string;
  controllerUrl: string;
  label?: string | null;
  adminTokenConfigured?: boolean;
};

export type CellMember = {
  profile: string;
  reachUrl: string;
  advertiseUrl: string;
  name: string;
  cellId: string;
};

export type AuthConfig = {
  mode: string;
  configured: boolean;
  setup_open?: boolean;
  setup_closes_in_secs?: number | null;
  setup_code_required?: boolean;
};

export type ClusterNode = {
  id?: string;
  addr?: string;
  name?: string;
  healthy?: boolean;
  is_self?: boolean;
  led_shards?: number[];
  preferred_shards?: number[];
};

export type ClusterStatus = {
  cluster_id?: string;
  generation?: number;
  enabled?: boolean;
  node_count?: number;
  healthy_count?: number;
  scheduler_leader_id?: string;
  this_node_scheduler_leader?: boolean;
  nodes?: ClusterNode[];
};

export const session = reactive({
  phase: "loading" as AuthPhase,
  apiBase: loadSavedApiBase(),
  token: "",
  revealedToken: "",
  revealWarning: "",
  auth: null as AuthConfig | null,
  health: "…",
  version: null as string | null,
  healthOk: false,
  ready: null as boolean | null,
  readyAt: 0,
  clusterLabel: "—",
  cluster: null as ClusterStatus | null,
  cells: [] as CellRecord[],
  members: [] as CellMember[],
  selectedCellId: loadSavedCellId(),
  themeMode: readThemeMode() as ThemeMode,
  splitAuth: false,
  joinToken: "",
  gateMessage: "",
  isSeed: false,
  cellsError: "",
  clusterError: "",
});

session.token = loadSavedToken(session.apiBase);
setBrokerAccessGuard(() => session.phase !== "app" || localBrokerSelected());
onAuthRejected((error) => handleAuthFailure(error));

export function localBrokerSelected(): boolean {
  return !isStandalone() && isLocalCell(session.selectedCellId || selectedCell()?.id || "local", session.cluster?.cluster_id);
}

let healthTimer: ReturnType<typeof setInterval> | null = null;
let clusterTimer: ReturnType<typeof setInterval> | null = null;
let retryTimer: ReturnType<typeof setTimeout> | null = null;
let retryDelay = 1000;

export function stopPolling() {
  if (healthTimer) clearInterval(healthTimer);
  if (clusterTimer) clearInterval(clusterTimer);
  if (retryTimer) clearTimeout(retryTimer);
  healthTimer = clusterTimer = retryTimer = null;
}

export function handleAuthFailure(error: unknown) {
  stopPolling();
  if (isAuthRejection(error)) {
    setToken("");
    session.phase = "gate";
    session.gateMessage = "Authentication rejected. Enter a valid credential.";
  } else {
    session.phase = "unavailable";
    session.gateMessage = "Broker unavailable. Your credential has been kept; retrying automatically.";
    retryTimer = setTimeout(() => void initAuth(), retryDelay);
    retryDelay = Math.min(retryDelay * 2, 30000);
  }
}

export function selectedCell(): CellRecord | null {
  return session.selectedCellId
    ? session.cells.find((c) => c.id === session.selectedCellId) || null
    : session.cells[0] || null;
}

export function setApiBase(base: string) {
  saveApiBase(base);
  session.apiBase = loadSavedApiBase();
  session.token = loadSavedToken(session.apiBase);
  void initAuth();
}

export function setToken(token: string) {
  session.token = token;
  saveToken(session.apiBase, token);
}

export function setCellId(id: string) {
  session.selectedCellId = id;
  saveCellId(id);
}

export function setTheme(mode: ThemeMode) {
  session.themeMode = mode;
  applyTheme(mode);
}

export async function initAuth() {
  stopPolling();
  applyTheme(session.themeMode);
  session.phase = "loading";
  session.splitAuth = false;
  session.gateMessage = "";
  try {
    if (isStandalone()) {
      await validateSessionToken();
      startApp();
      return;
    }
    const a = await apiGet<AuthConfig>(publicUrl("/v1/auth/config"));
    const b = await apiGet<AuthConfig>(publicUrl("/v1/auth/config"));
    if (a.configured !== b.configured || a.mode !== b.mode) {
      session.splitAuth = true;
      session.phase = "gate";
      session.auth = a;
      session.gateMessage =
        "Load balancer is returning different auth state from different brokers. Use one BetterMQ process for local dev.";
      return;
    }
    session.auth = a;
    if (isExternalAuth() && a.mode === "control_plane") {
      startApp();
      return;
    }
    if (!a.configured) {
      setToken("");
      session.phase = "setup";
      return;
    }
    if (!session.token) {
      session.phase = "gate";
      return;
    }
    try {
      await validateSessionToken();
      startApp();
    } catch (e) {
      handleAuthFailure(e);
    }
  } catch (e) {
    handleAuthFailure(e);
  }
}

export async function setupPassword(password: string, setupCode: string) {
  const res = await apiPost<{ token: string }>(publicUrl("/v1/local-auth/setup"), {
    password,
    setup_code: setupCode.trim() || undefined,
  });
  setToken(res.token);
  session.revealedToken = res.token;
  session.phase = "reveal";
}

export type AuthPropagation = {
  peers: number;
  applied: number;
  failed: { peer: string; error: string }[];
};

export const REGENERATE_WARNING =
  "Regenerating revokes the current API token for every producer, consumer and panel session " +
  "on every broker in this cell. All API clients must be updated with the new token. Continue?";

function propagationWarning(p: AuthPropagation | undefined): string {
  if (!p || !p.failed?.length) return "";
  const peers = p.failed.map((f) => `${f.peer} (${f.error})`).join("; ");
  return `The new token reached ${p.applied} of ${p.peers} peer broker(s). These still accept the old token until fixed (Infrastructure → Re-sync on each): ${peers}`;
}

export async function regenerateToken(password: string) {
  const res = await apiPost<{ token: string; propagation?: AuthPropagation }>(
    publicUrl("/v1/local-auth/regenerate"),
    { password },
  );
  setToken(res.token);
  session.revealedToken = res.token;
  session.revealWarning = propagationWarning(res.propagation);
  session.phase = "reveal";
}

export function finishReveal() {
  session.revealedToken = "";
  session.revealWarning = "";
  startApp();
}

export function submitGateToken(raw: string) {
  const t = raw.trim();
  if (!t) throw new Error(isStandalone() ? "Paste the standalone admin credential." : "Paste your sk_local_… token.");
  if (!isStandalone() && !t.startsWith("sk_local_")) throw new Error("Expected a sk_local_… token from this broker's setup.");
  setToken(t);
}

export function validateSessionToken() {
  return apiGet(isStandalone() ? adminUrl("/cells") : publicUrl("/v1/queues"));
}

export function startApp() {
  retryDelay = 1000;
  session.phase = "app";
  void refreshCells().catch(() => {});
  if (localBrokerSelected()) {
    void refreshQueues().catch(() => {});
    void refreshGroups().catch(() => {});
    void refreshFlows().catch(() => {});
  }
  void pollHealth().catch(() => {});
  void pollCluster().catch(() => {});
  if (healthTimer) clearInterval(healthTimer);
  if (clusterTimer) clearInterval(clusterTimer);
  healthTimer = setInterval(() => void pollHealth().catch(() => {}), 15000);
  clusterTimer = setInterval(() => void pollCluster().catch(() => {}), 5000);
}

export async function refreshCells() {
  try {
    const reg = await apiGet<{ cells?: CellRecord[]; members?: CellMember[] }>(adminUrl("/cells"));
    session.cells = reg.cells || [];
    session.members = reg.members || [];
    session.cellsError = "";
    if (!session.cells.some((c) => c.id === session.selectedCellId)) {
      session.selectedCellId = session.cells[0]?.id || "local";
    }
  } catch (error) {
    session.cellsError = String(error);
    throw error;
  }
}

export async function pollHealth() {
  const live = (async () => {
    try {
      const r = await fetch(publicUrl("/healthz"), { signal: AbortSignal.timeout(5000) });
      session.healthOk = r.ok;
      const j = (await r.json().catch(() => ({}))) as { status?: string; version?: string };
      session.version = r.ok ? serverVersion(j) : null;
      session.health = j.version || (r.ok ? "ok" : "down");
    } catch {
      session.healthOk = false;
      session.health = "down";
      session.version = null;
    }
  })();
  const readiness = (async () => {
    try {
      const result = await apiGet<{ ready?: boolean }>(isStandalone() ? adminUrl("/readyz") : publicUrl("/readyz"), { signal: AbortSignal.timeout(5000) });
      session.ready = typeof result.ready === "boolean" ? result.ready : null;
      session.readyAt = Date.now();
    } catch (error) {
      if (error instanceof ApiError && error.status === 503 && error.body && typeof error.body === "object" && "ready" in error.body && error.body.ready === false) {
        session.ready = false;
        session.readyAt = Date.now();
        return;
      }
      session.ready = null;
      session.readyAt = 0;
      throw error;
    }
  })();
  await Promise.all([live, readiness]);
}

export async function pollCluster() {
  if (isStandalone()) {
    session.clusterLabel = "federated";
    return;
  }
  try {
    const c = await apiGet<ClusterStatus>(publicUrl("/v1/cluster"));
    session.cluster = c;
    session.clusterError = "";
    if (c && (c.node_count || (c.nodes || []).length)) {
      const n = c.node_count || (c.nodes || []).length;
      const h = c.healthy_count ?? n;
      session.clusterLabel = `${h}/${n}`;
    } else {
      session.clusterLabel = "standalone";
    }
  } catch (error) {
    session.clusterError = String(error);
    session.clusterLabel = "—";
    throw error;
  }
}

export function setupHint(cfg: AuthConfig | null): string {
  if (!cfg) return "Choose a password to claim this broker.";
  if (cfg.setup_open) {
    const secs = cfg.setup_closes_in_secs;
    if (typeof secs === "number") {
      const mins = Math.max(1, Math.ceil(secs / 60));
      return `Open for about ${mins} more minute${mins === 1 ? "" : "s"}. Then restart BetterMQ to open setup again.`;
    }
    return "Anyone who can open this page can claim admin until a password is set.";
  }
  return "Setup window closed. Restart BetterMQ, then refresh this page.";
}
