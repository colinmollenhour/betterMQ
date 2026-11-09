import { adminApiRoot } from "@/lib/config";
import { allowlistedApiBase, loadSavedApiBase, loadSavedToken, normalizeToken } from "@/lib/storage";

export class ApiError extends Error {
  status: number;
  body: unknown;
  constructor(message: string, status: number, body: unknown) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

export function isAuthRejection(error: unknown): boolean {
  return error instanceof ApiError && (error.status === 401 || error.status === 403);
}

let brokerAccessAllowed = () => true;
let authRejected = (_error: ApiError) => {};
export function onAuthRejected(handler: (error: ApiError) => void) {
  authRejected = handler;
}
export function setBrokerAccessGuard(guard: () => boolean) {
  brokerAccessAllowed = guard;
}

function apiBase(): string {
  return allowlistedApiBase(loadSavedApiBase());
}

function authHeaders(): Record<string, string> {
  const t = normalizeToken(loadSavedToken(apiBase()));
  return t ? { Authorization: `Bearer ${t}` } : {};
}

export function adminUrl(path: string): string {
  const root = adminApiRoot();
  const base = root.indexOf("http") === 0 ? root : apiBase() + root;
  return base.replace(/\/$/, "") + path;
}

export function publicUrl(path: string): string {
  return apiBase().replace(/\/$/, "") + path;
}

async function parseBody(res: Response): Promise<unknown> {
  const text = await res.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function request<T>(url: string, init: RequestInit = {}): Promise<T> {
  const path = new URL(url, apiBase()).pathname;
  if (path.startsWith("/v1/") && path !== "/v1/cluster" && !path.startsWith("/v1/auth/") && !brokerAccessAllowed()) {
    throw new Error("Broker operations target the local broker only. Select its local cell to continue.");
  }
  const headers = new Headers(init.headers);
  const auth = authHeaders();
  Object.entries(auth).forEach(([k, v]) => headers.set(k, v));
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const res = await fetch(url, { ...init, headers });
  const body = await parseBody(res);
  if (!res.ok) {
    let msg = res.statusText || "request failed";
    if (body && typeof body === "object" && "error" in body) {
      msg = String((body as { error: unknown }).error || msg);
    }
    const error = new ApiError(msg, res.status, body);
    // Ignore a late rejection for a credential that has already been replaced.
    if (isAuthRejection(error) && auth.Authorization === authHeaders().Authorization) authRejected(error);
    throw error;
  }
  return body as T;
}

export function apiGet<T>(url: string): Promise<T> {
  return request<T>(url);
}

export function apiPost<T>(url: string, data?: unknown): Promise<T> {
  return request<T>(url, { method: "POST", body: data === undefined ? undefined : JSON.stringify(data) });
}

export function apiPut<T>(url: string, data?: unknown): Promise<T> {
  return request<T>(url, { method: "PUT", body: data === undefined ? undefined : JSON.stringify(data) });
}

export function apiDelete<T>(url: string): Promise<T> {
  return request<T>(url, { method: "DELETE" });
}
