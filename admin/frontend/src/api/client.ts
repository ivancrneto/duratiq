// Typed fetch client. Mirrors the backend Pydantic schemas. Calls are relative
// (`/api/...`) and reach the backend via the Vite dev proxy / nginx in prod.

import { getToken } from "../auth/token";

export type RunStatus =
  | "PENDING"
  | "RUNNING"
  | "SUSPENDED"
  | "COMPLETED"
  | "FAILED"
  | "CANCELLED";

export interface Run {
  id: string;
  name: string;
  version: number;
  status: RunStatus;
  input: unknown;
  result: unknown;
  error: unknown;
  idempotency_key: string | null;
  parent_run_id: string | null;
  parent_seq: number | null;
  lease_owner: string | null;
  lease_expires_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface RunDetail extends Run {
  search_attributes: Record<string, unknown>;
}

export interface RunList {
  items: Run[];
  total: number;
  limit: number;
  offset: number;
}

export interface Step {
  run_id: string;
  seq: number;
  kind: string;
  name: string;
  status: "SCHEDULED" | "COMPLETED" | "FAILED" | "CANCELLED";
  input: unknown;
  result: unknown;
  error: unknown;
  attempt: number;
  scheduled_at: string;
  completed_at: string | null;
  timeout_at: string | null;
  heartbeat: unknown;
}

export interface Stats {
  total: number;
  by_status: Record<string, number>;
}

export interface ActionResult {
  id: string;
  status: RunStatus;
  enqueued: boolean;
}

export const TERMINAL: RunStatus[] = ["COMPLETED", "FAILED", "CANCELLED"];

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const token = getToken();
  const res = await fetch(path, {
    ...init,
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...init?.headers,
    },
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      detail = (await res.json()).detail ?? detail;
    } catch {
      /* non-JSON body */
    }
    throw new ApiError(res.status, detail);
  }
  return res.json() as Promise<T>;
}

export interface ListParams {
  status?: string;
  name?: string;
  /** Search-attribute equality filters, ANDed (e.g. `{ region: "eu" }`). */
  searchAttributes?: Record<string, unknown>;
  limit?: number;
  offset?: number;
}

export const api = {
  stats: () => apiFetch<Stats>("/api/stats"),
  listRuns: (p: ListParams = {}) => {
    const q = new URLSearchParams();
    if (p.status) q.set("status", p.status);
    if (p.name) q.set("name", p.name);
    if (p.searchAttributes && Object.keys(p.searchAttributes).length > 0) {
      q.set("sa", JSON.stringify(p.searchAttributes));
    }
    q.set("limit", String(p.limit ?? 50));
    q.set("offset", String(p.offset ?? 0));
    return apiFetch<RunList>(`/api/runs?${q.toString()}`);
  },
  getRun: (id: string) => apiFetch<RunDetail>(`/api/runs/${encodeURIComponent(id)}`),
  getSteps: (id: string) =>
    apiFetch<Step[]>(`/api/runs/${encodeURIComponent(id)}/steps`),
  cancelRun: (id: string) =>
    apiFetch<ActionResult>(`/api/runs/${encodeURIComponent(id)}/cancel`, {
      method: "POST",
    }),
  retryRun: (id: string) =>
    apiFetch<ActionResult>(`/api/runs/${encodeURIComponent(id)}/retry`, {
      method: "POST",
    }),
  signalRun: (id: string, name: string, payload: unknown) =>
    apiFetch<ActionResult>(`/api/runs/${encodeURIComponent(id)}/signal`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, payload }),
    }),
};
