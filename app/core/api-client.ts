import type {
  BrowseResult, GlobalControl, Health, Job, LibraryAudit,
} from "./contracts";

const API_ROOT = "/api";

function apiError(payload: Record<string, unknown>, fallback: string) {
  return typeof payload.error === "string" ? payload.error : fallback;
}

export class ApiRequestError extends Error {
  constructor(message: string, readonly status: number, readonly payload: Record<string, unknown>) {
    super(message);
    this.name = "ApiRequestError";
  }
}

export type RetryCorrection = {
  tmdb_id?: number;
  media_type?: "movie" | "tv" | "collection";
  season?: number;
  archive_password?: string;
};

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_ROOT}${path}`, {
    ...init,
    cache: "no-store",
    headers: {
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...(init?.headers ?? {}),
    },
  });
  const payload = await response.json().catch(() => ({})) as Record<string, unknown>;
  if (!response.ok) throw new ApiRequestError(apiError(payload, `本地服务返回 ${response.status}`), response.status, payload);
  return payload as T;
}

function post<T>(path: string, payload: Record<string, unknown>) {
  return request<T>(path, { method: "POST", body: JSON.stringify(payload) });
}

export const scrapeFlowApi = {
  health: () => request<Health>("/health"),
  control: () => request<GlobalControl>("/control"),
  jobs: () => request<{ jobs: Job[] }>("/jobs"),
  job: (id: string) => request<{ job: Job }>(`/jobs/${id}`),
  browse: (path: string, refresh = false) => request<BrowseResult>(
    `/browse?path=${encodeURIComponent(path)}&refresh=${refresh ? "1" : "0"}`,
  ),
  /** Submitting a path starts the automatic identity-to-cleanup workflow. */
  create: (path: string) => post<{ job: Job }>("/jobs", { path }),
  /** This only asks the scheduler to retry now; it does not release a plan. */
  retry: (id: string, correction: RetryCorrection = {}) => post<{ job: Job }>(`/jobs/${id}/retry`, correction),
  cancel: (id: string) => post<{ job: Job }>(`/jobs/${id}/cancel`, {}),
  /** Terminal-only local record cleanup; it never deletes the media library. */
  cleanup: (id: string) => post<{ cleanup: { job_id: string; removed: boolean } }>(
    `/jobs/${id}/cleanup`,
    {},
  ),
  pause: (reason?: string) => post<GlobalControl>("/control/pause", reason ? { reason } : {}),
  resume: () => post<GlobalControl>("/control/resume", {}),
  /** A read-only structural scan of the three formal media roots. */
  latestAudit: () => request<{ audit: LibraryAudit | null }>("/library-audit/latest"),
  runAudit: () => post<{ audit: LibraryAudit }>("/library-audit/run", {}),
};
