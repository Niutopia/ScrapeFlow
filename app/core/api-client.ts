import type {
  BrowseResult, GlobalControl, Health, Job, JobRetryOptions, TargetCategory,
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
  create: (
    path: string,
    category: TargetCategory,
    tmdbId?: number,
    mediaType?: "tv" | "movie",
  ) => post<{ job: Job }>("/jobs", {
    path,
    category,
    type: tmdbId ? (mediaType || (category === "电影" ? "movie" : "tv")) : "auto",
    ...(tmdbId ? { tmdb_id: tmdbId } : {}),
    absolute: false,
    prefer_simplified: true,
  }),
  approve: (id: string, digest: string) => post<{ job: Job }>(`/jobs/${id}/approve`, { digest }),
  recover: (id: string) => post<{ job: Job }>(`/jobs/${id}/recover`, { confirm: true }),
  cancel: (id: string) => post<{ job: Job }>(`/jobs/${id}/cancel`, { confirm: true }),
  keepExisting: (id: string) => post<{ job: Job }>(`/jobs/${id}/resolve`, {
    action: "keep_existing",
    confirm: true,
  }),
  retry: (id: string, options: JobRetryOptions = {}) => post<{ job: Job }>(`/jobs/${id}/retry`, options),
  remove: (id: string) => request<{ deleted: string }>(`/jobs/${id}`, { method: "DELETE" }),
  clearData: () => request<{ cleared: number }>("/jobs", { method: "DELETE" }),
  pause: (reason?: string) => post<GlobalControl>("/control/pause", reason ? { confirm: true, reason } : { confirm: true }),
  resume: () => post<GlobalControl>("/control/resume", { confirm: true }),
};
