export const JOB_PHASES = [
  "queued", "analyzing", "identity_matching", "planning", "executing_media",
  "verifying", "cleaning", "retry_wait", "gap_discovering", "provider_searching",
  "acquiring", "staging_verifying", "child_planning", "child_executing",
  "final_verifying", "completed", "failed", "failed_identity", "failed_provider",
  "failed_write", "failed_verification", "failed_cleanup", "cancelled",
] as const;

export type JobPhase = typeof JOB_PHASES[number];
export type ReplenishmentProvider = "http" | "torrent" | "local" | "unknown";

export type ReplenishmentSelection = {
  provider?: ReplenishmentProvider;
  release_name?: string;
  resolution?: string;
  updated_at?: string | null;
  selected_gap_ids?: string[];
};

export type ResourceGap = {
  id?: string;
  kind?: string;
  label?: string;
  reason?: string;
  files?: string[];
};

export type ReplenishmentSummary = {
  status: string;
  gap_count?: number | null;
  gaps?: ResourceGap[];
  selection?: ReplenishmentSelection;
  selections?: ReplenishmentSelection[];
  attempts?: number;
  next_retry_seconds?: number | null;
  message?: string;
  terminal?: boolean;
};

export type PlanSummary = {
  kind: "media";
  title?: string;
  tmdb_id?: number;
  file_count?: number;
  source_root?: string;
  target_root?: string;
  cleanup_file_count?: number;
  resource_gaps?: ResourceGap[];
  resource_gap_count?: number;
  gap_count?: number;
  replenishment?: ReplenishmentSummary | null;
};

export type Job = {
  id: string;
  source: string;
  parent: string;
  updated_at: string;
  phase: JobPhase;
  error: string | null;
  plan: PlanSummary | null;
  queue_position?: number | null;
  queue_kind?: "analysis" | "execution" | null;
  progress?: {
    stage: string;
    completed: number;
    total: number;
    percent: number;
    message: string;
  } | null;
  automatic_attempts?: number | {
    identity?: number;
    write?: number;
    acquisition?: number;
    total?: number;
    [stage: string]: number | undefined;
  } | null;
  next_retry_seconds?: number | null;
  identity?: {
    status?: "pending" | "matched" | "failed";
    tmdb_id?: number | null;
    media_type?: "tv" | "movie" | "collection" | null;
    title?: string | null;
    confidence?: number | null;
    reason?: string | null;
  } | null;
  readback?: {
    status?: "pending" | "verified" | "failed";
    checked_at?: string | null;
    message?: string | null;
  } | null;
};

export type Health = {
  ok: boolean;
  mode: "automatic";
  connected: boolean;
  tmdb_configured: boolean;
  engine_configured: boolean;
  message?: string;
  intake_monitoring: boolean;
  intake?: {
    enabled: boolean;
    root: string;
    scan_seconds: number;
    last_scan_at?: string | null;
    last_error?: string | null;
    last_scheduled_count?: number;
  };
  operations?: {
    jobs_total: number;
    jobs_active: number;
    jobs_failed: number;
    jobs_completed: number;
    provider_active: number;
    formal_write_workers: number;
    provider_workers: number;
    audit_running: boolean;
  };
};

export type GlobalControl = {
  paused: boolean;
  updated_at: string | null;
  reason: string | null;
  persistent: boolean;
};

export type BrowseResult = {
  path: string;
  parent: string | null;
  directories: Array<{
    name: string;
    path: string;
    task_phase?: JobPhase;
    selectable?: boolean;
    disabled_reason?: string;
  }>;
};

/**
 * Evidence that is intentionally exposed to the UI but is not, by itself, an
 * authorization to mutate the formal library.  The API currently returns
 * duplicates and empty directories as observations; the optional safety
 * fields let newer reports explain whether an operator may automate a follow-
 * up without making older reports incompatible.
 */
export type LibraryAuditSafety = {
  safe_to_automate?: boolean;
  safe_to_delete?: boolean;
  requires_review?: boolean;
  review_required?: boolean;
  safety?: string;
  safety_reason?: string;
  reason?: string;
};

export type LibraryAuditIdentityScope = LibraryAuditSafety & {
  kind?: "video_file" | "tv_metadata_source" | "empty_movie_directory" | string;
  video_path?: string;
  metadata_source?: string;
  media_paths?: string[];
};

export type LibraryAuditBootstrap = LibraryAuditSafety & {
  nfo_work_count?: number;
  unowned_work_count?: number;
  unowned_gap_count?: number;
  unowned_unknown_count?: number;
  /** Identity bootstrap is report-only unless this is explicitly true. */
  creates_owner_tasks?: boolean;
  read_only?: boolean;
  works?: Array<{
    tmdb_id?: number;
    target_root?: string;
    media_type?: "tv" | "movie" | string;
    identity_source?: string;
    identity_scope?: LibraryAuditIdentityScope;
  }>;
};

export type LibraryAuditDuplicate = LibraryAuditSafety & {
  basename?: string;
  size?: number;
  paths?: string[];
};

export type LibraryAuditEmptyDirectory = LibraryAuditSafety & {
  path: string;
};

export type LibraryAuditObservation = LibraryAuditSafety & {
  path?: string;
  type?: string;
  video_count?: number;
  subtitle_count?: number;
  nfo_count?: number;
  poster_count?: number;
  has_subtitle?: boolean;
  has_nfo?: boolean;
  has_poster?: boolean;
  nfo_path?: string | null;
  poster_path?: string | null;
  nfo_inherited?: boolean;
  poster_inherited?: boolean;
  metadata_source?: string | null;
};

export type LibraryAudit = {
  started_at?: string;
  finished_at?: string | null;
  status: "completed" | "unavailable" | "error" | "pending";
  available: boolean;
  /** Structural traversal only; this does not mean the media library is done. */
  complete: boolean;
  /** Business completion after structural, semantic, and job evidence closes. */
  library_complete?: boolean;
  clean: boolean | null;
  roots: Array<{
    path: string;
    status?: string;
    file_count?: number;
    directory_count?: number;
    error?: string;
  }>;
  counts: {
    files?: number;
    directories?: number;
    videos?: number;
    subtitles?: number;
    nfo?: number;
    posters?: number;
  };
  /** Observations are retained for review and are never implicit delete work. */
  duplicates?: LibraryAuditDuplicate[];
  empty_directories?: Array<string | LibraryAuditEmptyDirectory>;
  observations?: {
    video_files?: Array<{ path?: string; size?: number }>;
    subtitle_files?: Array<{ path?: string; size?: number }>;
    nfo_files?: Array<{ path?: string; size?: number }>;
    poster_files?: Array<{ path?: string; size?: number }>;
    media_directories?: LibraryAuditObservation[];
  };
  automatic_tasks?: Array<{
    kind: string;
    path?: string;
    task?: string;
    basename?: string;
    paths?: string[];
  } & LibraryAuditSafety>;
  errors?: Array<{ scope?: string; code?: string; path?: string; error_type?: string } & LibraryAuditSafety>;
  semantic?: {
    status?: string;
    /** Mirrors the top-level value in enriched reports. */
    library_complete?: boolean;
    gap_count?: number;
    unknown_count?: number;
    job_gap_count?: number;
    gaps?: ResourceGap[];
    job_gaps?: Array<{ kind?: string; path?: string; phase?: string; reason?: string }>;
    unknowns?: Array<{
      kind?: string;
      path?: string;
      reason?: string;
      target_root?: string | null;
      uncovered_video_paths?: string[];
      identity_scope?: LibraryAuditIdentityScope;
    } & LibraryAuditSafety>;
    bootstrap?: LibraryAuditBootstrap;
    works?: Array<{
      work?: string;
      target_root?: string | null;
      identity_sources?: string[];
      owner_job_id?: string;
      gaps?: ResourceGap[];
      unknown?: boolean;
      identity_scope?: LibraryAuditIdentityScope;
    }>;
    acquisition_projects?: Array<Record<string, unknown>>;
  };
};

/** Whether the latest report finished walking every configured root. */
export function isLibraryAuditTraversalComplete(audit: LibraryAudit | null | undefined) {
  return audit?.complete === true;
}

/**
 * Resolve the business-level completion flag, retaining compatibility with
 * reports written before ``library_complete`` was introduced.  An explicit
 * top-level flag wins, followed by the mirrored semantic flag; only then do we
 * derive the old meaning from clean structural/semantic evidence.
 */
export function isLibraryAuditComplete(audit: LibraryAudit | null | undefined) {
  if (!audit) return false;
  if (typeof audit.library_complete === "boolean") return audit.library_complete;
  if (typeof audit.semantic?.library_complete === "boolean") return audit.semantic.library_complete;
  if (!isLibraryAuditTraversalComplete(audit) || audit.clean !== true) return false;
  const semantic = audit.semantic;
  if (semantic?.status && !["completed", "complete", "ok"].includes(semantic.status.toLowerCase())) {
    return false;
  }
  const semanticFindings = (semantic?.gap_count ?? semantic?.gaps?.length ?? 0)
    + (semantic?.unknown_count ?? semantic?.unknowns?.length ?? 0)
    + (semantic?.job_gap_count ?? semantic?.job_gaps?.length ?? 0);
  return (audit.automatic_tasks?.length ?? 0) === 0
    && (audit.errors?.length ?? 0) === 0
    && semanticFindings === 0;
}
