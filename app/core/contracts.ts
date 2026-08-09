export const JOB_PHASES = [
  "awaiting_target_shelf", "queued", "analyzing", "archive_preprocessing", "identity_matching",
  "target_policy_conflict", "planning", "executing_media",
  "verifying", "cleaning", "retry_wait", "gap_discovering", "provider_searching",
  "acquiring", "staging_verifying", "subtitle_installing", "child_planning", "child_executing",
  "final_verifying", "completed", "completed_with_gaps", "failed", "failed_archive", "failed_identity", "failed_planning", "failed_provider",
  "failed_write", "failed_verification", "failed_cleanup", "cancelled",
] as const;

export type JobPhase = typeof JOB_PHASES[number];
/** Closed API contract; the backend remains the only path-mapping authority. */
export type TargetShelf = "movie" | "anime" | "us_tv";
// The current backend exposes provider identity (magnet), while acquisition
// kind (torrent) is a separate field in the provider capability contract.
// cloud_share remains representable for unavailable legacy state, but it is
// never an active/ready provider.
export type ReplenishmentProvider = "magnet" | "cloud_share" | "unknown";

export type ProviderCapability = {
  status: "ready" | "unavailable" | "deferred" | string;
  acquisition_kinds?: string[];
  materializer?: string | null;
  reason?: string;
};

export type ReplenishmentSelection = {
  provider?: ReplenishmentProvider;
  acquisition_kind?: "torrent" | string;
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
  target_work_path?: string | null;
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
  /** User-confirmed stable shelf, or null while the task is waiting. */
  target_shelf?: TargetShelf | null;
  /** Fixed first-level shelf root, never the concrete work directory. */
  target_root?: string | null;
  /** Engine-planned concrete work directory after planning succeeds. */
  target_work_path?: string | null;
  allowed_target_shelves?: TargetShelf[];
  selected_at?: string | null;
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
  build_version?: string | null;
  build_commit?: string | null;
  build_time?: string | null;
  message?: string;
  provider_capabilities?: Record<string, ProviderCapability>;
  intake_monitoring: boolean;
  intake?: {
    enabled: boolean;
    root: string;
    scan_seconds: number;
    last_scan_at?: string | null;
    last_error?: string | null;
    last_scheduled_count?: number;
    last_registered_count?: number;
  };
  operations?: {
    jobs_total: number;
    jobs_awaiting_target_shelf?: number;
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

/**
 * A retained file which the audit makes visible without treating it as an
 * implicit cleanup instruction.  In particular, archive and unknown files
 * remain operator-review evidence until a later, explicit workflow owns them.
 */
export type LibraryAuditResidualObservation = LibraryAuditSafety & {
  path?: string;
  size?: number;
  residual_kind?: string;
  action?: string;
  evidence?: string[];
};

export type LibraryAuditOrphanSubtitle = LibraryAuditSafety & {
  path?: string;
  size?: number;
  reason?: string;
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
  residuals?: LibraryAuditResidualObservation[];
  archives?: LibraryAuditResidualObservation[];
  attachments?: LibraryAuditResidualObservation[];
  unknown_files?: LibraryAuditResidualObservation[];
  orphan_subtitles?: LibraryAuditOrphanSubtitle[];
  observations?: {
    video_files?: Array<{ path?: string; size?: number }>;
    subtitle_files?: Array<{ path?: string; size?: number }>;
    nfo_files?: Array<{ path?: string; size?: number }>;
    poster_files?: Array<{ path?: string; size?: number }>;
    media_directories?: LibraryAuditObservation[];
    residuals?: LibraryAuditResidualObservation[];
    archives?: LibraryAuditResidualObservation[];
    attachments?: LibraryAuditResidualObservation[];
    unknown_files?: LibraryAuditResidualObservation[];
    orphan_subtitles?: LibraryAuditOrphanSubtitle[];
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
