export const JOB_PHASES = [
  "queued", "planning_archives",
  "starting_archive_execution", "extracting_archives", "planning_media",
  "awaiting_media_approval", "starting_media_execution", "executing_media",
  "replenishing",
  "planning_recovery", "awaiting_recovery_approval",
  "starting_recovery_execution", "executing_recovery", "cancelling",
  "recovery_required", "completed", "recovered", "failed", "cancelled",
] as const;

export type JobPhase = typeof JOB_PHASES[number];
export type TargetCategory = "番剧" | "美剧" | "电影";

export type PlanSummary = {
  kind: "media" | "recovery";
  title?: string;
  year?: string;
  tmdb_id?: number;
  file_count?: number;
  normal_file_count?: number;
  source_root?: string;
  target_root?: string;
  warnings?: string[];
  warning_count?: number;
  notices?: Array<{ code: string; severity: string; requires_review: boolean; message: string; evidence?: Record<string, unknown> }>;
  decision_trace?: Record<string, unknown>;
  review?: {
    automation_eligible: boolean;
    risk_level: "low" | "medium" | "high";
    reasons: string[];
    destructive_cleanup_count?: number;
    match: {
      media_type?: string;
      tmdb_id?: number;
      title?: string;
      year?: string;
      confidence?: number;
      status?: string;
      top_candidates?: Array<{
        media_type: string;
        tmdb_id: number;
        title: string;
        year: string;
        status: string;
        confidence: number;
      }>;
    };
  };
  scan_report?: Record<string, unknown>;
  resource_gaps?: Array<{ kind: string; label: string; reason: string; files: string[] }>;
  resource_gap_count?: number;
  gap_count?: number;
  origin_job_id?: string;
  replenishment?: {
    status: string;
    round?: number;
    gap_count?: number | null;
    gaps?: Array<{ id?: string; label?: string }>;
    selection?: {
      provider?: "quark_share" | "quark_magnet" | "cloud_share" | "magnet";
      release_name?: string;
      resolution?: string;
      updated_at?: string | null;
      selected_gap_ids?: string[];
    };
    selections?: Array<{
      provider?: "quark_share" | "quark_magnet" | "cloud_share" | "magnet";
      release_name?: string;
      resolution?: string;
      updated_at?: string | null;
      selected_gap_ids?: string[];
    }>;
    followup_job_id?: string;
    followup_job_ids?: string[];
    candidate_count?: number;
    eligible_candidate_count?: number;
    covered_gap_count?: number;
    uncovered_gap_count?: number;
    rejection_reasons?: Record<string, number>;
    message?: string;
    evidence_status?: "sources_exhausted";
    failure_detail?: {
      stage: string;
      summary: string;
      evidence: string;
      technical_reason: string;
      upload_status: string;
      next_action: string;
    };
    projects?: Array<{
      status?: string;
      message?: string;
      gaps?: Array<{ id?: string; label?: string }>;
      selection?: {
        provider?: "quark_share" | "quark_magnet" | "cloud_share" | "magnet";
        release_name?: string;
        resolution?: string;
        updated_at?: string | null;
        selected_gap_ids?: string[];
      };
      selections?: Array<{
        provider?: "quark_share" | "quark_magnet" | "cloud_share" | "magnet";
        release_name?: string;
        resolution?: string;
        updated_at?: string | null;
        selected_gap_ids?: string[];
      }>;
    }>;
  };
  problem_files?: Array<{
    source: string;
    reason: string;
    target?: string | null;
  }>;
  problem_file_count?: number;
  cleanup_files?: Array<{ source: string; reason: string }>;
  cleanup_file_count?: number;
  cleanup_groups?: Array<{ reason: string; count: number; examples: string[]; truncated: boolean }>;
  cleanup_group_count?: number;
  truncated?: boolean;
};

export type Job = {
  id: string;
  source: string;
  parent: string;
  updated_at: string;
  phase: JobPhase;
  error: string | null;
  digest: string | null;
  plan: PlanSummary | null;
  settings?: {
    media_type: "auto" | "tv" | "movie" | "collection";
    tmdb_id: number | null;
    query: string | null;
    season: number | null;
    absolute: boolean;
  };
  recovery_available?: boolean;
  queue_position?: number | null;
  queue_kind?: "analysis" | "execution" | null;
  progress?: {
    stage: string;
    completed: number;
    total: number;
    percent: number;
    message: string;
  } | null;
};

export type JobRetryOptions = {
  tmdb_id?: number | null;
  media_type?: "tv" | "movie";
  query?: string | null;
  season?: number | null;
  archive_password?: string | null;
};

export type Health = {
  tmdb_configured: boolean;
  connected: boolean;
  message?: string;
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
    directory_state?: "pending_delete";
    selectable?: boolean;
    disabled_reason?: string;
  }>;
};
