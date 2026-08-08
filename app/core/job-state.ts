import type { Job, JobPhase } from "./contracts";

export const MEDIA_ROOT = "/quark/影视";
export const UNSCRAPED_MEDIA_ROOT = `${MEDIA_ROOT}/待刮削`;

export const ACTIVE_PHASES = new Set<JobPhase>([
  "queued", "analyzing", "identity_matching", "planning", "executing_media",
  "verifying", "cleaning", "retry_wait", "gap_discovering", "provider_searching",
  "acquiring", "staging_verifying", "child_planning", "child_executing",
  "final_verifying",
]);

export const TERMINAL_PHASES = new Set<JobPhase>([
  "completed", "failed", "failed_identity", "failed_provider", "failed_write",
  "failed_verification", "failed_cleanup", "cancelled",
]);

type PhaseMeta = { label: string; detail: string; progress: number; step: number; tone: string };

export const PHASE: Record<JobPhase, PhaseMeta> = {
  queued: { label: "排队中", detail: "任务已经进入本地队列", progress: 4, step: 1, tone: "quiet" },
  analyzing: { label: "分析来源", detail: "正在扫描媒体、字幕和目录线索", progress: 12, step: 1, tone: "live" },
  identity_matching: { label: "自动识别", detail: "正在匹配作品、类型、季度与集数", progress: 24, step: 1, tone: "live" },
  planning: { label: "生成计划", detail: "系统正在生成媒体树、NFO、海报与清理动作", progress: 38, step: 2, tone: "live" },
  verifying: { label: "AList 核对", detail: "正在刷新目录并精确读取远端结果", progress: 82, step: 4, tone: "live" },
  cleaning: { label: "自动清理", detail: "正在清理本任务拥有的来源和暂存文件", progress: 94, step: 5, tone: "live" },
  retry_wait: { label: "自动重试等待", detail: "系统将在退避时间后继续处理", progress: 40, step: 2, tone: "attention" },
  gap_discovering: { label: "检查缺口", detail: "正在核对作品树、字幕、海报和媒体库", progress: 87, step: 4, tone: "live" },
  provider_searching: { label: "自动搜索补源", detail: "正在从已配置来源筛选合格候选", progress: 89, step: 4, tone: "live" },
  acquiring: { label: "自动获取补源", detail: "正在下载或复制候选到任务暂存区", progress: 91, step: 4, tone: "live" },
  staging_verifying: { label: "核对补源文件", detail: "正在检查暂存内容、大小和覆盖范围", progress: 93, step: 4, tone: "live" },
  child_planning: { label: "自动规划补源", detail: "正在为已到盘资源生成内部补源计划", progress: 95, step: 4, tone: "live" },
  child_executing: { label: "自动整理补源", detail: "正在把已核对资源写入正式媒体库", progress: 97, step: 4, tone: "live" },
  final_verifying: { label: "最终核对", detail: "正在确认补源结果并收口缺口", progress: 98, step: 5, tone: "live" },
  executing_media: { label: "整理文件", detail: "正在安全移动并校验文件", progress: 92, step: 4, tone: "live" },
  completed: { label: "整理完成", detail: "目标文件已经通过校验", progress: 100, step: 5, tone: "success" },
  failed: { label: "任务失败", detail: "自动尝试已经结束，请查看失败原因", progress: 0, step: 0, tone: "danger" },
  failed_identity: { label: "自动识别失败", detail: "系统已耗尽身份匹配策略", progress: 0, step: 0, tone: "danger" },
  failed_provider: { label: "补源失败", detail: "系统已耗尽当前补源尝试", progress: 0, step: 0, tone: "danger" },
  failed_write: { label: "写入失败", detail: "正式库写入未能完成", progress: 0, step: 0, tone: "danger" },
  failed_verification: { label: "核对失败", detail: "AList 回读未能确认最终状态", progress: 0, step: 0, tone: "danger" },
  failed_cleanup: { label: "清理失败", detail: "任务拥有的临时文件尚未清理完毕", progress: 0, step: 0, tone: "danger" },
  cancelled: { label: "已停止", detail: "任务没有继续执行", progress: 0, step: 0, tone: "quiet" },
};

export function canRetry(job: Job) {
  return job.phase.startsWith("failed");
}

export function isFailed(job: Job) {
  return job.phase.startsWith("failed");
}

export function formatDate(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit",
  }).format(date);
}

export function humanizeWarning(value: string) {
  const seasons = value.match(/识别并合并\s*(\d+)\s*个季度/);
  if (seasons) return `识别到 ${seasons[1]} 个季度，已分别进入对应 Season 目录。`;
  if (value.includes("字幕发布序号与连续视频集号错位")) {
    return "字幕编号与视频编号不一致，系统已按完整对应关系逐集校准。";
  }
  const overflow = value.match(/E(\d+)\s+超出第\s*(\d+)\s*季.*映射为\s+SP(\d+)/i);
  if (overflow) return `原 E${overflow[1]} 已按 TMDB 归入 S00E${overflow[3].padStart(2, "0")}。`;
  return value;
}
