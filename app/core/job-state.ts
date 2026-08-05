import type { Job, JobPhase } from "./contracts";

export const MEDIA_ROOT = "/quark/影视";
export const UNSCRAPED_MEDIA_ROOT = `${MEDIA_ROOT}/待刮削`;

export const APPROVAL_PHASES = new Set<JobPhase>([
  "awaiting_media_approval", "awaiting_recovery_approval",
]);

export const ACTIVE_PHASES = new Set<JobPhase>([
  "queued", "planning_archives", "starting_archive_execution", "extracting_archives",
  "planning_media", "starting_media_execution", "executing_media", "planning_recovery",
  "replenishing",
  "starting_recovery_execution", "executing_recovery", "cancelling",
]);

export const TERMINAL_PHASES = new Set<JobPhase>([
  "completed", "recovered", "failed", "cancelled",
]);

type PhaseMeta = { label: string; detail: string; progress: number; step: number; tone: string };

export const PHASE: Record<JobPhase, PhaseMeta> = {
  queued: { label: "排队中", detail: "任务已经进入本地队列", progress: 4, step: 1, tone: "quiet" },
  planning_archives: { label: "扫描压缩包", detail: "正在读取目录结构与归档信息", progress: 14, step: 1, tone: "live" },
  starting_archive_execution: { label: "准备解压", detail: "正在校验批准摘要", progress: 34, step: 2, tone: "live" },
  extracting_archives: { label: "正在解压", detail: "完成后会自动继续识别", progress: 46, step: 2, tone: "live" },
  planning_media: { label: "识别媒体", detail: "正在匹配作品、季度与集数", progress: 64, step: 2, tone: "live" },
  awaiting_media_approval: { label: "等待终审", detail: "整理计划已经就绪", progress: 78, step: 3, tone: "attention" },
  starting_media_execution: { label: "准备执行", detail: "正在锁定计划与远端状态", progress: 84, step: 4, tone: "live" },
  executing_media: { label: "整理文件", detail: "正在安全移动并校验文件", progress: 92, step: 4, tone: "live" },
  replenishing: { label: "自动查补", detail: "正常刮削已提交，正在检索并等待缺项到盘", progress: 96, step: 5, tone: "live" },
  planning_recovery: { label: "检查恢复", detail: "正在读取执行日志", progress: 52, step: 3, tone: "live" },
  awaiting_recovery_approval: { label: "确认恢复", detail: "恢复范围已生成", progress: 60, step: 3, tone: "attention" },
  starting_recovery_execution: { label: "准备恢复", detail: "正在校验恢复摘要", progress: 70, step: 4, tone: "live" },
  executing_recovery: { label: "安全恢复", detail: "正在还原整理前状态", progress: 84, step: 4, tone: "live" },
  cancelling: { label: "安全停止", detail: "正在等待当前操作退出", progress: 0, step: 4, tone: "attention" },
  recovery_required: { label: "需要恢复", detail: "上次写入未完整结束", progress: 0, step: 3, tone: "danger" },
  completed: { label: "整理完成", detail: "目标文件已经通过校验", progress: 100, step: 5, tone: "success" },
  recovered: { label: "恢复完成", detail: "文件已回到整理前状态", progress: 100, step: 5, tone: "success" },
  failed: { label: "任务失败", detail: "修复问题后可以原地重试", progress: 0, step: 0, tone: "danger" },
  cancelled: { label: "已停止", detail: "任务没有继续执行", progress: 0, step: 0, tone: "quiet" },
};

export function isRecoverable(job: Job) {
  return !!job.recovery_available && ["recovery_required", "failed"].includes(job.phase);
}

export function canDelete(job: Job) {
  if (isRecoverable(job)) return false;
  return TERMINAL_PHASES.has(job.phase)
    || job.phase === "awaiting_media_approval";
}

export function canRetry(job: Job) {
  return job.phase === "failed" && !job.recovery_available;
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

export function approvalCopy(job: Job) {
  if (job.phase === "awaiting_recovery_approval") return {
    kicker: "RECOVERY REVIEW", title: "确认恢复范围", action: "批准恢复",
    note: "批准后按 journal 还原文件，完成前不会启动新的整理。",
  };
  return {
    kicker: "FINAL REVIEW", title: "确认最终整理计划", action: "批准并执行",
    note: "这是修改媒体文件前的最后一次确认。请核对作品、路径与清理项。",
  };
}
