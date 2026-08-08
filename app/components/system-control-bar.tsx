"use client";
import type { GlobalControl } from "../core/contracts";

export function SystemControlBar({ control, controlError, pending, onPause, onResume }: {
  control: GlobalControl | null;
  controlError: string;
  pending: boolean;
  onPause: () => void;
  onResume: () => void;
}) {
  const paused = !!control?.paused;
  const statusLabel = paused
    ? `已暂停${control?.reason ? ` · ${control.reason}` : ""}`
    : "运行中";
  return <section className="system-control-bar" aria-label="自动流程控制">
    <div className="system-control-group">
      <header><div><span>FLOW CONTROL</span><b>调度控制</b></div><i className={paused ? "paused" : "open"} aria-hidden="true" /></header>
      <p>{statusLabel}{paused ? "；新建与现有任务会保持排队，不会开始新的文件操作。" : "调度会按队列继续分析、整理和结果检查。"}</p>
      <div className="system-control-actions">
        {paused ? <button type="button" disabled={pending} onClick={onResume}>{pending ? "正在处理…" : "恢复自动流程"}</button>
          : <button type="button" disabled={pending} onClick={onPause}>{pending ? "正在处理…" : "暂停自动流程"}</button>}
        <span>{paused ? "恢复后才会继续派发排队任务。" : "暂停不会中断当前操作，只停止后续派发。"}</span>
      </div>
      {controlError ? <p className="error">流程控制读取失败：{controlError}</p> : null}
    </div>
  </section>;
}
