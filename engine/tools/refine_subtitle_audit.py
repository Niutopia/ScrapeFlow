#!/usr/bin/env python3
"""Refine external-sidecar gaps with bounded read-only content/stream probes."""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
from typing import Any, Callable, Mapping

from engine.scrapeflow.clients.http import redact_sensitive_text
from engine.scraper import AListClient


TEXT_SUBTITLE_EXTS = {".ass", ".ssa", ".srt", ".vtt", ".txt"}
CONTENT_CLASSIFIER_VERSION = 5
TEXT_STREAM_CODECS = {"ass", "ssa", "subrip", "srt", "webvtt", "mov_text", "text"}
BITMAP_STREAM_CODECS = {"hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle", "xsub"}
STREAM_TIMEOUT_REASONS = {
    "ffmpeg_timeout", "ffmpeg_fast_seek_timeout",
    "packet_capture_timeout_without_language_evidence",
}
STREAM_UNDETERMINED_REASONS = {
    "packet_content_undetermined", "ffprobe_packet_nonzero_exit",
}
FORMAL_LIBRARY_ROOTS = (
    "/quark/影视/电影", "/quark/影视/番剧", "/quark/影视/美剧",
)
CHINESE_MARKERS = set(
    "这么个们说对从还没过种现经只进着与将开关问间点体应实认学当无听书画风云电东业两习买乱争于亚产亲价众优伟传伤会侠养写军决况净凉减几击划则刚创删别剧务动势华协单卖卫历压县参双变叶号叹吗吨听启员响团园围图圆场块坚坛执扩扫扬扰抚抢护报担拟拥拦拧拨择挥损换据揽携摄摆摇摊敌数断旧显晓暂术机杀杂权条来极构枪标栏树样档桥梦检欢欧残气汇汉汤沟没泽洁洒浅浆浇测济浓涂涛润涨渔湾湿溃满滤滥滨滩潜灭灯灵灾灿炉点炼烁烂烛烟烦烧烫热爱爷牵独狮狱猎猪猫献环现电监盖盘着矿码砖礼祸种积称稳竞笼签简紧纠红纤约级纪纬纯纲纳纵纷纸纹线练组细织终绍经绑结绕绘给络绝统绣继续绿编缘缚缝缩网罗罚罢职联聪肃肠肤肿胀胁胆胜胶脉脏脑脚脱脸腻腾舰舱艳艺节芦苍苏范茧药获莲莱营萧蓝虑虚虽蚀蚁蚂蚊蛮蜕蝴补装裤袭见观规视览觉誉计订认讨让训议讯记讲许论设访证评识诈诉词译试诗诚话询该详语误说请诸读课谁调谈谋谢谱贝负财责贤败账货质购贯贺贼资赏赔赖赚赛赞赠赢赵赶趋跃践踪车轨转轮软轰轻载较辅辆辈辉辑输辖边达迁过迈运还进远违连迟选递逻遗邻郑释里鉴钟钢钥钱铁铃铜铝银铺链销锁锅锋锐错锦键镇镜长门闪闭问闯闲间闷闹闻阁阅队阳阴阵阶际陆陈险随隐难雾静顶项顺须顾顿预领频题颜额风飞饥饭饮饰饱馆马驰驱驻驾骂验骑骗鱼鲁鲜鸟鸡鸣鸭鹅鹤鹰麦黄齐齿龙龟"
)
TRADITIONAL_MARKERS = set(
    "亂亞來俠個們偉傳傷價優兩別刪則剛創劃劇動務勝勢匯協參員問啓單嗎嘆噸圍園圓圖團執堅報場塊塗壇壓夢學實寫將對幾從悶愛慮應掃揚換揮損搖搶撥撫擁擇擊擔據擬擰擴擺擾攔攜攝攤攬敗敵數斷於暫曉書會東條業極構槍標樣樹橋機檔檢欄權歐歡歷殘殺氣決沒況涼淨淺減測湯溝溼滅滿漁漢漲漿潔潛潤潰澆澤濃濟濤濫濱濾灑灘灣災無煉煙煩熱燈燒燙營燦燭爍爐爛爭爺牽獄獅獨獲獵獻現環產畫當監盤碼磚礦禍禮種稱積穩競節範簡籠籤糾紀約紅紋納純紙級紛細紹終組結絕絡給統綁經綠綱網緊線緣編緯練縛縣縫縮縱織繞繡繪繭繼續纖罰罵罷羅習聞聯聰職聽肅脅脈脫脹腦腫腳腸膚膠膩膽臉與舊艙艦華萊葉蒼蓋蓮蕭藍藝藥蘆蘇虛號蛻蝕螞蟻蠻衆術衛裏補裝褲襲見規視親覺覽觀訂計訊討訓記訪設許訴詐評詞詢試詩話該詳認語誠誤說誰課調談請論諸謀講謝證識譜譯議護譽讀變讓豔豬貓貝負財貨貫責買賀資賊賞賠賢賣質賬賴賺購賽贈贊贏趕趙趨踐蹤躍車軌軍軟較載輔輕輛輝輩輪輯輸轄轉轟這連進運過達違遞遠遲遷選遺邁還邊邏鄭鄰釋鈴銀銅銳銷鋁鋒鋪鋼錢錦錯鍋鍵鍾鎖鎮鏈鏡鐵鑑鑰長門閃閉開閒間閣閱闖關陣陰陳陸陽隊階際隨險隱雖雙雜雞難雲電霧靈靜響頂項順須預頓領頻題額顏顧顯風飛飢飯飲飽飾養館馬馳駐駕騎騙騰驅驗髒體鬧魚魯鮮鳥鳴鴨鵝鶴鷹麥麼黃點齊齒龍龜"
)


def classify_subtitle_content(payload: bytes, suffix: str = ".ass") -> dict[str, Any]:
    """Classify a bounded subtitle prefix without pretending binary formats are text."""
    if suffix.casefold() not in TEXT_SUBTITLE_EXTS:
        return {"status": "undetermined", "reason": "binary_or_bitmap_subtitle"}
    if not payload:
        return {"status": "undetermined", "reason": "empty_payload"}
    text = None
    encoding = None
    replacement_characters = 0
    if payload.startswith(b"\xef\xbb\xbf"):
        # A BOM is authoritative encoding evidence.  Real-world ASS files can
        # contain one damaged trailer byte; falling through to UTF-16 turns an
        # otherwise valid UTF-8 file into one line of CJK-looking gibberish and
        # can falsely label Simplified Chinese as Japanese.  Keep the bounded
        # payload usable and expose the replacement count as audit evidence.
        text = payload.decode("utf-8-sig", errors="replace")
        encoding = "utf-8-sig"
        replacement_characters = text.count("\ufffd")
    else:
        encodings = (
            ("utf-16", "utf-8-sig", "gb18030", "big5")
            if payload.startswith((b"\xff\xfe", b"\xfe\xff"))
            else ("utf-8-sig", "gb18030", "big5", "utf-16")
        )
        for candidate in encodings:
            try:
                text = payload.decode(candidate)
                encoding = candidate
                break
            except UnicodeDecodeError:
                continue
    if text is None:
        return {"status": "undetermined", "reason": "unknown_encoding"}
    if "\x00" in text[:4096]:
        return {"status": "undetermined", "reason": "binary_payload"}
    dialogue = []
    normalized_suffix = suffix.casefold()
    for raw_line in text.splitlines():
        is_ass_dialogue = raw_line.casefold().startswith("dialogue:")
        is_ass_packet = (
            normalized_suffix in {".ass", ".ssa"}
            and re.match(r"^\s*\d+\s*,\s*\d+\s*,", raw_line) is not None
        )
        if normalized_suffix in {".ass", ".ssa"}:
            if not is_ass_dialogue and not is_ass_packet:
                continue
        line = re.sub(r"\{[^}]*\}", "", raw_line)
        if is_ass_dialogue:
            parts = line.split(",", 9)
            line = parts[-1] if len(parts) == 10 else line
        elif is_ass_packet:
            parts = line.split(",", 8)
            line = parts[-1] if len(parts) == 9 else line
        if re.fullmatch(r"\s*(?:\d+:)?\d{1,2}:\d{2}[,.]\d+\s*--?>.*", line):
            continue
        if normalized_suffix in {".srt", ".vtt"} and (
            re.fullmatch(r"\s*\d+\s*", line)
            or line.strip().casefold() == "webvtt"
        ):
            continue
        if not line.strip():
            continue
        dialogue.append(line)
    sample = "\n".join(dialogue)
    han = re.findall(r"[\u3400-\u9fff]", sample)
    kana = re.findall(r"[\u3040-\u30ff]", sample)
    chinese_markers = sum(character in CHINESE_MARKERS for character in han)
    traditional_markers = sum(character in TRADITIONAL_MARKERS for character in han)
    han_only_lines = sum(
        1 for line in dialogue
        if len(re.findall(r"[\u3400-\u9fff]", line)) >= 4
        and not re.search(r"[\u3040-\u30ff]", line)
    )
    evidence = {
        "encoding": encoding,
        "han_characters": len(han),
        "kana_characters": len(kana),
        "simplified_markers": chinese_markers,
        "traditional_markers": traditional_markers,
        "han_only_lines": han_only_lines,
        "decode_replacement_characters": replacement_characters,
        "timed_text_lines": len(dialogue),
        "classifier_version": CONTENT_CLASSIFIER_VERSION,
    }
    if (
        len(han) >= 20 and chinese_markers >= 3 and han_only_lines >= 3
        and chinese_markers >= traditional_markers
    ):
        return {"status": "chinese", "language_variant": "simplified_chinese", **evidence}
    if (
        len(han) >= 20 and traditional_markers >= 3 and han_only_lines >= 3
        and traditional_markers > chinese_markers
    ):
        return {"status": "non_chinese", "language_variant": "traditional_chinese", **evidence}
    if len(kana) >= 10:
        return {"status": "japanese", **evidence}
    letters = len(re.findall(r"[A-Za-z]", sample))
    if letters >= 80 and len(han) < 10:
        return {"status": "non_chinese", **evidence, "latin_characters": letters}
    return {"status": "undetermined", "reason": "insufficient_language_evidence", **evidence}


def _safe_ffprobe_headers(headers: Mapping[str, Any]) -> str | None:
    output = []
    for raw_name, raw_value in headers.items():
        name = str(raw_name).strip()
        value = str(raw_value).strip()
        if not re.fullmatch(r"[A-Za-z0-9-]+", name) or "\r" in value or "\n" in value:
            return None
        output.append(f"{name}: {value}\r\n")
    return "".join(output)


def classify_subtitle_streams(streams: list[dict[str, Any]]) -> dict[str, Any]:
    if not streams:
        return {"status": "no_subtitle_stream", "streams": []}
    classified = []
    has_chinese = False
    has_unknown = False
    for stream in streams:
        tags = stream.get("tags") if isinstance(stream.get("tags"), Mapping) else {}
        language = str(tags.get("language") or "").casefold().replace("_", "-")
        title = str(tags.get("title") or "")
        marker = f"{language} {title}".casefold()
        title_token = re.sub(r"[\s._-]+", "", title).casefold()
        if (
            language in {"zh", "zho", "chi", "zh-cn", "zh-hans", "chs"}
            or title_token in {"sc", "chs"}
            or re.search(
            r"(?:中文|简中|简体|简日|简英|chinese|chs|hans)", marker, re.I
            )
        ):
            kind = "chinese"
            has_chinese = True
        elif title_token in {"tc", "cht"}:
            kind = "non_chinese"
        elif language in {"", "und", "unknown", "mul", "mis"}:
            kind = "unknown"
            has_unknown = True
        elif language in {"ja", "jpn", "jp", "en", "eng", "ko", "kor", "zh-tw", "zh-hant", "cht"} or language:
            kind = "non_chinese"
        classified.append({
            "index": stream.get("index"),
            "codec_name": stream.get("codec_name"),
            "language": language,
            "title": title,
            "classification": kind,
        })
    if has_chinese:
        status = "embedded_chinese"
    elif has_unknown:
        status = "subtitle_stream_language_unknown"
    else:
        status = "embedded_non_chinese_only"
    return {"status": status, "streams": classified}


def probe_remote_subtitle_streams(
    alist: AListClient, video_path: str, *, timeout: int = 30,
) -> dict[str, Any]:
    ffprobe = shutil.which("ffprobe")
    if ffprobe is None:
        return {"status": "probe_failed", "error": "ffprobe_not_installed"}
    try:
        raw_url, headers = alist.file_link(video_path, refresh=True)
        safe_headers = _safe_ffprobe_headers(headers)
        if safe_headers is None:
            return {"status": "probe_failed", "error": "unsafe_provider_headers"}
        command = [
            ffprobe, "-v", "error", "-rw_timeout", "15000000",
            "-probesize", "10000000", "-analyzeduration", "10000000",
        ]
        if safe_headers:
            command.extend(["-headers", safe_headers])
        command.extend([
            "-select_streams", "s", "-show_entries",
            "stream=index,codec_name:stream_tags=language,title", "-of", "json", raw_url,
        ])
        completed = subprocess.run(
            command, check=False, capture_output=True, text=True, timeout=timeout,
        )
        if completed.returncode != 0:
            return {
                "status": "probe_failed",
                "error": "ffprobe_nonzero_exit",
                "returncode": completed.returncode,
                "stderr": redact_sensitive_text(
                    completed.stderr,
                    secrets=[raw_url, *(str(value) for value in headers.values())],
                ),
            }
        payload = json.loads(completed.stdout)
        streams = payload.get("streams")
        if not isinstance(streams, list) or not all(isinstance(row, dict) for row in streams):
            return {"status": "probe_failed", "error": "invalid_ffprobe_output"}
        return classify_subtitle_streams(streams)
    except subprocess.TimeoutExpired:
        return {"status": "probe_failed", "error": "ffprobe_timeout"}
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "probe_failed", "error": type(exc).__name__}


def extract_remote_text_subtitle_stream(
    alist: AListClient,
    video_path: str,
    stream_index: int,
    *,
    timeout: int = 30,
) -> dict[str, Any]:
    """Extract short samples using input-side seeks instead of scanning from zero."""
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        return {"status": "undetermined", "reason": "ffmpeg_not_installed"}
    try:
        raw_url, headers = alist.file_link(video_path, refresh=True)
        safe_headers = _safe_ffprobe_headers(headers)
        if safe_headers is None:
            return {"status": "undetermined", "reason": "unsafe_provider_headers"}
        attempts: list[dict[str, Any]] = []
        # Matroska text tracks often expose an early dialogue packet without
        # requiring a costly remote seek/index lookup.  Try the cheap linear
        # read first, then fall back to two bounded input-side seeks.
        for offset_seconds in (0, 300, 600):
            command = [ffmpeg, "-v", "error", "-rw_timeout", "10000000"]
            if safe_headers:
                command.extend(["-headers", safe_headers])
            command.extend([
                "-ss", str(offset_seconds), "-i", raw_url,
                "-map", f"0:{stream_index}", "-t", "90",
                "-f", "srt", "pipe:1",
            ])
            try:
                completed = subprocess.run(
                    command, check=False, capture_output=True, timeout=timeout,
                )
            except subprocess.TimeoutExpired:
                attempts.append({"offset_seconds": offset_seconds, "status": "timeout"})
                continue
            if completed.returncode != 0:
                attempts.append({
                    "offset_seconds": offset_seconds,
                    "status": "nonzero_exit",
                    "returncode": completed.returncode,
                })
                continue
            result = classify_subtitle_content(completed.stdout[:256 * 1024], ".srt")
            attempts.append({
                "offset_seconds": offset_seconds,
                "status": result.get("status"),
                "sample_bytes": min(len(completed.stdout), 256 * 1024),
            })
            if result.get("status") != "undetermined":
                return {
                    **result,
                    "stream_index": stream_index,
                    "sample_bytes": min(len(completed.stdout), 256 * 1024),
                    "seek_offset_seconds": offset_seconds,
                    "attempts": attempts,
                }
        reason = (
            "ffmpeg_fast_seek_timeout"
            if attempts and all(row["status"] == "timeout" for row in attempts)
            else "fast_seek_content_undetermined"
        )
        return {
            "status": "undetermined",
            "reason": reason,
            "stream_index": stream_index,
            "attempts": attempts,
        }
    except (OSError, ValueError) as exc:
        return {"status": "undetermined", "reason": type(exc).__name__, "stream_index": stream_index}


def decode_ffprobe_packet_hexdump(payload: bytes) -> bytes:
    """Recover packet bytes from ffprobe ``-show_data`` output, even if JSON is partial."""
    output = bytearray()
    text = payload.decode("utf-8", errors="ignore")
    for line in text.splitlines():
        match = re.search(
            r"(?:^|\\n)[0-9A-Fa-f]{8}:\s+((?:[0-9A-Fa-f]{4}\s+){1,8})",
            line,
        )
        if not match:
            continue
        try:
            output.extend(bytes.fromhex("".join(match.group(1).split())))
        except ValueError:
            continue
    return bytes(output)


def extract_remote_text_subtitle_packets(
    alist: AListClient,
    video_path: str,
    stream_index: int,
    *,
    timeout: int = 6,
) -> dict[str, Any]:
    """Read subtitle packets without decoding the full remote media timeline."""
    ffprobe = shutil.which("ffprobe")
    if ffprobe is None:
        return {"status": "undetermined", "reason": "ffprobe_not_installed"}
    try:
        raw_url, headers = alist.file_link(video_path, refresh=True)
        safe_headers = _safe_ffprobe_headers(headers)
        if safe_headers is None:
            return {"status": "undetermined", "reason": "unsafe_provider_headers"}
        packet_chunks: list[bytes] = []
        attempts: list[dict[str, Any]] = []
        for interval in ("%+180", "300%+180"):
            command = [ffprobe, "-v", "error", "-rw_timeout", "10000000"]
            if safe_headers:
                command.extend(["-headers", safe_headers])
            command.extend([
                "-select_streams", str(stream_index),
                "-read_intervals", interval,
                "-show_packets", "-show_data",
                "-show_entries", "packet=data", "-of", "json", raw_url,
            ])
            timed_out = False
            try:
                completed = subprocess.run(
                    command, check=False, capture_output=True, timeout=timeout,
                )
                raw_output = completed.stdout
                returncode = completed.returncode
            except subprocess.TimeoutExpired as exc:
                raw_output = exc.stdout or b""
                returncode = None
                timed_out = True
            packet_bytes = decode_ffprobe_packet_hexdump(raw_output[:1024 * 1024])
            packet_chunks.append(packet_bytes)
            combined = b"\n".join(packet_chunks)[:512 * 1024]
            result = classify_subtitle_content(combined, ".ass")
            attempts.append({
                "interval": interval,
                "returncode": returncode,
                "timed_out": timed_out,
                "sample_bytes": len(packet_bytes),
                "combined_sample_bytes": len(combined),
                "status": result.get("status"),
            })
            if result.get("status") != "undetermined":
                return {
                    **result,
                    "stream_index": stream_index,
                    "sample_bytes": len(combined),
                    "capture_timed_out_after_sample": any(
                        row["timed_out"] for row in attempts
                    ),
                    "capture_attempts": attempts,
                }
        packet_bytes = b"\n".join(packet_chunks)[:512 * 1024]
        result = classify_subtitle_content(packet_bytes, ".ass")
        return {
            **result,
            "status": "undetermined",
            "reason": (
                "packet_capture_timeout_without_language_evidence"
                if attempts and all(row["timed_out"] for row in attempts) else
                "packet_content_undetermined"
                if any(row["returncode"] == 0 for row in attempts) else
                "ffprobe_packet_nonzero_exit"
            ),
            "stream_index": stream_index,
            "sample_bytes": len(packet_bytes),
            "capture_attempts": attempts,
        }
    except (OSError, ValueError) as exc:
        return {"status": "undetermined", "reason": type(exc).__name__, "stream_index": stream_index}


def refine_rows(
    rows: list[dict[str, Any]],
    *,
    content_results: Mapping[str, dict[str, Any]],
    probe_results: Mapping[str, dict[str, Any]],
    stream_results: Mapping[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    extracted = stream_results or {}
    confirmed: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    resolved: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        reason = str(row.get("reason_code") or "")
        external_language_undetermined = False
        # ``companion_subtitles`` is always the exact stem-paired set; a
        # mismatch row has none by construction.  Any companion whose content
        # verifies as Chinese satisfies the video regardless of its filename
        # hint (``.ja.ass``/``.zh-TW.ass`` files routinely carry Simplified
        # Chinese text), so the check applies to every row with companions.
        paths = [str(path) for path in row.get("companion_subtitles", [])]
        results = [content_results.get(path, {"status": "undetermined"}) for path in paths]
        row["external_content_evidence"] = results
        if any(result.get("status") == "chinese" for result in results):
            row["resolution"] = "external_chinese_content_confirmed"
            resolved.append(row)
            continue
        if any(result.get("status") == "undetermined" for result in results):
            # A filename hint is not language evidence, so an undetermined
            # companion (insufficient evidence, transient read failure, bitmap
            # sidecar) must keep the row pending, never confirmed missing.
            external_language_undetermined = True
        elif reason == "subtitle_language_unverified" and not results:
            # Preserve the original conservative path when the unverified
            # reason carries no companions at all.  Rows with companions only
            # enter pending above, on undetermined content.
            external_language_undetermined = True
        probe = probe_results.get(str(row.get("video_path") or ""), {
            "status": "probe_failed", "error": "probe_not_run",
        })
        row["embedded_probe"] = probe
        status = probe.get("status")
        if status == "subtitle_stream_language_unknown":
            unknown_streams = [
                stream for stream in probe.get("streams", [])
                if stream.get("classification") == "unknown"
            ]
            stream_evidence = [
                extracted.get(
                    f"{row.get('video_path')}#stream={stream.get('index')}",
                    {"status": "undetermined", "reason": "stream_extraction_not_run"},
                )
                for stream in unknown_streams
            ]
            row["stream_content_evidence"] = stream_evidence
            if any(result.get("status") == "chinese" for result in stream_evidence):
                row["resolution"] = "embedded_chinese_content_confirmed"
                resolved.append(row)
            elif stream_evidence and all(
                result.get("status") in {"japanese", "non_chinese"}
                for result in stream_evidence
            ) and not external_language_undetermined:
                row["confirmation"] = "confirmed_missing_chinese_subtitle"
                confirmed.append(row)
            else:
                row["pending_reason"] = (
                    "bitmap_subtitle_ocr_required"
                    if any(
                        stream.get("codec_name") in BITMAP_STREAM_CODECS
                        for stream in unknown_streams
                    )
                    else "text_stream_content_undetermined"
                )
                row["recommended_action"] = (
                    "OCR bitmap subtitle sample, then classify Chinese variant"
                    if row["pending_reason"] == "bitmap_subtitle_ocr_required"
                    else "retry bounded text extraction or inspect a local subtitle sample"
                )
                pending.append(row)
        elif status == "embedded_chinese":
            row["resolution"] = "embedded_chinese_confirmed"
            resolved.append(row)
        elif status in {"no_subtitle_stream", "embedded_non_chinese_only"}:
            if external_language_undetermined:
                row["pending_reason"] = "external_subtitle_language_undetermined"
                row["recommended_action"] = "inspect or transcode the external subtitle sample manually"
                pending.append(row)
            else:
                row["confirmation"] = "confirmed_missing_chinese_subtitle"
                confirmed.append(row)
        else:
            row["pending_reason"] = str(status or "probe_failed")
            row["recommended_action"] = "retry read-only media probe; never schedule from this row"
            pending.append(row)
    return {"confirmed_missing_chinese": confirmed, "pending_review_or_probe": pending, "resolved_with_chinese": resolved}


def formal_library_category(path: str) -> str | None:
    for root in FORMAL_LIBRARY_ROOTS:
        if path == root or path.startswith(root + "/"):
            return PurePosixPath(root).name
    return None


def _counts_by_formal_root(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(
        formal_library_category(str(row.get("video_path") or ""))
        for row in rows
    )
    return {category: counts.get(category, 0) for category in ("电影", "番剧", "美剧")}


def _unique_videos_by_formal_root(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        category: len({
            str(row.get("video_path") or "")
            for row in rows
            if formal_library_category(str(row.get("video_path") or "")) == category
        })
        for category in ("电影", "番剧", "美剧")
    }


def _unique_videos_by_media_type(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        media_type: len({
            str(row.get("video_path") or "")
            for row in rows if row.get("media_type") == media_type
        })
        for media_type in ("movie", "tv", "extra")
    }


def _load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": 1,
            "external_content": {},
            "video_probes": {},
            "stream_content": {},
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("字幕探测缓存格式无效")
    payload.setdefault("external_content", {})
    payload.setdefault("video_probes", {})
    payload.setdefault("stream_content", {})
    return payload


def invalidate_stale_content_cache(cache: dict[str, Any]) -> int:
    """Drop stale text classifications after decoder or language-policy changes.

    Before the UTF-8 BOM fix, a valid ASS prefix with one damaged trailer byte
    could fall through to UTF-16 and be cached as Japanese/non-Chinese.  Keep
    Current-version evidence is preserved.  Older positive Chinese evidence is
    also reread because zh-CN now requires Simplified-vs-Traditional content
    evidence rather than generic Han text.
    """
    removed = 0
    external = cache.get("external_content")
    if not isinstance(external, dict):
        return removed
    for path, result in list(external.items()):
        if not isinstance(result, Mapping):
            continue
        suffix = PurePosixPath(path).suffix.casefold()
        stale_bom_negative = (
            suffix in TEXT_SUBTITLE_EXTS
            and result.get("classifier_version") != CONTENT_CLASSIFIER_VERSION
            and result.get("encoding") == "utf-16"
            and result.get("status") in {"japanese", "non_chinese"}
        )
        stale_undetermined = (
            suffix in TEXT_SUBTITLE_EXTS
            and result.get("status") == "undetermined"
            and result.get("reason") in {"empty_or_binary_payload", "unknown_encoding"}
        )
        stale_language_variant = (
            suffix in TEXT_SUBTITLE_EXTS
            and result.get("status") in {"chinese", "non_chinese"}
            and result.get("classifier_version") != CONTENT_CLASSIFIER_VERSION
        )
        if stale_bom_negative or stale_undetermined or stale_language_variant:
            del external[path]
            removed += 1
    return removed


def invalidate_stream_probe_cache(
    cache: dict[str, Any], *, retry_timeouts: bool, retry_undetermined: bool,
) -> int:
    """Remove only explicitly selected negative stream evidence for a bounded retry."""
    stream_content = cache.get("stream_content")
    if not isinstance(stream_content, dict):
        return 0
    reasons = set()
    if retry_timeouts:
        reasons.update(STREAM_TIMEOUT_REASONS)
    if retry_undetermined:
        reasons.update(STREAM_UNDETERMINED_REASONS)
    removed = 0
    for key, result in list(stream_content.items()):
        if isinstance(result, Mapping) and result.get("reason") in reasons:
            del stream_content[key]
            removed += 1
    return removed


def invalidate_stale_stream_content_cache(cache: dict[str, Any]) -> int:
    """Drop stream evidence from older text or language-policy classifiers."""
    stream_content = cache.get("stream_content")
    if not isinstance(stream_content, dict):
        return 0
    removed = 0
    for key, result in list(stream_content.items()):
        if (
            isinstance(result, Mapping)
            and result.get("status") in {"chinese", "non_chinese", "japanese"}
            and result.get("classifier_version") != CONTENT_CLASSIFIER_VERSION
        ):
            del stream_content[key]
            removed += 1
    return removed


def text_streams_to_extract(
    video_probes: Mapping[str, Any],
) -> dict[str, tuple[str, int]]:
    """Return every unknown text stream that still needs content evidence.

    A container can expose Simplified and Traditional Chinese as sibling
    tracks with identical or missing language tags.  Sampling only the first
    track leaves the remaining tracks permanently ``stream_extraction_not_run``
    and prevents the audit from converging.  Bitmap streams stay on the OCR
    path and are deliberately excluded here.
    """
    streams: dict[str, tuple[str, int]] = {}
    for video_path, raw_probe in video_probes.items():
        if not isinstance(raw_probe, Mapping) or raw_probe.get("status") != (
            "subtitle_stream_language_unknown"
        ):
            continue
        raw_streams = raw_probe.get("streams")
        if not isinstance(raw_streams, list):
            continue
        for stream in raw_streams:
            if not isinstance(stream, Mapping):
                continue
            index = stream.get("index")
            if (
                stream.get("classification") != "unknown"
                or stream.get("codec_name") not in TEXT_STREAM_CODECS
                or not isinstance(index, int)
            ):
                continue
            key = f"{video_path}#stream={index}"
            streams[key] = (str(video_path), index)
    return streams


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--content-workers", type=int, default=8)
    parser.add_argument("--probe-workers", type=int, default=3)
    parser.add_argument("--probe-timeout", type=int, default=30)
    parser.add_argument("--stream-timeout", type=int, default=12)
    parser.add_argument("--stream-method", choices=("packet", "fastseek"), default="packet")
    parser.add_argument("--max-stream-extractions", type=int)
    parser.add_argument("--retry-timeout-streams", action="store_true")
    parser.add_argument("--retry-undetermined-streams", action="store_true")
    parser.add_argument("--retry-failed-video-probes", action="store_true")
    parser.add_argument("--skip-stream-extractions", action="store_true")
    parser.add_argument("--skip-video-probes", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.content_workers <= 16 or not 1 <= args.probe_workers <= 4:
        raise ValueError("字幕读取/媒体探测并发数超出安全范围")
    source = json.loads(args.input.read_text(encoding="utf-8"))
    rows = [
        dict(row) for row in source.get("missing_subtitles", [])
        if isinstance(row, dict)
        and formal_library_category(str(row.get("video_path") or "")) is not None
    ]
    compliant_rows = [
        {**dict(row), "resolution": "external_required_language_present"}
        for row in source.get("subtitle_inventory", [])
        if isinstance(row, dict)
        and row.get("status") == "external_required_language_present"
        and formal_library_category(str(row.get("video_path") or "")) is not None
    ]
    cache = _load_cache(args.cache)
    client = AListClient(
        os.environ.get("ALIST_URL", "http://127.0.0.1:5244"),
        os.environ.get("ALIST_USERNAME", ""), os.environ.get("ALIST_PASSWORD", ""),
        allow_insecure_http=True,
    )
    client.login()

    if args.retry_failed_video_probes:
        for path, result in list(cache["video_probes"].items()):
            if result.get("status") == "probe_failed":
                del cache["video_probes"][path]
    invalidate_stream_probe_cache(
        cache,
        retry_timeouts=args.retry_timeout_streams,
        retry_undetermined=args.retry_undetermined_streams,
    )
    invalidate_stale_stream_content_cache(cache)

    invalidate_stale_content_cache(cache)

    # Content-classify every companion, not only rows whose reason already
    # demands it.  Misnamed sidecars are common (``.ja.ass`` carrying Simplified
    # Chinese, ``.zh-TW.ass`` carrying Traditional): only content evidence lets
    # the refinement decide correctly, so a ``required_subtitle_language_missing``
    # row must not stay permanently unverifiable just because its filename hint
    # is not ``zh-CN``.
    external_paths = sorted({
        str(path)
        for row in rows
        for path in row.get("companion_subtitles", [])
        if isinstance(path, str) and path
    })
    missing_external = [path for path in external_paths if path not in cache["external_content"]]

    def inspect_external(path: str) -> tuple[str, dict[str, Any]]:
        try:
            payload = client.read_file_prefix(path, max_bytes=128 * 1024)
            return path, {
                **classify_subtitle_content(payload, PurePosixPath(path).suffix),
                "classifier_version": CONTENT_CLASSIFIER_VERSION,
            }
        except Exception as exc:  # remote read failures remain explicit pending evidence
            return path, {"status": "undetermined", "reason": type(exc).__name__}

    with ThreadPoolExecutor(max_workers=args.content_workers) as executor:
        futures = [executor.submit(inspect_external, path) for path in missing_external]
        for index, future in enumerate(as_completed(futures), 1):
            path, result = future.result()
            cache["external_content"][path] = result
            if index % 100 == 0:
                _write_json(args.cache, cache)

    preliminary = refine_rows(rows, content_results=cache["external_content"], probe_results={})
    videos_to_probe = sorted({
        str(row.get("video_path") or "")
        for row in preliminary["confirmed_missing_chinese"] + preliminary["pending_review_or_probe"]
        if row.get("video_path")
        and row.get("pending_reason") != "external_subtitle_language_undetermined"
    })
    missing_videos = [
        path for path in videos_to_probe
        if path not in cache["video_probes"] and not args.skip_video_probes
    ]
    with ThreadPoolExecutor(max_workers=args.probe_workers) as executor:
        future_paths = {
            executor.submit(
                probe_remote_subtitle_streams, client, path, timeout=args.probe_timeout,
            ): path for path in missing_videos
        }
        for index, future in enumerate(as_completed(future_paths), 1):
            path = future_paths[future]
            try:
                cache["video_probes"][path] = future.result()
            except Exception as exc:  # one remote must not discard the evidence batch
                cache["video_probes"][path] = {"status": "probe_failed", "error": type(exc).__name__}
            if index % 25 == 0:
                _write_json(args.cache, cache)

    _write_json(args.cache, cache)
    streams_to_extract = text_streams_to_extract(cache["video_probes"])
    missing_streams = {
        key: value for key, value in streams_to_extract.items()
        if key not in cache["stream_content"] and not args.skip_stream_extractions
    }
    if args.max_stream_extractions is not None:
        if args.max_stream_extractions <= 0:
            raise ValueError("max-stream-extractions 必须大于 0")
        missing_streams = dict(sorted(missing_streams.items())[:args.max_stream_extractions])
    with ThreadPoolExecutor(max_workers=args.probe_workers) as executor:
        extractor = (
            extract_remote_text_subtitle_packets
            if args.stream_method == "packet"
            else extract_remote_text_subtitle_stream
        )
        future_keys = {
            executor.submit(
                extractor, client, video_path, stream_index,
                timeout=args.stream_timeout,
            ): key
            for key, (video_path, stream_index) in missing_streams.items()
        }
        for index, future in enumerate(as_completed(future_keys), 1):
            key = future_keys[future]
            try:
                cache["stream_content"][key] = future.result()
            except Exception as exc:
                cache["stream_content"][key] = {
                    "status": "undetermined", "reason": type(exc).__name__,
                }
            if index % 25 == 0:
                _write_json(args.cache, cache)
    _write_json(args.cache, cache)
    refined = refine_rows(
        rows,
        content_results=cache["external_content"],
        probe_results=cache["video_probes"],
        stream_results=cache["stream_content"],
    )
    refined["resolved_with_chinese"] = compliant_rows + refined["resolved_with_chinese"]
    payload = {
        "schema_version": 1,
        "audited_at": datetime.now(timezone.utc).isoformat(),
        "source_audit": str(args.input),
        "policy": {
            "required_language": "zh-CN",
            "external_prefix_bytes": 128 * 1024,
            "probe_workers": args.probe_workers,
            "probe_timeout_seconds": args.probe_timeout,
            "stream_timeout_seconds": args.stream_timeout,
            "stream_method": args.stream_method,
            "video_probes_skipped": args.skip_video_probes,
            "remote_mutations": False,
        },
        "pending_actions": {
            "text_stream_content_undetermined": "retry bounded extraction or inspect a local text sample",
            "bitmap_subtitle_ocr_required": "OCR a bounded PGS/VobSub sample before language classification",
            "external_subtitle_language_undetermined": "manually inspect or transcode the external subtitle",
            "probe_failed": "retry the read-only probe; failure is never a confirmed gap",
        },
        "summary": {
            "input_gaps": len(rows),
            "audited_inventory_rows": len(source.get("subtitle_inventory", [])),
            "confirmed_missing_chinese": len(refined["confirmed_missing_chinese"]),
            "pending_review_or_probe": len(refined["pending_review_or_probe"]),
            "resolved_with_chinese": len(refined["resolved_with_chinese"]),
            "confirmed_by_formal_root": _counts_by_formal_root(refined["confirmed_missing_chinese"]),
            "pending_by_formal_root": _counts_by_formal_root(refined["pending_review_or_probe"]),
            "resolved_by_formal_root": _counts_by_formal_root(refined["resolved_with_chinese"]),
            "audited_videos_by_formal_root": _unique_videos_by_formal_root([
                row for row in source.get("subtitle_inventory", []) if isinstance(row, dict)
            ]),
            "confirmed_videos_by_formal_root": _unique_videos_by_formal_root(refined["confirmed_missing_chinese"]),
            "pending_videos_by_formal_root": _unique_videos_by_formal_root(refined["pending_review_or_probe"]),
            "resolved_videos_by_formal_root": _unique_videos_by_formal_root(refined["resolved_with_chinese"]),
            "audited_videos_by_media_type": _unique_videos_by_media_type([
                row for row in source.get("subtitle_inventory", []) if isinstance(row, dict)
            ]),
            "confirmed_videos_by_media_type": _unique_videos_by_media_type(refined["confirmed_missing_chinese"]),
            "pending_videos_by_media_type": _unique_videos_by_media_type(refined["pending_review_or_probe"]),
            "resolved_videos_by_media_type": _unique_videos_by_media_type(refined["resolved_with_chinese"]),
            "external_content_statuses": dict(Counter(
                row.get("status") for row in cache["external_content"].values()
            )),
            "video_probe_statuses": dict(Counter(
                row.get("status") for row in cache["video_probes"].values()
            )),
            "stream_content_statuses": dict(Counter(
                row.get("status") for row in cache["stream_content"].values()
            )),
        },
        **refined,
    }
    _write_json(args.output, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
