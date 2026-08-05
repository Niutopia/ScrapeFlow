import tempfile
import unittest
import io
from contextlib import contextmanager
from pathlib import Path

from engine.tools.subtitle_executor import (
    artifact_candidates, build_requests, build_selection, canonical_digest,
    execute_selection, prepare_selection, validate_candidate,
)


def chinese_payload():
    return "\n".join(
        f"Dialogue: 0,0:00:0{i}.00,0:00:01.00,Default,,0,0,0,,这是第{i}条中文字幕，我们现在开始。"
        for i in range(5)
    ).encode()


class ExactUploadMixin:
    def _payloads(self):
        if not hasattr(self, "_exact_payloads"):
            self._exact_payloads = {}
        return self._exact_payloads

    def exact_file_info(self, path):
        payload = self._payloads().get(path)
        if payload is None:
            return None
        return {"size": len(payload), "sha256": None, "version": "test"}

    def open_file_reader(self, path):
        payload = self._payloads().get(path)
        if payload is None:
            raise OSError(f"missing exact target: {path}")
        return io.BytesIO(payload)

    def upload_file(self, path, source, content_type="application/octet-stream"):
        if path in self._payloads():
            raise OSError(f"target exists: {path}")
        payload = source.read_bytes()
        self.upload_bytes(path, payload, content_type)
        self._payloads()[path] = payload

    def mkdir(self, _path):
        return None

    def remove(self, parent, names):
        for name in names:
            self._payloads().pop(parent.rstrip("/") + "/" + name, None)


class SubtitleExecutorTests(unittest.TestCase):
    def test_requests_deduplicate_multi_episode_rows(self):
        video = "/quark/影视/番剧/A/A - S01E01-E02.mkv"
        payload = build_requests({
            "confirmed_missing_chinese": [
                {"video_path": video, "label": "S01E01", "title": "A"},
                {"video_path": video, "label": "S01E02", "title": "A"},
            ],
            "pending_review_or_probe": [],
        })
        self.assertEqual(len(payload["requests"]), 1)
        self.assertEqual(payload["requests"][0]["labels"], ["S01E01", "S01E02"])

    def test_pending_request_is_probe_only_and_confirmation_gets_a_new_id(self):
        video = "/quark/影视/番剧/A/A - S01E01.mkv"
        pending = build_requests({
            "confirmed_missing_chinese": [],
            "pending_review_or_probe": [{"video_path": video, "title": "A"}],
        })["requests"][0]
        confirmed = build_requests({
            "confirmed_missing_chinese": [{"video_path": video, "title": "A"}],
            "pending_review_or_probe": [],
        })["requests"][0]
        self.assertEqual(pending["lane"], "subtitle_verification")
        self.assertEqual(pending["mutation_scope"], "probe_only_no_mutation")
        self.assertEqual(confirmed["lane"], "ensure_external_zh_CN")
        self.assertEqual(confirmed["mutation_scope"], "create_external_subtitle_only")
        self.assertNotEqual(pending["request_id"], confirmed["request_id"])

    def test_selection_requires_exact_identity_and_verified_chinese(self):
        requests = build_requests({
            "confirmed_missing_chinese": [{
                "video_path": "/quark/影视/番剧/A/A - S01E01.mkv",
                "label": "S01E01", "title": "A",
            }], "pending_review_or_probe": [],
        })
        good = validate_candidate({
            "candidate_id": "good", "path": "/quark/影视/ScrapeFlow/字幕备份/A - S01E01.ass",
            "extension": ".ass", "source_kind": "alist",
        }, chinese_payload())
        bad = validate_candidate({
            "candidate_id": "bad", "path": "/quark/影视/ScrapeFlow/字幕备份/A - S01E01.srt",
            "extension": ".srt", "source_kind": "alist",
        }, b"English subtitles without any Chinese dialogue. " * 10)
        selection = build_selection(requests, [bad, good])
        self.assertEqual(len(selection["selections"]), 1)
        self.assertEqual(selection["selections"][0]["candidate_id"], "good")
        self.assertTrue(selection["selections"][0]["target_path"].endswith(".zh-CN.ass"))
        self.assertEqual(selection["summary"]["video_mutations"], 0)

    def test_pending_existing_companion_remains_verification_only(self):
        requests = build_requests({
            "confirmed_missing_chinese": [],
            "pending_review_or_probe": [{
                "video_path": "/quark/影视/番剧/A/A - S01E01.mkv",
                "label": "S01E01", "title": "A",
            }],
        })
        candidate = validate_candidate({
            "candidate_id": "existing",
            "path": "/quark/影视/番剧/A/A - S01E01.ass",
            "extension": ".ass", "source_kind": "alist",
        }, chinese_payload())
        selection = build_selection(requests, [candidate])
        self.assertFalse(selection["selections"])
        self.assertFalse(selection["acquisition_requests"])
        self.assertEqual(
            selection["failures"][0]["status"],
            "subtitle_verification_required",
        )

    def test_confirmed_existing_companion_is_satisfied_without_new_target(self):
        requests = build_requests({
            "confirmed_missing_chinese": [{
                "video_path": "/quark/影视/番剧/A/A - S01E01.mkv",
                "label": "S01E01", "title": "A",
            }],
            "pending_review_or_probe": [],
        })
        candidate = validate_candidate({
            "candidate_id": "existing",
            "path": "/quark/影视/番剧/A/A - S01E01.ass",
            "extension": ".ass", "source_kind": "alist",
        }, chinese_payload())
        row = build_selection(requests, [candidate])["selections"][0]
        self.assertEqual(row["operation"], "already_satisfied_existing_companion")
        self.assertEqual(row["target_path"], candidate["path"])

    def test_manifest_requests_only_subtitle_member(self):
        candidates = artifact_candidates([{
            "torrent": {"paired_video_path": "A - S01E01.mkv", "files": [
                {"member_path": "A - S01E01.ass"},
                {"member_path": "A - S01E01.mkv"},
            ]},
        }])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["retrieval_mode"], "fetch_subtitle_member_only")
        validated = validate_candidate(candidates[0], None)
        self.assertFalse(validated["acquisition_request"]["include_video"])

    def test_ambiguous_different_payloads_are_isolated(self):
        requests = build_requests({"confirmed_missing_chinese": [{
            "video_path": "/quark/影视/番剧/A/A - S01E01.mkv", "title": "A",
        }], "pending_review_or_probe": []})
        candidates = []
        for cid, extra in (("one", b"1"), ("two", b"2")):
            candidates.append(validate_candidate({
                "candidate_id": cid,
                "path": f"/backup/{cid}/A - S01E01.ass",
                "extension": ".ass", "source_kind": "alist",
            }, chinese_payload() + extra))
        selection = build_selection(requests, candidates)
        self.assertFalse(selection["selections"])
        self.assertEqual(selection["failures"][0]["status"], "ambiguous_verified_candidates")

    def test_execution_digest_gate_idempotency_and_failure_isolation(self):
        payload = chinese_payload()
        digest = __import__("hashlib").sha256(payload).hexdigest()
        core = {
            "schema_version": 1, "kind": "subtitle_selection", "request_sha256": "r",
            "selections": [
                {"request_id": "ok", "lane": "ensure_external_zh_CN", "target_path": "/quark/影视/番剧/A/A.zh-CN.ass", "candidate_path": "/src/A.ass", "candidate_source_kind": "alist", "payload_sha256": digest},
                {"request_id": "bad", "lane": "ensure_external_zh_CN", "target_path": "/quark/影视/番剧/B/B.zh-CN.ass", "candidate_path": "/src/missing.ass", "candidate_source_kind": "alist", "payload_sha256": digest},
            ], "acquisition_requests": [], "failures": [],
        }
        selection = {**core, "selection_sha256": canonical_digest(core)}

        class FakeClient(ExactUploadMixin):
            def __init__(self): self.uploads = []
            def try_list(self, *_args, **_kwargs): return []
            def read_file_bytes(self, path, **_kwargs):
                if "missing" in path: raise OSError("missing")
                return payload
            def upload_bytes(self, path, data, content_type): self.uploads.append((path, data, content_type))

        client = FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            journal_path = Path(directory) / "journal.json"
            with self.assertRaises(ValueError):
                execute_selection(client, selection, approved_selection_sha256="0" * 64, journal_path=journal_path)
            journal = execute_selection(client, selection, approved_selection_sha256=selection["selection_sha256"], journal_path=journal_path)
            self.assertEqual([row["status"] for row in journal["records"]], ["created", "failed"])
            self.assertEqual(len(client.uploads), 1)
            execute_selection(client, selection, approved_selection_sha256=selection["selection_sha256"], journal_path=journal_path)
            self.assertEqual(len(client.uploads), 1)
            self.assertEqual(len(journal["records"]), 2)
            tampered = dict(selection)
            tampered["selections"] = [dict(selection["selections"][0], target_path="/quark/影视/番剧/X/X.zh-CN.ass")]
            with self.assertRaisesRegex(ValueError, "自身摘要"):
                execute_selection(client, tampered, approved_selection_sha256=selection["selection_sha256"], journal_path=journal_path)

    def test_execution_rejects_verification_lane_before_io_or_journal(self):
        core = {
            "schema_version": 1, "kind": "subtitle_selection", "request_sha256": "r",
            "selections": [{
                "request_id": "pending", "lane": "subtitle_verification",
                "target_path": "/quark/影视/番剧/A/A.zh-CN.ass",
                "candidate_path": "/src/A.ass", "candidate_source_kind": "alist",
                "payload_sha256": "0" * 64,
            }],
            "acquisition_requests": [], "failures": [],
        }
        selection = {**core, "selection_sha256": canonical_digest(core)}

        class NoIoClient:
            def __getattr__(self, _name):
                raise AssertionError("verification lane must not perform IO")

        with tempfile.TemporaryDirectory() as directory:
            journal_path = Path(directory) / "journal.json"
            with self.assertRaisesRegex(ValueError, "非 confirmed lane"):
                execute_selection(
                    NoIoClient(), selection,
                    approved_selection_sha256=selection["selection_sha256"],
                    journal_path=journal_path,
                )
            self.assertFalse(journal_path.exists())

    def test_failed_record_is_retryable_but_success_remains_idempotent(self):
        payload = chinese_payload()
        digest = __import__("hashlib").sha256(payload).hexdigest()
        core = {
            "schema_version": 1, "kind": "subtitle_selection", "request_sha256": "r",
            "selections": [{
                "request_id": "retry", "lane": "ensure_external_zh_CN",
                "target_path": "/quark/影视/番剧/A/A.zh-CN.ass",
                "candidate_path": "/src/A.ass", "candidate_source_kind": "alist",
                "payload_sha256": digest,
            }], "acquisition_requests": [], "failures": [],
        }
        selection = {**core, "selection_sha256": canonical_digest(core)}

        class FlakyClient(ExactUploadMixin):
            def __init__(self): self.attempts = 0; self.uploads = 0
            def try_list(self, *_args, **_kwargs): return []
            def read_file_bytes(self, *_args, **_kwargs):
                self.attempts += 1
                if self.attempts == 1: raise OSError("temporary")
                return payload
            def upload_bytes(self, *_args, **_kwargs): self.uploads += 1

        client = FlakyClient()
        with tempfile.TemporaryDirectory() as directory:
            journal_path = Path(directory) / "journal.json"
            first = execute_selection(
                client, selection, approved_selection_sha256=selection["selection_sha256"],
                journal_path=journal_path,
            )
            self.assertEqual(first["status"], "completed_with_isolated_failures")
            second = execute_selection(
                client, selection, approved_selection_sha256=selection["selection_sha256"],
                journal_path=journal_path,
            )
            self.assertEqual(second["status"], "success")
            self.assertEqual([row["status"] for row in second["records"]], ["failed", "created"])
            execute_selection(
                client, selection, approved_selection_sha256=selection["selection_sha256"],
                journal_path=journal_path,
            )
            self.assertEqual(client.uploads, 1)

    def test_local_verified_cache_requires_root_and_never_reads_video(self):
        payload = chinese_payload()
        digest = __import__("hashlib").sha256(payload).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); cache = root / "cache" / "r" / "value.ass"
            cache.parent.mkdir(parents=True); cache.write_bytes(payload)
            core = {
                "schema_version": 1, "kind": "subtitle_selection", "request_sha256": "r",
                "selections": [{
                    "request_id": "cache", "lane": "ensure_external_zh_CN",
                    "target_path": "/quark/影视/番剧/A/A.zh-CN.ass",
                    "candidate_path": str(cache),
                    "candidate_source_kind": "local_verified_cache",
                    "payload_sha256": digest,
                }], "acquisition_requests": [], "failures": [],
            }
            selection = {**core, "selection_sha256": canonical_digest(core)}
            class Client(ExactUploadMixin):
                def __init__(self): self.uploads = 0
                def try_list(self, *_args, **_kwargs): return []
                def read_file_bytes(self, *_args, **_kwargs):
                    raise AssertionError("cache execution must not read remote/video")
                def upload_bytes(self, *_args, **_kwargs): self.uploads += 1
            client = Client()
            rejected = execute_selection(
                client, selection, approved_selection_sha256=selection["selection_sha256"],
                journal_path=root / "rejected-journal.json",
            )
            self.assertEqual(rejected["records"][0]["status"], "failed")
            self.assertIn("缓存未授权", rejected["records"][0]["error"])
            journal = execute_selection(
                client, selection, approved_selection_sha256=selection["selection_sha256"],
                journal_path=root / "journal.json", local_cache_root=root / "cache",
            )
            self.assertEqual(journal["records"][0]["status"], "created")
            self.assertEqual(client.uploads, 1)

    def test_prepare_skips_absent_optional_root_but_blocks_real_scan_error(self):
        class FakeClient(ExactUploadMixin):
            def __init__(self, fail=False): self.fail = fail
            def try_list(self, root, **_kwargs):
                if root.endswith("/备份"): return None
                if self.fail: raise OSError("storage unavailable")
                return []
            def walk(self, *_args, **_kwargs): return []

        refined = {"confirmed_missing_chinese": [], "pending_review_or_probe": []}
        roots = ("/quark/影视/电影", "/quark/影视/ScrapeFlow/备份")
        prepared = prepare_selection(FakeClient(), refined, roots=roots)
        self.assertFalse(prepared["inventory_scan_failures"])
        self.assertEqual(
            prepared["inventory_missing_optional_roots"],
            ["/quark/影视/ScrapeFlow/备份"],
        )
        failed = prepare_selection(
            FakeClient(fail=True), refined, roots=("/quark/影视/电影",),
        )
        self.assertIn("storage unavailable", failed["inventory_scan_failures"][0]["error"])

    def test_item_pause_guard_aborts_before_remote_io_or_journal_record(self):
        payload = chinese_payload()
        digest = __import__("hashlib").sha256(payload).hexdigest()
        core = {
            "schema_version": 1, "kind": "subtitle_selection", "request_sha256": "r",
            "selections": [{
                "request_id": "paused", "lane": "ensure_external_zh_CN",
                "target_path": "/quark/影视/番剧/A/A.zh-CN.ass",
                "candidate_path": "/src/A.ass", "candidate_source_kind": "alist",
                "payload_sha256": digest,
            }], "acquisition_requests": [], "failures": [],
        }
        selection = {**core, "selection_sha256": canonical_digest(core)}

        class NoIoClient:
            def __getattr__(self, _name):
                raise AssertionError("暂停态不得访问远端")

        @contextmanager
        def paused_guard():
            raise RuntimeError("global_pause_active")
            yield

        with tempfile.TemporaryDirectory() as directory:
            journal_path = Path(directory) / "journal.json"
            with self.assertRaisesRegex(RuntimeError, "global_pause_active"):
                execute_selection(
                    NoIoClient(), selection,
                    approved_selection_sha256=selection["selection_sha256"],
                    journal_path=journal_path, item_guard=paused_guard,
                )
            self.assertFalse(journal_path.exists())

    def test_pause_after_first_item_stops_without_second_journal_or_upload(self):
        payload = chinese_payload()
        digest = __import__("hashlib").sha256(payload).hexdigest()
        selections = [
            {
                "request_id": request_id,
                "lane": "ensure_external_zh_CN",
                "target_path": f"/quark/影视/番剧/{request_id}/{request_id}.zh-CN.ass",
                "candidate_path": f"/src/{request_id}.ass",
                "candidate_source_kind": "alist", "payload_sha256": digest,
            }
            for request_id in ("one", "two")
        ]
        core = {
            "schema_version": 1, "kind": "subtitle_selection", "request_sha256": "r",
            "selections": selections, "acquisition_requests": [], "failures": [],
        }
        selection = {**core, "selection_sha256": canonical_digest(core)}

        class FakeClient(ExactUploadMixin):
            def __init__(self): self.uploads = []
            def try_list(self, *_args, **_kwargs): return []
            def read_file_bytes(self, path, **_kwargs): return payload
            def upload_bytes(self, path, _data, _content_type): self.uploads.append(path)

        client = FakeClient()
        entered = 0

        @contextmanager
        def pauses_after_first():
            nonlocal entered
            entered += 1
            if entered > 1:
                raise RuntimeError("global_pause_active")
            yield

        with tempfile.TemporaryDirectory() as directory:
            journal_path = Path(directory) / "journal.json"
            with self.assertRaisesRegex(RuntimeError, "global_pause_active"):
                execute_selection(
                    client, selection,
                    approved_selection_sha256=selection["selection_sha256"],
                    journal_path=journal_path, item_guard=pauses_after_first,
                )
            journal = __import__("json").loads(journal_path.read_text())
            self.assertEqual(
                [(row["request_id"], row["status"]) for row in journal["records"]],
                [("one", "created")],
            )
            self.assertEqual(len(client.uploads), 1)
            execute_selection(
                client, selection,
                approved_selection_sha256=selection["selection_sha256"],
                journal_path=journal_path,
            )
            journal = __import__("json").loads(journal_path.read_text())
            self.assertEqual(
                [(row["request_id"], row["status"]) for row in journal["records"]],
                [("one", "created"), ("two", "created")],
            )
            self.assertEqual(len(client.uploads), 2)

    def test_pause_after_last_item_blocks_terminal_journal_until_resume(self):
        payload = chinese_payload()
        digest = __import__("hashlib").sha256(payload).hexdigest()
        core = {
            "schema_version": 1, "kind": "subtitle_selection", "request_sha256": "r",
            "selections": [{
                "request_id": "last", "lane": "ensure_external_zh_CN",
                "target_path": "/quark/影视/番剧/A/A.zh-CN.ass",
                "candidate_path": "/src/A.ass", "candidate_source_kind": "alist",
                "payload_sha256": digest,
            }], "acquisition_requests": [], "failures": [],
        }
        selection = {**core, "selection_sha256": canonical_digest(core)}

        class FakeClient(ExactUploadMixin):
            def __init__(self): self.uploads = 0
            def try_list(self, *_args, **_kwargs): return []
            def read_file_bytes(self, *_args, **_kwargs): return payload
            def upload_bytes(self, *_args, **_kwargs): self.uploads += 1

        client = FakeClient()
        entered = 0

        @contextmanager
        def pauses_before_terminal_commit():
            nonlocal entered
            entered += 1
            if entered > 1:
                raise RuntimeError("global_pause_active")
            yield

        with tempfile.TemporaryDirectory() as directory:
            journal_path = Path(directory) / "journal.json"
            with self.assertRaisesRegex(RuntimeError, "global_pause_active"):
                execute_selection(
                    client, selection,
                    approved_selection_sha256=selection["selection_sha256"],
                    journal_path=journal_path,
                    item_guard=pauses_before_terminal_commit,
                )
            journal = __import__("json").loads(journal_path.read_text())
            self.assertEqual(journal["status"], "running")
            self.assertNotIn("completed_at", journal)
            self.assertEqual(journal["records"][0]["status"], "created")
            self.assertEqual(client.uploads, 1)

            resumed = execute_selection(
                client, selection,
                approved_selection_sha256=selection["selection_sha256"],
                journal_path=journal_path,
            )
            self.assertEqual(resumed["status"], "success")
            self.assertIn("completed_at", resumed)
            self.assertEqual(client.uploads, 1)


if __name__ == "__main__":
    unittest.main()
