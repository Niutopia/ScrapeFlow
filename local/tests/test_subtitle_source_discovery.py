import json
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from engine.scrapeflow.subtitle_member_acquisition import (
    bind_source_manifest,
    build_search_batches,
)
from engine.tools.subtitle_executor import canonical_digest
from local.scrapeflow_api.subtitle_source_discovery import (
    SubtitleSourceDiscoveryRuntime,
)


def search_fixture():
    requests = {"requests": [{
        "request_id": "req1", "video_path": "/quark/影视/番剧/My Show/My Show - S01E02.mkv",
        "target_root": "/quark/影视/番剧/My Show", "title": "My Show",
        "season": 1, "episodes": [2], "lane": "ensure_external_zh_CN",
    }]}
    selection = {"failures": [{
        "request_id": "req1", "status": "no_verified_zh_CN_candidate",
    }]}
    return build_search_batches(requests, selection)


def open_control():
    return {"paused": False, "persistent": True}


class SubtitleSourceDiscoveryRuntimeTests(unittest.TestCase):
    def test_pending_first_attempt_is_not_starved_by_smaller_retryable_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); queue = root / "queue"
            runtime = SubtitleSourceDiscoveryRuntime(queue, root / "manifests")
            runtime.enqueue(search_fixture())
            original = next(queue.glob("*.json"))
            retryable = queue / "000000000000000000000000.json"
            pending = queue / "ffffffffffffffffffffffff.json"
            row = json.loads(original.read_text())
            original.unlink()
            due = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
            retryable.write_text(json.dumps({
                **row, "batch_id": "000000000000000000000000",
                "status": "retryable", "attempts": 50,
                "next_attempt_at": due,
            }))
            pending.write_text(json.dumps({
                **row, "batch_id": "ffffffffffffffffffffffff",
                "status": "pending", "attempts": 0,
                "next_attempt_at": due,
            }))

            selected = runtime._due_task()

            self.assertIsNotNone(selected)
            self.assertEqual(selected[0], pending)

    def test_retryable_batches_rotate_by_due_time_not_filename(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); queue = root / "queue"
            runtime = SubtitleSourceDiscoveryRuntime(queue, root / "manifests")
            runtime.enqueue(search_fixture())
            original = next(queue.glob("*.json"))
            later = queue / "000000000000000000000000.json"
            earlier = queue / "ffffffffffffffffffffffff.json"
            row = json.loads(original.read_text())
            original.unlink()
            now = datetime.now(timezone.utc)
            later.write_text(json.dumps({
                **row, "batch_id": "000000000000000000000000",
                "status": "retryable", "attempts": 1,
                "next_attempt_at": (now - timedelta(seconds=10)).isoformat(),
            }))
            earlier.write_text(json.dumps({
                **row, "batch_id": "ffffffffffffffffffffffff",
                "status": "retryable", "attempts": 2,
                "next_attempt_at": (now - timedelta(minutes=2)).isoformat(),
            }))

            selected = runtime._due_task()

            self.assertIsNotNone(selected)
            self.assertEqual(selected[0], earlier)

    def test_import_or_paused_startup_never_recovers_running_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); queue = root / "queue"; manifests = root / "manifests"
            runtime = SubtitleSourceDiscoveryRuntime(queue, manifests, poll_seconds=0.01)
            runtime.enqueue(search_fixture())
            task_path = next(queue.glob("*.json"))
            task = json.loads(task_path.read_text()); task["status"] = "running"
            task_path.write_text(json.dumps(task))
            before = task_path.read_bytes()
            # Constructor and a paused dispatch are both zero-write.
            runtime = SubtitleSourceDiscoveryRuntime(queue, manifests, poll_seconds=0.01)
            result = runtime.run_once(
                lambda _batch: self.fail("paused worker ran"),
                lambda: {"paused": True, "persistent": True},
            )
            self.assertFalse(result["ran"])
            self.assertEqual(task_path.read_bytes(), before)

    def test_pause_winning_after_provider_read_leaves_no_terminal_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = SubtitleSourceDiscoveryRuntime(root / "queue", root / "manifests")
            runtime.enqueue(search_fixture())
            control = open_control()

            @contextmanager
            def guard():
                if control["paused"]:
                    raise RuntimeError("paused")
                yield

            def discover(_batch):
                control["paused"] = True
                return {"manifests": [], "provider_telemetry": {}, "search_complete": True}

            result = runtime.run_once(discover, lambda: control, item_guard=guard)
            task = json.loads(next((root / "queue").glob("*.json")).read_text())
            self.assertEqual(result["status"], "paused_without_terminal_commit")
            self.assertEqual(task["status"], "running")
            self.assertFalse((root / "manifests").exists())

    def test_open_restart_recovers_and_persists_valid_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); queue = root / "queue"; manifests = root / "manifests"
            runtime = SubtitleSourceDiscoveryRuntime(queue, manifests, retry_seconds=1)
            runtime.enqueue(search_fixture())
            task_path = next(queue.glob("*.json"))
            task = json.loads(task_path.read_text()); task["status"] = "running"
            task_path.write_text(json.dumps(task))
            manifest = bind_source_manifest(
                provider="quark_share", locator="quark_share:share",
                release_name="My Show S01E02", search_request_ids=["req1"],
                acquisition={"share_id": "share"}, files=[
                    {"file_id": "v", "path": "My Show - S01E02.mkv", "size": 1000},
                    {"file_id": "s", "path": "My Show - S01E02.ass", "size": 200},
                ],
            )
            result = runtime.run_once(
                lambda _batch: {"manifests": [manifest], "provider_telemetry": {}, "search_complete": True},
                open_control,
            )
            self.assertEqual(result["status"], "completed")
            self.assertTrue((manifests / f"{manifest['manifest_sha256']}.json").exists())
            self.assertEqual(json.loads(task_path.read_text())["attempts"], 1)

    def test_required_complete_with_capped_optional_page_keeps_paging(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = SubtitleSourceDiscoveryRuntime(
                root / "queue", root / "manifests", retry_seconds=7,
            )
            runtime.enqueue(search_fixture())
            before = datetime.now(timezone.utc)

            result = runtime.run_once(
                lambda _batch: {
                    "manifests": [], "search_complete": True,
                    "provider_telemetry": {"torrent": {
                        "optional_sources_capped": ["mikan"],
                    }},
                },
                open_control,
            )

            task = json.loads(next((root / "queue").glob("*.json")).read_text())
            due = datetime.fromisoformat(task["next_attempt_at"])
            self.assertEqual(result["status"], "retryable")
            self.assertEqual(task["last_error"], "optional_source_pages_remaining")
            self.assertEqual(task["version"], runtime.VERSION)
            self.assertGreaterEqual(due, before + timedelta(seconds=6))
            self.assertLessEqual(due, before + timedelta(seconds=8))

    def test_enqueue_wakes_a_running_background_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = SubtitleSourceDiscoveryRuntime(root / "queue", root / "manifests", poll_seconds=60)
            calls = []
            runtime.start(
                lambda batch: calls.append(batch) or {"manifests": [], "provider_telemetry": {}, "search_complete": True},
                open_control,
            )
            runtime.enqueue(search_fixture())
            deadline = time.monotonic() + 2
            while not calls and time.monotonic() < deadline:
                time.sleep(0.01)
            runtime.stop()
            self.assertEqual(len(calls), 1)

    def test_parallel_workers_claim_distinct_durable_tasks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); queue = root / "queue"; manifests = root / "manifests"
            seed = SubtitleSourceDiscoveryRuntime(queue, manifests)
            seed.enqueue(search_fixture())
            original = next(queue.glob("*.json"))
            row = json.loads(original.read_text())
            original.unlink()
            for index in range(4):
                batch_id = f"{index + 1:024x}"
                (queue / f"{batch_id}.json").write_text(json.dumps({
                    **row,
                    "batch_id": batch_id,
                    "batch": {**row["batch"], "title": f"Show {index + 1}"},
                    "status": "pending",
                }))
            calls = []
            release_workers = threading.Event()

            def discover(batch):
                calls.append(batch["title"])
                release_workers.wait(timeout=2)
                return {
                    "manifests": [], "provider_telemetry": {},
                    "search_complete": True,
                }

            runtime = SubtitleSourceDiscoveryRuntime(
                queue, manifests, poll_seconds=60, worker_count=4,
            )
            runtime.start(
                discover,
                open_control,
            )
            deadline = time.monotonic() + 2
            while len(calls) < 4 and time.monotonic() < deadline:
                time.sleep(0.01)
            active_snapshot = runtime.snapshot()

            self.assertEqual(len(calls), 4)
            self.assertEqual(len(set(calls)), 4)
            self.assertEqual(active_snapshot["worker_count"], 4)
            self.assertEqual(active_snapshot["active_worker_count"], 4)
            self.assertTrue(active_snapshot["running"])
            self.assertEqual(active_snapshot["worker_threads_alive"], 4)
            release_workers.set()
            deadline = time.monotonic() + 2
            while (
                runtime.snapshot()["active_worker_count"]
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            idle_snapshot = runtime.snapshot()
            runtime.stop()
            self.assertEqual(idle_snapshot["active_worker_count"], 0)
            self.assertFalse(idle_snapshot["running"])
            self.assertEqual(idle_snapshot["worker_threads_alive"], 4)
            self.assertTrue(all(
                json.loads(path.read_text())["status"] == "completed"
                for path in queue.glob("*.json")
            ))

    def test_completed_empty_batch_stays_inert_until_title_refresh_is_due(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = SubtitleSourceDiscoveryRuntime(
                root / "queue", root / "manifests", refresh_seconds=60,
            )
            runtime.enqueue(search_fixture())
            task_path = next((root / "queue").glob("*.json"))
            task = json.loads(task_path.read_text())
            task["status"] = "completed"
            task["updated_at"] = (
                datetime.now(timezone.utc) - timedelta(days=30)
            ).isoformat()
            task["next_attempt_at"] = (
                datetime.now(timezone.utc) + timedelta(minutes=1)
            ).isoformat()
            task_path.write_text(json.dumps(task))

            result = runtime.enqueue(search_fixture())

            self.assertEqual(result["reopened"], 0)
            self.assertEqual(result["existing"], 1)
            self.assertEqual(json.loads(task_path.read_text())["status"], "completed")
            self.assertIsNone(runtime._due_task())

    def test_same_title_reopens_completed_empty_batch_after_refresh_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = SubtitleSourceDiscoveryRuntime(
                root / "queue", root / "manifests", refresh_seconds=60,
            )
            runtime.enqueue(search_fixture())
            task_path = next((root / "queue").glob("*.json"))
            task = json.loads(task_path.read_text())
            task["status"] = "completed"
            task["next_attempt_at"] = (
                datetime.now(timezone.utc) - timedelta(seconds=1)
            ).isoformat()
            task_path.write_text(json.dumps(task))

            result = runtime.enqueue(search_fixture())
            reopened = json.loads(task_path.read_text())

            self.assertEqual(result["reopened"], 1)
            self.assertEqual(result["existing"], 0)
            self.assertEqual(reopened["status"], "retryable")
            self.assertEqual(reopened["last_error"], "periodic_no_manifest_refresh")
            self.assertIsNotNone(runtime._due_task())

    def test_unrelated_title_enqueue_never_rewrites_completed_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); queue = root / "queue"
            runtime = SubtitleSourceDiscoveryRuntime(queue, root / "manifests")
            original = search_fixture()
            runtime.enqueue(original)
            original_path = next(queue.glob("*.json"))
            original_task = json.loads(original_path.read_text())
            original_task["status"] = "completed"
            original_path.write_text(json.dumps(original_task))
            before = original_path.read_bytes()

            other = search_fixture()
            batch = dict(other["batches"][0])
            batch["request_ids"] = ["other-title-request"]
            core = {key: batch.get(key) for key in (
                "target_root", "title", "aliases", "request_ids", "query_terms", "providers",
                "manifest_requirement", "member_policy",
            )}
            batch["search_batch_id"] = canonical_digest(core)[:24]
            body = {
                "schema_version": 1, "kind": "subtitle_search_batches",
                "batches": [batch], "ambiguity_cases": [],
            }
            result = runtime.enqueue({
                **body, "search_batches_sha256": canonical_digest(body),
            })

            self.assertEqual(result["archived"], 0)
            self.assertEqual(original_path.read_bytes(), before)
            self.assertEqual(len(list(queue.glob("*.json"))), 2)

    def test_ambiguous_candidates_are_persisted_in_independent_evidence_queue(self):
        requests = {"requests": [{
            "request_id": "req1", "video_path": "/quark/影视/番剧/A/A - S01E01.mkv",
            "target_root": "/quark/影视/番剧/A", "title": "A",
            "season": 1, "episodes": [1], "lane": "ensure_external_zh_CN",
        }]}
        selection = {"failures": [{
            "request_id": "req1", "status": "ambiguous_verified_candidates",
            "candidate_ids": ["one", "two"],
            "candidate_evidence": [{"candidate_id": "one"}, {"candidate_id": "two"}],
        }]}
        search = build_search_batches(requests, selection)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = SubtitleSourceDiscoveryRuntime(root / "queue", root / "manifests")
            runtime.enqueue(search)
            rows = list((root / "queue/ambiguities").glob("*.json"))
            self.assertEqual(len(rows), 1)
            state = json.loads(rows[0].read_text())
            self.assertEqual(state["status"], "awaiting_deterministic_evidence")
            self.assertEqual(state["candidate_ids"], ["one", "two"])

    def test_candidate_failure_reopens_completed_batch_immediately(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = SubtitleSourceDiscoveryRuntime(root / "queue", root / "manifests")
            runtime.enqueue(search_fixture())
            task_path = next((root / "queue").glob("*.json"))
            task = json.loads(task_path.read_text()); task["status"] = "completed"
            task_path.write_text(json.dumps(task))
            self.assertEqual(runtime.reopen_for_request(
                "req1", reason="candidate_resource_failed",
            ), 1)
            reopened = json.loads(task_path.read_text())
            self.assertEqual(reopened["status"], "retryable")
            self.assertEqual(reopened["last_error"], "candidate_resource_failed")

    def test_completed_legacy_algorithm_with_manifests_reopens_on_enqueue(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = SubtitleSourceDiscoveryRuntime(root / "queue", root / "manifests")
            runtime.enqueue(search_fixture())
            task_path = next((root / "queue").glob("*.json"))
            task = json.loads(task_path.read_text())
            task.update({
                "version": 1, "status": "completed",
                "manifest_sha256s": ["a" * 64],
            })
            task_path.write_text(json.dumps(task))

            result = runtime.enqueue(search_fixture())

            self.assertEqual(result["reopened"], 1)
            reopened = json.loads(task_path.read_text())
            self.assertEqual(reopened["version"], runtime.VERSION)
            self.assertEqual(reopened["status"], "retryable")
            self.assertEqual(
                reopened["last_error"], "discovery_algorithm_upgraded_v3",
            )
            self.assertEqual(reopened["manifest_sha256s"], ["a" * 64])

    def test_retry_accumulates_resource_failures_for_capped_page_exclusion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); queue = root / "queue"
            runtime = SubtitleSourceDiscoveryRuntime(
                queue, root / "manifests", retry_seconds=1,
            )
            runtime.enqueue(search_fixture())
            calls = 0

            def discover(_batch):
                nonlocal calls
                calls += 1
                return {
                    "manifests": [], "search_complete": False,
                    "provider_telemetry": {
                        "torrent": {
                            "hit_cap": True,
                            "resource_failed_locators": [f"torrent:https://example/{calls}.torrent"],
                        },
                    },
                }

            self.assertEqual(runtime.run_once(discover, open_control)["status"], "retryable")
            task_path = next(queue.glob("*.json"))
            task = json.loads(task_path.read_text())
            task["next_attempt_at"] = datetime.now(timezone.utc).isoformat()
            task_path.write_text(json.dumps(task))
            self.assertEqual(runtime.run_once(discover, open_control)["status"], "retryable")
            persisted = json.loads(task_path.read_text())
            self.assertEqual(
                persisted["provider_telemetry"]["torrent"]["resource_failed_locators"],
                [
                    "torrent:https://example/1.torrent",
                    "torrent:https://example/2.torrent",
                ],
            )


if __name__ == "__main__":
    unittest.main()
