import unittest
from unittest.mock import patch

from engine.scrapeflow.subtitle_content_witness import (
    build_text_witness,
    resolve_ambiguous_by_embedded_witness,
)
from engine.tools.subtitle_executor import (
    _resolve_one_ambiguity, build_requests, build_selection, canonical_digest, prepare_selection,
    validate_candidate,
)


def ass_payload(rows):
    lines = ["[Script Info]", "ScriptType: v4.00+", "[Events]"]
    for index, (start, end, text) in enumerate(rows):
        lines.append(
            f"Dialogue: 0,{start},{end},Default,,0,0,0,,{text} 这是中文字幕内容"
        )
    return ("\n".join(lines) + "\n").encode()


def candidate_payload(prefix, *, short_timeline=False):
    rows = [("0:00:02.00", "0:00:04.00", f"{prefix}开场内容")]
    for offset in (300, 600):
        minute, second = divmod(offset, 60)
        for index in range(10):
            rows.append((
                f"0:{minute:02d}:{second + index * 2:02d}.00",
                f"0:{minute:02d}:{second + index * 2 + 1:02d}.00",
                f"{prefix}窗口{offset}唯一对白{index}",
            ))
    rows.append((
        "0:10:20.00" if short_timeline else "0:23:10.00",
        "0:10:21.00" if short_timeline else "0:23:11.00",
        f"{prefix}结束内容",
    ))
    return ass_payload(rows)


def sample_payload(prefix, offset):
    return ass_payload([
        (
            f"0:00:{index * 2:02d}.00", f"0:00:{index * 2 + 1:02d}.00",
            f"{prefix}窗口{offset}唯一对白{index}",
        )
        for index in range(10)
    ])


def resolve(candidates, *, samples=None, duration=1402.0):
    return resolve_ambiguous_by_embedded_witness(
        {"request_id": "request", "video_path": "/library/series/S01E02.mkv"},
        candidates,
        {"duration_seconds": duration},
        samples or [
            {
                "offset_seconds": offset, "duration_seconds": 30,
                "extension": ".ass", "payload": sample_payload("main", offset),
            }
            for offset in (300, 600)
        ],
    )


class SubtitleContentWitnessTests(unittest.TestCase):
    def test_build_witness_keeps_hashes_and_timeline_but_not_dialogue_text(self):
        witness = build_text_witness(candidate_payload("main"), ".ass")
        self.assertEqual(witness["status"], "ok")
        self.assertEqual(witness["language_evidence"]["status"], "chinese")
        self.assertEqual(witness["first_dialogue_seconds"], 2.0)
        self.assertEqual(witness["last_dialogue_seconds"], 1391.0)
        self.assertGreaterEqual(witness["unique_line_count"], 20)
        self.assertNotIn("main", str(witness))

    def test_two_independent_exact_windows_and_closed_timeline_select_unique_candidate(self):
        candidates = [
            {"candidate_id": "spin-off", "extension": ".ass", "payload": candidate_payload("other", short_timeline=True)},
            {"candidate_id": "main", "extension": ".ass", "payload": candidate_payload("main")},
        ]
        result = resolve(candidates)
        self.assertEqual(result["status"], "selected")
        self.assertEqual(result["selected_candidate_id"], "main")
        proofs = {row["candidate_id"]: row for row in result["candidate_proofs"]}
        self.assertTrue(proofs["main"]["timeline_closed"])
        self.assertEqual([row["coverage"] for row in proofs["main"]["windows"]], [1.0, 1.0])
        self.assertEqual([row["coverage"] for row in proofs["spin-off"]["windows"]], [0.0, 0.0])

    def test_candidate_order_and_paths_never_break_ties(self):
        candidates = [
            {"candidate_id": "z-backup", "path": "/z", "extension": ".ass", "payload": candidate_payload("main")},
            {"candidate_id": "a-formal", "path": "/formal", "extension": ".ass", "payload": candidate_payload("other")},
        ]
        first = resolve(candidates)
        second = resolve(list(reversed(candidates)))
        self.assertEqual(first["selected_candidate_id"], "z-backup")
        self.assertEqual(second["selected_candidate_id"], "z-backup")

    def test_one_window_or_nearby_windows_remain_unresolved(self):
        candidates = [
            {"candidate_id": name, "extension": ".ass", "payload": candidate_payload(name)}
            for name in ("one", "two")
        ]
        one = resolve(candidates, samples=[{
            "offset_seconds": 300, "duration_seconds": 30,
            "extension": ".ass", "payload": sample_payload("one", 300),
        }])
        self.assertEqual(one["reason"], "insufficient_sample_windows")
        nearby = resolve(candidates, samples=[
            {
                "offset_seconds": offset, "duration_seconds": 30,
                "extension": ".ass", "payload": sample_payload("one", offset),
            }
            for offset in (300, 360)
        ])
        self.assertEqual(nearby["reason"], "sample_windows_not_independent")

    def test_matching_content_with_open_timeline_remains_unresolved(self):
        candidates = [
            {"candidate_id": "short", "extension": ".ass", "payload": candidate_payload("main", short_timeline=True)},
            {"candidate_id": "other", "extension": ".ass", "payload": candidate_payload("other")},
        ]
        result = resolve(candidates)
        self.assertEqual(result["status"], "unresolved")
        self.assertEqual(result["reason"], "no_unique_content_timeline_winner")

    def test_two_matching_candidates_remain_ambiguous(self):
        payload = candidate_payload("main")
        result = resolve([
            {"candidate_id": "one", "extension": ".ass", "payload": payload},
            {"candidate_id": "two", "extension": ".ass", "payload": payload + b"\n"},
        ])
        self.assertEqual(result["status"], "unresolved")
        self.assertEqual(result["reason"], "multiple_content_timeline_winners")

    def test_low_information_sample_remains_unresolved(self):
        result = resolve(
            [
                {"candidate_id": "one", "extension": ".ass", "payload": candidate_payload("main")},
                {"candidate_id": "two", "extension": ".ass", "payload": candidate_payload("other")},
            ],
            samples=[
                {
                    "offset_seconds": offset, "duration_seconds": 30,
                    "extension": ".ass",
                    "payload": ass_payload([("0:00:01.00", "0:00:02.00", "这是唯一一行")]),
                }
                for offset in (300, 600)
            ],
        )
        self.assertEqual(result["reason"], "embedded_sample_has_insufficient_dialogue")

    def test_digest_bound_resolution_enters_selection_and_tampering_fails_closed(self):
        requests = build_requests({
            "confirmed_missing_chinese": [{
                "video_path": "/quark/影视/番剧/节目/节目 - S01E01.mkv",
                "title": "节目",
            }],
            "pending_review_or_probe": [],
        })
        request = requests["requests"][0]
        payloads = {"winner": candidate_payload("main"), "other": candidate_payload("other")}
        validated = [
            validate_candidate({
                "candidate_id": candidate_id,
                "path": f"/backup/{candidate_id}/节目 - S01E01.ass",
                "extension": ".ass", "source_kind": "alist",
            }, payload)
            for candidate_id, payload in payloads.items()
        ]
        resolution = resolve_ambiguous_by_embedded_witness(
            request,
            [
                {"candidate_id": candidate_id, "extension": ".ass", "payload": payload}
                for candidate_id, payload in payloads.items()
            ],
            {"duration_seconds": 1402, "stream_index": 2},
            [
                {
                    "offset_seconds": offset, "duration_seconds": 30,
                    "extension": ".ass", "payload": sample_payload("main", offset),
                }
                for offset in (300, 600)
            ],
        )
        selected = build_selection(
            requests, validated,
            ambiguity_resolutions={request["request_id"]: resolution},
        )
        self.assertEqual(selected["selections"][0]["candidate_id"], "winner")
        self.assertEqual(
            selected["selections"][0]["ambiguity_resolution"]["proof_sha256"],
            resolution["proof_sha256"],
        )
        core = {
            key: selected[key] for key in (
                "schema_version", "kind", "request_sha256", "selections",
                "acquisition_requests", "failures",
            )
        }
        self.assertEqual(selected["selection_sha256"], canonical_digest(core))

        tampered = dict(resolution, selected_candidate_id="other")
        blocked = build_selection(
            requests, validated,
            ambiguity_resolutions={request["request_id"]: tampered},
        )
        self.assertFalse(blocked["selections"])
        self.assertEqual(blocked["failures"][0]["status"], "ambiguous_verified_candidates")

        changed = list(validated)
        changed[0] = validate_candidate({
            "candidate_id": "winner", "path": "/backup/winner/节目 - S01E01.ass",
            "extension": ".ass", "source_kind": "alist",
        }, candidate_payload("changed"))
        stale = build_selection(
            requests, changed,
            ambiguity_resolutions={request["request_id"]: resolution},
        )
        self.assertFalse(stale["selections"])
        self.assertEqual(stale["failures"][0]["status"], "ambiguous_verified_candidates")

    def test_prepare_selection_runs_ambiguity_resolver_and_persists_proof(self):
        paths = {
            "/quark/影视/番剧/来源/节目 - S01E01.a.ass": candidate_payload("main"),
            "/quark/影视/番剧/来源/节目 - S01E01.b.ass": candidate_payload("other"),
        }

        class FakeClient:
            def try_list(self, *_args, **_kwargs):
                return []

            def walk(self, *_args, **_kwargs):
                return [{"full_path": path} for path in paths]

            def read_file_bytes(self, path, **_kwargs):
                return paths[path]

        resolver_called = []

        def resolver(_client, requests, selection, validated, payload_cache, **_kwargs):
            resolver_called.append(True)
            request = requests["requests"][0]
            ambiguous = selection["failures"][0]
            by_id = {row["candidate_id"]: row for row in validated}
            candidates = [{
                "candidate_id": candidate_id,
                "extension": by_id[candidate_id]["extension"],
                "payload": payload_cache[by_id[candidate_id]["path"]],
            } for candidate_id in ambiguous["candidate_ids"]]
            result = resolve_ambiguous_by_embedded_witness(
                request, candidates, {"duration_seconds": 1402, "stream_index": 2},
                [{
                    "offset_seconds": offset, "duration_seconds": 30,
                    "extension": ".ass", "payload": sample_payload("main", offset),
                } for offset in (300, 600)],
            )
            return {request["request_id"]: result}

        prepared = prepare_selection(
            FakeClient(),
            {"confirmed_missing_chinese": [{
                "video_path": "/quark/影视/番剧/节目/节目 - S01E01.mkv",
                "title": "节目",
            }], "pending_review_or_probe": []},
            roots=("/quark/影视/番剧",),
            ambiguity_resolver=resolver,
        )
        self.assertTrue(resolver_called)
        self.assertEqual(len(prepared["selection"]["selections"]), 1)
        self.assertIn(
            "proof_sha256",
            prepared["selection"]["selections"][0]["ambiguity_resolution"],
        )

    def test_io_adapter_uses_probe_and_two_windows_for_unique_winner(self):
        request = {"request_id": "request", "video_path": "/library/video.mkv"}
        candidates = [
            {"candidate_id": "winner", "extension": ".ass", "payload": candidate_payload("main")},
            {"candidate_id": "other", "extension": ".ass", "payload": candidate_payload("other", short_timeline=True)},
        ]

        class Client:
            def file_link(self, *_args, **_kwargs):
                return "https://example.invalid/video", {"Referer": "https://example.invalid/"}

        calls = []

        def run(command, **_kwargs):
            calls.append(command)
            if command[0] == "ffprobe":
                return type("Result", (), {
                    "returncode": 0,
                    "stdout": '{"format":{"duration":"1402"},"streams":[{"index":2,"codec_name":"ass"}]}',
                })()
            offset = int(float(command[command.index("-ss") + 1]))
            return type("Result", (), {
                "returncode": 0, "stdout": sample_payload("main", offset),
            })()

        with patch("engine.tools.subtitle_executor.shutil.which", return_value="/tool"), patch(
            "engine.tools.subtitle_executor.subprocess.run", side_effect=run,
        ):
            result = _resolve_one_ambiguity(Client(), request, candidates)
        self.assertEqual(result["selected_candidate_id"], "winner")
        self.assertRegex(result["proof_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(sum(command[0] == "ffmpeg" for command in calls), 2)

    def test_multiple_matching_streams_and_probe_failure_do_not_select(self):
        request = {"request_id": "request", "video_path": "/library/video.mkv"}
        candidates = [
            {"candidate_id": "winner", "extension": ".ass", "payload": candidate_payload("main")},
            {"candidate_id": "other", "extension": ".ass", "payload": candidate_payload("other")},
        ]

        class Client:
            def file_link(self, *_args, **_kwargs):
                return "https://example.invalid/video", {}

        def multiple(command, **_kwargs):
            if command[0] == "ffprobe":
                return type("Result", (), {
                    "returncode": 0,
                    "stdout": '{"format":{"duration":"1402"},"streams":[{"index":2,"codec_name":"ass"},{"index":3,"codec_name":"ass"}]}',
                })()
            offset = int(float(command[command.index("-ss") + 1]))
            return type("Result", (), {
                "returncode": 0, "stdout": sample_payload("main", offset),
            })()

        with patch("engine.tools.subtitle_executor.shutil.which", return_value="/tool"), patch(
            "engine.tools.subtitle_executor.subprocess.run", side_effect=multiple,
        ):
            result = _resolve_one_ambiguity(Client(), request, candidates)
        self.assertEqual(result["status"], "unresolved")
        self.assertEqual(result["reason"], "multiple_embedded_stream_winners")

        def failed_probe(command, **_kwargs):
            return type("Result", (), {"returncode": 1, "stdout": ""})()

        with patch("engine.tools.subtitle_executor.shutil.which", return_value="/tool"), patch(
            "engine.tools.subtitle_executor.subprocess.run", side_effect=failed_probe,
        ):
            with self.assertRaisesRegex(RuntimeError, "ffprobe_nonzero_exit"):
                _resolve_one_ambiguity(Client(), request, candidates)


if __name__ == "__main__":
    unittest.main()
