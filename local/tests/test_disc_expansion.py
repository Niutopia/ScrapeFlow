"""Focused regressions for the disc-expansion mapping proof.

Covers the order-preserving duration DP (including its bonus-playlist skip
semantics), the scope derivation that consumes it, and the operator ruling
lane that resolves scopes whose discs declare no readable episode order.
All fixtures are pure data — no filesystem, no network.
"""

from __future__ import annotations

import pytest

from engine.scrapeflow import disc_expansion as de


def _roster(minutes: list[int], *, season: int = 1) -> de.SeasonEpisodeRoster:
    return de.SeasonEpisodeRoster(
        season=season,
        episodes=tuple(
            (index + 1, value) for index, value in enumerate(minutes)
        ),
    )


def _candidate(
    *,
    duration_seconds: float,
    playlist: str = "/BDMV/PLAYLIST/00051.mpls",
    image: str = "memory://disc1.iso",
    disc_ordinal: int | None = 1,
    playlist_ordinal: int | None = None,
) -> de.PlaylistCandidate:
    if playlist_ordinal is None:
        playlist_ordinal = int(playlist.rsplit("/", 1)[-1].split(".")[0])
    return de.PlaylistCandidate(
        image_path=image,
        image_size=25_000_000_000,
        image_version="v1",
        playlist_inner_path=playlist,
        clip_inner_path="/BDMV/STREAM/00005.m2ts",
        clip_size=20_000_000_000,
        duration_seconds=duration_seconds,
        disc_ordinal=disc_ordinal,
        playlist_ordinal=playlist_ordinal,
    )


class TestOrderPreservingAssignment:
    def test_exact_bijection_still_proves(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [61 * 60, 59 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment == [1, 2]

    def test_bonus_playlist_above_threshold_is_skipped(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [59 * 60, 61 * 60, 20 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment == [1, 2, None]

    def test_leading_bonus_playlist_is_skipped(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [20 * 60, 59 * 60, 61 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment == [None, 1, 2]

    def test_two_skip_patterns_are_ambiguous(self) -> None:
        # Both (59, 61, skip) and (skip, 59, 61) cover the roster.
        assignment = de._unique_order_preserving_assignment(
            [59 * 60, 59 * 60, 61 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment is None

    def test_too_few_playlists_fail_closed(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [59 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment is None

    def test_unpublished_runtime_fails_closed(self) -> None:
        roster = de.SeasonEpisodeRoster(
            season=1, episodes=((1, 59), (2, None))
        )
        assignment = de._unique_order_preserving_assignment(
            [59 * 60, 61 * 60],
            roster.episodes,
            tolerance_seconds=120.0,
        )
        assert assignment is None

    def test_out_of_tolerance_playlist_fails_closed(self) -> None:
        assignment = de._unique_order_preserving_assignment(
            [59 * 60, 90 * 60],
            _roster([59, 61]).episodes,
            tolerance_seconds=120.0,
        )
        assert assignment is None


class TestDeriveScopeExpansion:
    def test_skipped_bonus_playlist_is_reported(self) -> None:
        plan = de.derive_scope_expansion(
            scope_path="/source/season-1",
            season=1,
            roster=_roster([59, 61]),
            candidates=[
                _candidate(
                    duration_seconds=59 * 60,
                    playlist="/BDMV/PLAYLIST/00051.mpls",
                ),
                _candidate(
                    duration_seconds=61 * 60,
                    playlist="/BDMV/PLAYLIST/00052.mpls",
                ),
                _candidate(
                    duration_seconds=20 * 60,
                    playlist="/BDMV/PLAYLIST/00204.mpls",
                ),
            ],
            staging_root="/staging/root",
            work_name="作品",
        )
        assert plan.proven
        assert plan.basis == "duration-dp"
        assert plan.skipped_playlists == ("/BDMV/PLAYLIST/00204.mpls",)
        assert [m.episode for m in plan.mappings] == [1, 2]
        assert plan.mappings[0].target_path == (
            "/staging/root/season-1/Season 01/作品 - S01E01.mkv"
        )


def _s03_style_candidates() -> list[de.PlaylistCandidate]:
    return [
        _candidate(
            duration_seconds=3_520,
            playlist="/BDMV/PLAYLIST/00051.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_317,
            playlist="/BDMV/PLAYLIST/00057.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_189,
            playlist="/BDMV/PLAYLIST/00058.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_086,
            playlist="/BDMV/PLAYLIST/00054.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_370,
            playlist="/BDMV/PLAYLIST/00055.mpls",
            image="memory://s03d1.iso",
        ),
        _candidate(
            duration_seconds=3_267,
            playlist="/BDMV/PLAYLIST/00056.mpls",
            image="memory://s03d1.iso",
        ),
    ]


def _s03_style_ruling() -> de.ScopeMappingRuling:
    return de.ScopeMappingRuling.from_mapping(
        {
            "scope_path": "/source/season-3",
            "season": 3,
            "assignments": [
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00051.mpls",
                    "episode": 1,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00057.mpls",
                    "episode": 2,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00058.mpls",
                    "episode": 3,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00054.mpls",
                    "episode": 4,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00055.mpls",
                    "episode": 5,
                },
                {
                    "image_path": "memory://s03d1.iso",
                    "playlist_inner_path": "/BDMV/PLAYLIST/00056.mpls",
                    "episode": 6,
                },
            ],
            "operator": "operator",
            "note": "fixture",
            "filed_at": "2026-09-02T00:00:00+08:00",
        }
    )


class TestScopeMappingRuling:
    def test_happy_path_applies_ruling(self) -> None:
        plan = de.apply_scope_mapping_ruling(
            scope_path="/source/season-3",
            season=3,
            roster=_roster([58, 55, 53, 51, 56, 54], season=3),
            candidates=_s03_style_candidates(),
            ruling=_s03_style_ruling(),
            staging_root="/staging/root",
            work_name="作品",
        )
        assert plan.proven
        assert plan.basis == "operator-ruling"
        assert [m.episode for m in plan.mappings] == [1, 2, 3, 4, 5, 6]
        assert plan.mappings[1].candidate.playlist_inner_path == (
            "/BDMV/PLAYLIST/00057.mpls"
        )
        assert plan.mappings[1].target_path == (
            "/staging/root/season-3/Season 03/作品 - S03E02.mkv"
        )

    def test_ruling_may_skip_bonus_candidates(self) -> None:
        # A DIY disc whose extra feature survived the duration selection has
        # more candidates than roster episodes — without an explicit skip
        # the ruling was structurally inapplicable in exactly the scenario
        # the rescue lane exists for.
        candidates = _s03_style_candidates() + [
            _candidate(
                duration_seconds=1_800,
                playlist="/BDMV/PLAYLIST/00999.mpls",
                image="memory://s03d1.iso",
            ),
        ]
        ruling = de.ScopeMappingRuling.from_mapping(
            {
                "scope_path": "/source/season-3",
                "season": 3,
                "assignments": _s03_style_ruling().as_dict()["assignments"],
                "skipped": [
                    {
                        "image_path": "memory://s03d1.iso",
                        "playlist_inner_path": "/BDMV/PLAYLIST/00999.mpls",
                    }
                ],
                "operator": "operator",
                "note": "00999 是制作花絮特典，不映射集号",
                "filed_at": "2026-09-07T00:00:00+08:00",
            }
        )
        plan = de.apply_scope_mapping_ruling(
            scope_path="/source/season-3",
            season=3,
            roster=_roster([58, 55, 53, 51, 56, 54], season=3),
            candidates=candidates,
            ruling=ruling,
            staging_root="/staging/root",
            work_name="作品",
        )
        assert plan.proven
        assert [m.episode for m in plan.mappings] == [1, 2, 3, 4, 5, 6]
        assert plan.skipped_playlists == ("/BDMV/PLAYLIST/00999.mpls",)

    def test_ruling_without_skip_must_still_cover_every_candidate(self) -> None:
        # Legacy shape (no skipped key): an extra candidate is still a set
        # mismatch, never a silent partial ruling.
        candidates = _s03_style_candidates() + [
            _candidate(
                duration_seconds=1_800,
                playlist="/BDMV/PLAYLIST/00999.mpls",
                image="memory://s03d1.iso",
            ),
        ]
        with pytest.raises(ValueError, match="集合与镜像候选不一致"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=candidates,
                ruling=_s03_style_ruling(),
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_assignment_and_skip_on_same_playlist_is_rejected(self) -> None:
        ruling = de.ScopeMappingRuling.from_mapping(
            {
                "scope_path": "/source/season-3",
                "season": 3,
                "assignments": _s03_style_ruling().as_dict()["assignments"],
                "skipped": [
                    {
                        "image_path": "memory://s03d1.iso",
                        "playlist_inner_path": "/BDMV/PLAYLIST/00051.mpls",
                    }
                ],
                "operator": "operator",
                "note": "自相矛盾的裁决",
                "filed_at": "2026-09-07T00:00:00+08:00",
            }
        )
        with pytest.raises(ValueError, match="既指定集号又跳过"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=_s03_style_candidates(),
                ruling=ruling,
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_wrong_scope_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="别的来源范围"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/other",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=_s03_style_candidates(),
                ruling=_s03_style_ruling(),
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_playlist_set_mismatch_is_rejected(self) -> None:
        candidates = _s03_style_candidates()[:-1]
        with pytest.raises(ValueError, match="集合与镜像候选不一致"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=candidates,
                ruling=_s03_style_ruling(),
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_episode_set_mismatch_is_rejected(self) -> None:
        ruling = _s03_style_ruling()
        object.__setattr__(
            ruling,
            "assignments",
            ruling.assignments[:-1]
            + (
                ("memory://s03d1.iso", "/BDMV/PLAYLIST/00056.mpls", 7),
            ),
        )
        with pytest.raises(ValueError, match="集号集合"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=_s03_style_candidates(),
                ruling=ruling,
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_duplicate_episode_is_rejected(self) -> None:
        ruling = _s03_style_ruling()
        object.__setattr__(
            ruling,
            "assignments",
            ruling.assignments[:-1]
            + (
                ("memory://s03d1.iso", "/BDMV/PLAYLIST/00056.mpls", 5),
            ),
        )
        with pytest.raises(ValueError, match="同一集"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=_s03_style_candidates(),
                ruling=ruling,
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_duration_contradiction_is_rejected(self) -> None:
        # Swapping E05/E06 keeps the playlist and episode sets intact but
        # puts the 3370s playlist on the 54-minute episode: +130s.
        ruling = _s03_style_ruling()
        object.__setattr__(
            ruling,
            "assignments",
            ruling.assignments[:4]
            + (
                ("memory://s03d1.iso", "/BDMV/PLAYLIST/00055.mpls", 6),
                ("memory://s03d1.iso", "/BDMV/PLAYLIST/00056.mpls", 5),
            ),
        )
        with pytest.raises(ValueError, match="与时长矛盾"):
            de.apply_scope_mapping_ruling(
                scope_path="/source/season-3",
                season=3,
                roster=_roster([58, 55, 53, 51, 56, 54], season=3),
                candidates=_s03_style_candidates(),
                ruling=ruling,
                staging_root="/staging/root",
                work_name="作品",
            )

    def test_from_mapping_requires_provenance(self) -> None:
        with pytest.raises(ValueError, match="operator"):
            de.ScopeMappingRuling.from_mapping(
                {
                    "scope_path": "/source/season-3",
                    "season": 3,
                    "assignments": [
                        {
                            "image_path": "memory://s03d1.iso",
                            "playlist_inner_path": "/BDMV/PLAYLIST/00051.mpls",
                            "episode": 1,
                        }
                    ],
                    "note": "n",
                    "filed_at": "t",
                }
            )


class TestRemuxEvidenceHashes:
    """The declared hashes must cover the produced file, not the input.

    The provider's commit callback verifies the declared md5/sha1 against
    the uploaded object.  A hash taken over the bytes fed to ffmpeg made
    every real remux upload fail deterministically with CallbackFailed.
    """

    HEADER = b"FAKE-MATROSKA-CONTAINER-BYTES"

    def _write_fake_tools(self, tmp_path) -> tuple[str, str]:
        ffmpeg = tmp_path / "fake-ffmpeg"
        ffmpeg.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "output = sys.argv[sys.argv.index('-y') + 1]\n"
            "with open(output, 'wb') as handle:\n"
            "    handle.write(b'FAKE-MATROSKA-CONTAINER-BYTES')\n"
            "    while True:\n"
            "        block = sys.stdin.buffer.read(65536)\n"
            "        if not block:\n"
            "            break\n"
            "        handle.write(block)\n",
            encoding="utf-8",
        )
        ffmpeg.chmod(0o755)
        ffprobe = tmp_path / "fake-ffprobe"
        ffprobe.write_text(
            "#!/usr/bin/env python3\n"
            "import json, sys\n"
            "del sys.argv\n"
            "print(json.dumps({\n"
            "    'streams': [\n"
            "        {'codec_type': 'video'},\n"
            "        {'codec_type': 'audio'},\n"
            "    ],\n"
            "    'format': {'duration': '61.0'},\n"
            "}))\n",
            encoding="utf-8",
        )
        ffprobe.chmod(0o755)
        return str(ffmpeg), str(ffprobe)

    def _write_fat_tail_tools(self, tmp_path) -> tuple[str, str]:
        """ffprobe reports a container overrun by a trailing subtitle track.

        The video stream matches the playlist while a DIY subtitle stream
        keeps its last timestamp ~7s past the video, stretching the
        container duration beyond the tolerance.  The remux must pass: the
        episode runtime is the video stream's duration.
        """
        ffmpeg = tmp_path / "fat-tail-ffmpeg"
        ffmpeg.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "output = sys.argv[sys.argv.index('-y') + 1]\n"
            "import os\n"
            "with open(output, 'wb') as handle:\n"
            "    handle.write(b'FAKE-MATROSKA-CONTAINER-BYTES')\n"
            "    while True:\n"
            "        block = sys.stdin.buffer.read(65536)\n"
            "        if not block:\n"
            "            break\n"
            "        handle.write(block)\n",
            encoding="utf-8",
        )
        ffmpeg.chmod(0o755)
        ffprobe = tmp_path / "fat-tail-ffprobe"
        ffprobe.write_text(
            "#!/usr/bin/env python3\n"
            "import json, sys\n"
            "del sys.argv\n"
            "print(json.dumps({\n"
            "    'streams': [\n"
            "        {'index': 0, 'codec_type': 'video',\n"
            "         'duration': '3465.128'},\n"
            "        {'index': 1, 'codec_type': 'audio'},\n"
            "        {'index': 7, 'codec_type': 'subtitle',\n"
            "         'duration': '3472.512'},\n"
            "    ],\n"
            "    'format': {'duration': '3472.512'},\n"
            "}))\n",
            encoding="utf-8",
        )
        ffprobe.chmod(0o755)
        return str(ffmpeg), str(ffprobe)

    def test_duration_gate_uses_video_stream_not_container(self, tmp_path) -> None:
        import hashlib

        from engine.scrapeflow.disc_image import InnerFile

        ffmpeg, _ffprobe = self._write_fat_tail_tools(tmp_path)
        payload = bytes(range(256)) * 8192  # 2 MiB of structured input
        image = b"\x00" * 4096 + payload
        inner = InnerFile(
            inner_path="/BDMV/STREAM/00016.M2TS",
            size=len(payload),
            extents=((2, len(payload) // 2048),),
        )

        def read_range(offset: int, length: int) -> bytes:
            return image[offset:offset + length]

        output = tmp_path / "buffer.mkv"
        evidence = de.remux_inner_file_to_matroska(
            read_range,
            inner,
            image_size=len(image),
            output_path=str(output),
            chunk_bytes=256 * 1024,
            ffmpeg_argv=(ffmpeg,),
            expected_duration_seconds=3465.170,
        )
        # The video stream (3465.128s) is the reported runtime, not the
        # subtitle-extended container (3472.512s).
        assert evidence.duration_seconds == pytest.approx(3465.128)

    def test_evidence_hashes_cover_the_produced_file(self, tmp_path) -> None:
        import hashlib
        import os

        from engine.scrapeflow.disc_image import InnerFile

        ffmpeg, _ffprobe = self._write_fake_tools(tmp_path)
        payload = bytes(range(256)) * 8192  # 2 MiB of structured input
        image = b"\x00" * 4096 + payload
        inner = InnerFile(
            inner_path="/BDMV/STREAM/00001.M2TS",
            size=len(payload),
            extents=((2, len(payload) // 2048),),
        )

        def read_range(offset: int, length: int) -> bytes:
            return image[offset:offset + length]

        output = tmp_path / "buffer.mkv"
        evidence = de.remux_inner_file_to_matroska(
            read_range,
            inner,
            image_size=len(image),
            output_path=str(output),
            chunk_bytes=256 * 1024,
            ffmpeg_argv=(ffmpeg,),
            expected_duration_seconds=61.0,
        )

        produced = output.read_bytes()
        assert produced == self.HEADER + payload
        assert evidence.output_bytes == len(produced)
        assert evidence.md5 == hashlib.md5(produced).hexdigest()
        assert evidence.sha1 == hashlib.sha1(produced).hexdigest()
        # The input-stream hash is a different digest: this is exactly the
        # mismatch that made the provider reject the commit.
        assert evidence.md5 != hashlib.md5(payload).hexdigest()


class TestTransferResilience:
    """The executor survives provider-side visibility and upload blips.

    Two observed failure classes on quark_uc, both generic: the exact-path
    stat stays blind past the whole readback window while the refreshed
    listing already shows the committed object, and a part-level transport
    blip is retried by the provider proxy with an already-drained reader so
    the part arrives empty and the upload is rejected with nothing
    committed.
    """

    PAYLOAD = b"FAKE-MATROSKA-BYTES" * 4

    class FakeAList:
        def __init__(self, entries=None):
            self.entries = entries or []
            self.list_calls = 0

        def list(self, path, refresh=False):
            self.list_calls += 1
            return list(self.entries)

    def _executor(
        self,
        tmp_path,
        *,
        statter,
        alist,
        uploader,
    ):
        state_dir = tmp_path / "states"
        buffer_dir = tmp_path / "buffers"
        state_dir.mkdir()
        buffer_dir.mkdir()
        remux_calls = []

        def fake_remux(read_range, inner_file, **kwargs):
            remux_calls.append(1)
            buffer_path = kwargs["output_path"]
            with open(buffer_path, "wb") as handle:
                handle.write(self.PAYLOAD)
            return de.RemuxEvidence(
                output_path=buffer_path,
                output_bytes=len(self.PAYLOAD),
                duration_seconds=61.0,
                video_streams=1,
                audio_streams=1,
                subtitle_streams=0,
                md5="a" * 32,
                sha1="b" * 40,
            )

        import contextlib

        executor = de.DiscExpansionExecutor(
            alist,
            state_dir=str(state_dir),
            local_buffer_dir=str(buffer_dir),
            chunk_bytes=8,
            min_free_buffer_bytes=0,
            readback_attempts=1,
            readback_interval_seconds=0.0,
            sleep=lambda _seconds: None,
            reader_opener=lambda _alist, **_kwargs: contextlib.nullcontext(
                lambda offset, length: self.PAYLOAD[offset:offset + length]
            ),
            statter=statter,
            uploader=uploader,
            remux=fake_remux,
        )
        return executor, remux_calls

    def _mapping(self):
        return de.EpisodeMapping(
            candidate=_candidate(duration_seconds=61 * 60),
            season=1,
            episode=1,
            target_path="/staging/Show/Season 01/Show - S01E01.mkv",
        )

    def test_readback_accepts_the_refreshed_listing(self, tmp_path) -> None:
        # The exact-path stat never sees the object; the refreshed listing
        # does.  The readback must confirm through the second opinion and
        # the mapping completes instead of failing the window.
        alist = self.FakeAList([
            {"name": "Show - S01E01.mkv", "size": len(self.PAYLOAD),
             "is_dir": False},
        ])

        def uploader(target_path, chunks, **kwargs):
            for _ in chunks:
                pass

        executor, _remux_calls = self._executor(
            tmp_path,
            statter=lambda _path: None,
            alist=alist,
            uploader=uploader,
        )
        state = executor.execute_mapping(
            self._mapping(), inner_file=object()
        )
        assert state.status == "completed"
        assert state.output_bytes == len(self.PAYLOAD)
        assert alist.list_calls >= 1

    def test_upload_blip_is_retried_from_the_intact_buffer(
        self, tmp_path
    ) -> None:
        # First upload is rejected by the provider proxy (nothing
        # committed); the buffer is still intact, so the retry streams the
        # identical bytes and the mapping completes.
        attempts = []
        committed = []

        def uploader(target_path, chunks, **kwargs):
            payload = b"".join(chunks)
            attempts.append(payload)
            if len(attempts) == 1:
                raise RuntimeError("up status: 400 EntityTooSmall")
            committed.append(1)

        def statter(path):
            if committed:
                return {"size": len(self.PAYLOAD)}
            return None

        class CommitAwareAList(self.FakeAList):
            def list(self, path, refresh=False):
                self.list_calls += 1
                if committed:
                    return [
                        {"name": "Show - S01E01.mkv",
                         "size": len(self.PAYLOAD), "is_dir": False},
                    ]
                return []

        alist = CommitAwareAList()

        executor, _remux_calls = self._executor(
            tmp_path, statter=statter, alist=alist, uploader=uploader
        )
        state = executor.execute_mapping(
            self._mapping(), inner_file=object()
        )
        assert state.status == "completed"
        assert len(attempts) == 2
        assert attempts[0] == self.PAYLOAD
        assert attempts[1] == self.PAYLOAD

    def test_committed_despite_error_is_not_reuploaded(self, tmp_path) -> None:
        # The transport broke after the provider committed: the reconcile
        # finds the target at the declared size, so no second upload is
        # attempted and the mapping still completes.  The listing only
        # reveals the object after the upload attempt (the precheck must
        # not adopt it before this run has even tried).
        calls = []
        committed = []
        payload = self.PAYLOAD

        def uploader(target_path, chunks, **kwargs):
            calls.append(1)
            for _ in chunks:
                pass
            committed.append(1)
            raise OSError("connection reset after commit")

        class LaggingAList(self.FakeAList):
            def list(self, path, refresh=False):
                self.list_calls += 1
                if committed:
                    return [
                        {"name": "Show - S01E01.mkv",
                         "size": len(payload), "is_dir": False},
                    ]
                return []

        executor, _remux_calls = self._executor(
            tmp_path,
            statter=lambda _path: None,
            alist=LaggingAList(),
            uploader=uploader,
        )
        state = executor.execute_mapping(
            self._mapping(), inner_file=object()
        )
        assert state.status == "completed"
        assert calls == [1]

    def test_target_from_crashed_attempt_is_adopted_by_the_size_gate(
        self, tmp_path
    ) -> None:
        # The E03 trap: a previous attempt committed the upload and died
        # before any state was saved.  The precheck finds the target, the
        # deterministic remux recomputes the evidence, the size gate proves
        # the object is ours, and the mapping completes without re-uploading.
        uploads = []

        def uploader(target_path, chunks, **kwargs):
            uploads.append(1)
            for _ in chunks:
                pass

        alist = self.FakeAList([
            {"name": "Show - S01E01.mkv", "size": len(self.PAYLOAD),
             "is_dir": False},
        ])
        executor, remux_calls = self._executor(
            tmp_path,
            statter=lambda _path: None,
            alist=alist,
            uploader=uploader,
        )
        state = executor.execute_mapping(
            self._mapping(), inner_file=object()
        )
        assert state.status == "completed"
        assert state.output_bytes == len(self.PAYLOAD)
        assert remux_calls == [1]
        assert uploads == []

    def test_foreign_target_size_is_a_hard_refusal(self, tmp_path) -> None:
        # A pre-existing object whose size disagrees with the deterministic
        # remux evidence is not ours: never overwrite, park with the sizes.
        uploads = []

        def uploader(target_path, chunks, **kwargs):
            uploads.append(1)
            for _ in chunks:
                pass

        alist = self.FakeAList([
            {"name": "Show - S01E01.mkv", "size": len(self.PAYLOAD) + 1,
             "is_dir": False},
        ])
        executor, _remux_calls = self._executor(
            tmp_path,
            statter=lambda _path: None,
            alist=alist,
            uploader=uploader,
        )
        with pytest.raises(de.DiscExpansionError, match="拒绝覆盖"):
            executor.execute_mapping(self._mapping(), inner_file=object())
        assert uploads == []

    def _saved_state(self, executor, mapping, *, status):
        state = de.ExpansionTransferState.from_mapping(mapping)
        state.status = status
        state.inner_size = 20_000_000_000
        state.duration_seconds = 61.0
        state.output_bytes = len(self.PAYLOAD)
        state.md5 = "a" * 32
        state.sha1 = "b" * 40
        state.updated_at = "2026-09-03T00:00:00Z"
        executor._save_state(state)
        return state

    def test_uploaded_state_completes_via_readback_without_rework(
        self, tmp_path
    ) -> None:
        # Crash between the provider commit and the readback: the saved
        # "uploaded" state plus the size-gated readback completes the
        # mapping with neither a remux nor an upload.
        uploads = []
        alist = self.FakeAList([
            {"name": "Show - S01E01.mkv", "size": len(self.PAYLOAD),
             "is_dir": False},
        ])
        executor, remux_calls = self._executor(
            tmp_path,
            statter=lambda _path: None,
            alist=alist,
            uploader=lambda target_path, chunks, **kwargs: (
                uploads.append(1),
            ),
        )
        mapping = self._mapping()
        self._saved_state(executor, mapping, status="uploaded")
        state = executor.execute_mapping(mapping, inner_file=object())
        assert state.status == "completed"
        assert remux_calls == []
        assert uploads == []

    def test_uploading_state_adopts_committed_target_without_remux(
        self, tmp_path
    ) -> None:
        # Crash mid-upload after the provider had already committed: the
        # pre-upload "uploading" state carries the evidence, so the
        # precheck's size gate adopts the target without any rework.
        uploads = []
        alist = self.FakeAList([
            {"name": "Show - S01E01.mkv", "size": len(self.PAYLOAD),
             "is_dir": False},
        ])
        executor, remux_calls = self._executor(
            tmp_path,
            statter=lambda _path: None,
            alist=alist,
            uploader=lambda target_path, chunks, **kwargs: (
                uploads.append(1),
            ),
        )
        mapping = self._mapping()
        self._saved_state(executor, mapping, status="uploading")
        state = executor.execute_mapping(mapping, inner_file=object())
        assert state.status == "completed"
        assert remux_calls == []
        assert uploads == []

    def test_repeated_upload_failure_raises_the_last_error(
        self, tmp_path
    ) -> None:
        def uploader(target_path, chunks, **kwargs):
            for _ in chunks:
                pass
            raise RuntimeError("up status: 400 EntityTooSmall")

        executor, _remux_calls = self._executor(
            tmp_path,
            statter=lambda _path: None,
            alist=self.FakeAList([]),
            uploader=uploader,
        )
        with pytest.raises(RuntimeError, match="EntityTooSmall"):
            executor.execute_mapping(self._mapping(), inner_file=object())
