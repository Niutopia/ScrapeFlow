"""AList adapter safety tests for read-only disc-image probing."""

from __future__ import annotations

import contextlib
from unittest import mock

import pytest

from engine.scrapeflow import disc_image

_IMAGE_PATH = "/quark/影视/待刮削/series/S01-DISC1.iso"
_DISC_BYTES = 50 * 1024**3


class _FakeAList:
    def __init__(self, reader, *, versions=("v1", "v1")) -> None:
        self.reader = reader
        self.versions = list(versions)
        self.stat_calls: list[str] = []
        self.open_calls: list[tuple[str, int, bool]] = []

    def exact_file_info(self, path: str):
        self.stat_calls.append(path)
        version = self.versions.pop(0)
        return {"size": _DISC_BYTES, "version": version}

    @contextlib.contextmanager
    def open_file_range_reader(
        self,
        path: str,
        *,
        expected_size: int,
        refresh: bool = True,
    ):
        self.open_calls.append((path, expected_size, refresh))
        yield self.reader


def test_adapter_uses_one_reusable_strict_reader_and_stable_fingerprint() -> None:
    requested: list[tuple[int, int]] = []
    expected_ranges = (
        (0, 2048),
        (256 * 2048, 4096),
        (_DISC_BYTES - 2048, 2048),
    )

    def reader(offset: int, length: int) -> bytes:
        requested.append((offset, length))
        return bytes([offset % 251]) * length

    alist = _FakeAList(reader)
    sentinel = object()

    def fake_probe(
        read_range,
        *,
        image_path: str = "",
        prefer=None,
        image_size=None,
        limits=None,
    ):
        assert image_path == _IMAGE_PATH
        assert prefer is None
        assert image_size == _DISC_BYTES
        assert limits is None
        for offset, length in expected_ranges:
            assert read_range(offset, length) == bytes([offset % 251]) * length
        return sentinel

    with mock.patch.object(disc_image, "probe_disc_image", side_effect=fake_probe):
        result = disc_image.probe_disc_image_via_alist(alist, _IMAGE_PATH)

    assert result is sentinel
    assert requested == list(expected_ranges)
    assert alist.stat_calls == [_IMAGE_PATH, _IMAGE_PATH]
    assert alist.open_calls == [(_IMAGE_PATH, _DISC_BYTES, True)]
    assert sum(length for _, length in requested) < 1024 * 1024


def test_adapter_rejects_source_change_after_probe() -> None:
    alist = _FakeAList(lambda _offset, length: b"x" * length, versions=("v1", "v2"))

    with mock.patch.object(disc_image, "probe_disc_image", return_value=object()):
        with pytest.raises(disc_image.DiscImageError, match="探测期间发生变化"):
            disc_image.probe_disc_image_via_alist(alist, _IMAGE_PATH)

    assert alist.stat_calls == [_IMAGE_PATH, _IMAGE_PATH]


def test_adapter_requires_nonempty_stable_version_before_any_range() -> None:
    alist = _FakeAList(lambda _offset, length: b"x" * length, versions=(None, None))

    with mock.patch.object(disc_image, "probe_disc_image") as probe:
        with pytest.raises(disc_image.DiscImageError, match="稳定读取的镜像版本"):
            disc_image.probe_disc_image_via_alist(alist, _IMAGE_PATH)

    probe.assert_not_called()
    assert alist.open_calls == []


def test_adapter_propagates_disc_proof_failure_without_post_stat() -> None:
    alist = _FakeAList(lambda _offset, length: b"x" * length)

    with mock.patch.object(
        disc_image,
        "probe_disc_image",
        side_effect=disc_image.DiscImageError("budget exhausted"),
    ):
        with pytest.raises(disc_image.DiscImageError, match="budget exhausted"):
            disc_image.probe_disc_image_via_alist(alist, _IMAGE_PATH)

    assert alist.stat_calls == [_IMAGE_PATH]


def test_adapter_rejects_clients_without_safe_stat_or_range_interface() -> None:
    with pytest.raises(disc_image.DiscImageError, match="exact_file_info"):
        disc_image.probe_disc_image_via_alist(object(), _IMAGE_PATH)

    class StatOnly:
        @staticmethod
        def exact_file_info(_path: str):
            return {"size": _DISC_BYTES, "version": "v1"}

    with pytest.raises(disc_image.DiscImageError, match="严格 HTTP Range"):
        disc_image.probe_disc_image_via_alist(StatOnly(), _IMAGE_PATH)
