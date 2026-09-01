"""Pure B/W projection tests for verified optical-disc inventories.

These tests deliberately stop at ``analysis_rows``.  They do not compose
``root_boundaries``, create runtime jobs, acquire content, or invoke a writer.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from engine.scrapeflow.boundary_analysis import analyze_boundaries
from engine.scrapeflow.disc_image import DiscInventory, InnerFile
from engine.scrapeflow.source_inventory import (
    DiscAnalysisProjectionError,
    build_source_inventory,
    count_disc_image_files,
    count_video_files,
    project_disc_inventories_to_analysis_rows,
    project_disc_inventory_to_analysis_rows,
)
from engine.scrapeflow.source_objects import SourceManifest


BLOCK = 2048
ROOT = "/quark/影视/待刮削/Generic Disc Set"
SEASON = ROOT + "/Season 01"
IMAGE = SEASON + "/DISC1.iso"
IMAGE_2 = SEASON + "/DISC2.udf"
IMAGE_SIZE = 64 * BLOCK


def _physical_rows(*, include_second_image: bool = False) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = [
        {
            "name": "Season 01",
            "is_dir": True,
            "full_path": SEASON,
            "size": 0,
            "modified": "v1",
        },
        {
            "name": "DISC1.iso",
            "is_dir": False,
            "full_path": IMAGE,
            "size": IMAGE_SIZE,
            "modified": "v1",
        },
        {
            "name": "readme.txt",
            "is_dir": False,
            "full_path": SEASON + "/readme.txt",
            "size": 17,
            "modified": "v1",
        },
    ]
    if include_second_image:
        rows.append(
            {
                "name": "DISC2.udf",
                "is_dir": False,
                "full_path": IMAGE_2,
                "size": IMAGE_SIZE,
                "version": "v2",
            }
        )
    return rows


def _udf_inventory(
    *inner_files: InnerFile,
    image_path: str = IMAGE,
) -> DiscInventory:
    files = inner_files or (
        InnerFile(
            "/BDMV/STREAM/00001.m2ts",
            2 * BLOCK + 17,
            ((10, 3),),
        ),
        InnerFile(
            "/BDMV/PLAYLIST/00001.mpls",
            128,
            ((20, 1),),
        ),
    )
    return DiscInventory(
        image_path=image_path,
        kind="udf",
        inner_files=tuple(files),
        structure="bdmv",
    )


def _project(
    inventory: DiscInventory,
    *,
    rows: list[dict[str, object]] | None = None,
    size: int = IMAGE_SIZE,
    version: object = "v1",
) -> list[dict[str, object]]:
    return project_disc_inventories_to_analysis_rows(
        _physical_rows() if rows is None else rows,
        ROOT,
        (inventory,),
        backing_provenance={
            inventory.image_path: {
                "size": size,
                "version": version,
            }
        },
    )


def test_verified_udf_projects_to_analysis_rows_without_mutating_physical_rows() -> None:
    rows = _physical_rows()
    original = deepcopy(rows)

    analysis_rows = _project(_udf_inventory(), rows=rows)

    assert rows == original
    assert not any(
        row["full_path"] == IMAGE and row["is_dir"] is False
        for row in analysis_rows
    )
    image_root = next(row for row in analysis_rows if row["full_path"] == IMAGE)
    assert image_root["is_dir"] is True
    assert image_root["object_type"] == "directory"

    stream = next(
        row
        for row in analysis_rows
        if row["full_path"] == IMAGE + "/BDMV/STREAM/00001.m2ts"
    )
    assert stream["object_type"] == "video"
    assert stream["size"] == 2 * BLOCK + 17
    assert stream["disc_extents"] == [[10, 3]]

    virtual_rows = [row for row in analysis_rows if row.get("is_virtual") is True]
    assert virtual_rows
    for row in virtual_rows:
        assert row["content_expansion"] == "disc_inventory"
        assert row["backing_image_path"] == IMAGE
        assert row["backing_image_size"] == IMAGE_SIZE
        assert row["backing_image_version"] == "v1"
        assert row["disc_kind"] == "udf"
        assert row["disc_structure"] == "bdmv"

    node = build_source_inventory(analysis_rows, ROOT)
    assert count_video_files(node) == 1
    assert count_disc_image_files(node) == 0
    candidates = analyze_boundaries(node, root_task_id="projection-pure-bw")
    assert candidates
    assert all(not candidate.requires_content_expansion for candidate in candidates)


def test_physical_source_manifest_stays_on_original_iso_not_virtual_members() -> None:
    physical_rows = _physical_rows()
    analysis_rows = _project(_udf_inventory(), rows=physical_rows)

    manifest = SourceManifest.from_listing_rows(
        physical_rows,
        root_path=ROOT,
        snapshot_id="snapshot-physical-only",
    )

    assert manifest.object_at(IMAGE) is not None
    assert manifest.object_at(IMAGE).object_type == "disc_image"  # type: ignore[union-attr]
    assert manifest.object_at(IMAGE + "/BDMV/STREAM/00001.m2ts") is None
    assert any(
        row["full_path"] == IMAGE + "/BDMV/STREAM/00001.m2ts"
        for row in analysis_rows
    )


def test_projection_is_generic_for_multiple_udf_and_iso9660_images() -> None:
    first = _udf_inventory()
    second = DiscInventory(
        image_path=IMAGE_2,
        kind="iso9660",
        inner_files=(
            InnerFile(
                "/VIDEO_TS/VTS_01_1.VOB",
                BLOCK + 11,
                ((30, 2),),
            ),
        ),
        structure="video_ts",
    )

    analysis_rows = project_disc_inventories_to_analysis_rows(
        _physical_rows(include_second_image=True),
        ROOT,
        (first, second),
        backing_provenance={
            IMAGE: {"size": IMAGE_SIZE, "version": "v1"},
            IMAGE_2: {"size": IMAGE_SIZE, "version": "v2"},
        },
    )

    files = {
        row["full_path"]: row
        for row in analysis_rows
        if row.get("is_dir") is False
    }
    assert IMAGE + "/BDMV/STREAM/00001.m2ts" in files
    assert IMAGE_2 + "/VIDEO_TS/VTS_01_1.VOB" in files
    assert files[IMAGE_2 + "/VIDEO_TS/VTS_01_1.VOB"]["disc_kind"] == "iso9660"
    assert not any(
        key in row
        for row in analysis_rows
        for key in ("season_number", "episode_number", "gap", "acquisition")
    )


def test_unverified_disc_image_remains_opaque_in_analysis_rows() -> None:
    analysis_rows = project_disc_inventories_to_analysis_rows(
        _physical_rows(include_second_image=True),
        ROOT,
        (_udf_inventory(),),
        backing_provenance={
            IMAGE: {"size": IMAGE_SIZE, "version": "v1"},
        },
    )

    opaque = next(row for row in analysis_rows if row["full_path"] == IMAGE_2)
    assert opaque["is_dir"] is False
    assert count_disc_image_files(build_source_inventory(analysis_rows, ROOT)) == 1


def test_single_inventory_wrapper_carries_backing_provenance() -> None:
    analysis_rows = project_disc_inventory_to_analysis_rows(
        _physical_rows(),
        ROOT,
        _udf_inventory(),
        backing_size=IMAGE_SIZE,
        backing_version="v1",
    )

    stream = next(
        row for row in analysis_rows
        if row.get("disc_inner_path") == "/BDMV/STREAM/00001.m2ts"
    )
    assert stream["backing_image_path"] == IMAGE
    assert stream["backing_image_size"] == IMAGE_SIZE
    assert stream["backing_image_version"] == "v1"


@pytest.mark.parametrize(
    "inner_paths",
    [
        ("/BDMV/STREAM/00001.m2ts", "/BDMV/STREAM/00001.m2ts"),
        ("/BDMV/STREAM/Episode.m2ts", "/bdmv/stream/episode.M2TS"),
        ("/BDMV/STREAM/épisode.m2ts", "/BDMV/STREAM/e\u0301pisode.m2ts"),
    ],
)
def test_duplicate_or_provider_ambiguous_inner_paths_are_rejected(
    inner_paths: tuple[str, str],
) -> None:
    inventory = _udf_inventory(
        InnerFile(inner_paths[0], BLOCK, ((10, 1),)),
        InnerFile(inner_paths[1], BLOCK, ((11, 1),)),
    )

    with pytest.raises(DiscAnalysisProjectionError, match="重复|碰撞"):
        _project(inventory)


def test_inner_file_cannot_also_be_an_ancestor_directory() -> None:
    inventory = _udf_inventory(
        InnerFile("/FEATURE", BLOCK, ((10, 1),)),
        InnerFile("/FEATURE/episode.mkv", BLOCK, ((11, 1),)),
    )

    with pytest.raises(DiscAnalysisProjectionError, match="碰撞|重复"):
        _project(inventory)


def test_source_file_cannot_claim_descendant_rows() -> None:
    rows = _physical_rows()
    rows.append(
        {
            "name": "BDMV",
            "is_dir": True,
            "full_path": IMAGE + "/BDMV",
            "size": 0,
        }
    )

    with pytest.raises(DiscAnalysisProjectionError, match="文件.*后代路径"):
        _project(_udf_inventory(), rows=rows)


@pytest.mark.parametrize(
    "inner_file",
    [
        InnerFile("/too-far.mkv", BLOCK, ((63, 2),)),
        InnerFile("/too-short.mkv", 2 * BLOCK + 1, ((10, 2),)),
        InnerFile("/too-wide.mkv", 1, ((10, 2),)),
        InnerFile("/overlap.mkv", 3 * BLOCK, ((10, 2), (11, 2))),
        InnerFile("/duplicate.mkv", 2 * BLOCK, ((10, 1), (10, 1))),
    ],
)
def test_extent_out_of_bounds_overlap_or_bad_coverage_is_rejected(
    inner_file: InnerFile,
) -> None:
    with pytest.raises(DiscAnalysisProjectionError, match="extent"):
        _project(_udf_inventory(inner_file))


def test_path_traversal_is_rejected_before_virtual_rows_are_created() -> None:
    inventory = _udf_inventory(
        InnerFile("/BDMV/../escape.mkv", BLOCK, ((10, 1),)),
    )

    with pytest.raises(DiscAnalysisProjectionError, match="未规范化|无效路径段"):
        _project(inventory)


@pytest.mark.parametrize(
    ("size", "version", "match"),
    [
        (IMAGE_SIZE - 1, "v1", "size"),
        (IMAGE_SIZE, "stale", "version"),
        (IMAGE_SIZE, "", "version"),
    ],
)
def test_backing_size_and_version_must_match_physical_source(
    size: int,
    version: object,
    match: str,
) -> None:
    with pytest.raises(DiscAnalysisProjectionError, match=match):
        _project(_udf_inventory(), size=size, version=version)


def test_inventory_and_provenance_path_sets_must_match_exactly() -> None:
    with pytest.raises(DiscAnalysisProjectionError, match="路径集合不一致"):
        project_disc_inventories_to_analysis_rows(
            _physical_rows(include_second_image=True),
            ROOT,
            (_udf_inventory(),),
            backing_provenance={
                IMAGE: {"size": IMAGE_SIZE, "version": "v1"},
                IMAGE_2: {"size": IMAGE_SIZE, "version": "v2"},
            },
        )


def test_empty_inventory_cannot_hide_a_physical_disc_image() -> None:
    empty = DiscInventory(
        image_path=IMAGE,
        kind="udf",
        inner_files=(),
        structure="unknown",
    )

    with pytest.raises(DiscAnalysisProjectionError, match="为空"):
        _project(empty)
