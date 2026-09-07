"""Pure-memory safety regressions for bounded ISO/UDF inspection.

The fixtures in this module intentionally model only the descriptor fields used
by the reader, but they are internally consistent: every byte-range read is
served from a fixed-size in-memory image and every UDF descriptor has a valid
CRC/tag checksum.  No fixture touches the filesystem or network.
"""

from __future__ import annotations

from dataclasses import dataclass
import struct
from typing import Mapping, Sequence

import pytest

from engine.scrapeflow import disc_image

_SECTOR = disc_image.SECTOR_SIZE
_UDF_TAG_FILE_ENTRY = 261
_UDF_TAG_FILE_ID = 257
_UDF_FILE = 5
_UDF_DIRECTORY = 4
_UDF_FID_DELETED = 0x04


class _MemoryImage:
    """Fixed-size byte image that records every requested range."""

    def __init__(self, sectors: int = 800) -> None:
        self.data = bytearray(sectors * _SECTOR)
        self.requests: list[tuple[int, int]] = []

    @property
    def size(self) -> int:
        return len(self.data)

    def put(self, lba: int, payload: bytes | bytearray) -> None:
        start = lba * _SECTOR
        end = start + len(payload)
        assert lba >= 0 and end <= len(self.data)
        self.data[start:end] = payload

    def read_range(self, offset: int, length: int) -> bytes:
        self.requests.append((offset, length))
        if offset < 0 or length < 0 or offset + length > len(self.data):
            return b""
        return bytes(self.data[offset : offset + length])


def _both_endian_u16(buf: bytearray, offset: int, value: int) -> None:
    struct.pack_into("<H", buf, offset, value)
    struct.pack_into(">H", buf, offset + 2, value)


def _both_endian_u32(buf: bytearray, offset: int, value: int) -> None:
    struct.pack_into("<I", buf, offset, value)
    struct.pack_into(">I", buf, offset + 4, value)


def _iso_dir_record(
    name: str | bytes,
    *,
    extent: int,
    size: int,
    is_directory: bool,
) -> bytes:
    encoded = name if isinstance(name, bytes) else name.encode("ascii")
    # ISO 9660 directory records are even-sized.  A padding byte follows an
    # even-length file identifier because the fixed prefix is 33 bytes.
    record_length = 33 + len(encoded) + (1 if len(encoded) % 2 == 0 else 0)
    record = bytearray(record_length)
    record[0] = record_length
    _both_endian_u32(record, 2, extent)
    _both_endian_u32(record, 10, size)
    record[25] = 0x02 if is_directory else 0
    _both_endian_u16(record, 28, 1)
    record[32] = len(encoded)
    record[33 : 33 + len(encoded)] = encoded
    return bytes(record)


def _minimal_iso9660(
    *,
    file_names: Sequence[str] = ("VTS_01_1.VOB;1",),
) -> _MemoryImage:
    """Build a tiny ISO 9660 tree: /VIDEO_TS/<file_names>."""

    image = _MemoryImage(sectors=64)
    root_lba = 20
    video_ts_lba = 21

    video_records_without_dot = [
        _iso_dir_record(
            name,
            extent=30 + index,
            size=2 * _SECTOR + 17 + index,
            is_directory=False,
        )
        for index, name in enumerate(file_names)
    ]
    video_size = (
        len(_iso_dir_record(b"\x00", extent=video_ts_lba, size=0, is_directory=True))
        + len(_iso_dir_record(b"\x01", extent=root_lba, size=0, is_directory=True))
        + sum(len(record) for record in video_records_without_dot)
    )
    video_records = [
        _iso_dir_record(
            b"\x00", extent=video_ts_lba, size=video_size, is_directory=True
        ),
        _iso_dir_record(b"\x01", extent=root_lba, size=video_size, is_directory=True),
        *video_records_without_dot,
    ]
    video_directory = b"".join(video_records)

    root_child = _iso_dir_record(
        "VIDEO_TS", extent=video_ts_lba, size=len(video_directory), is_directory=True
    )
    root_size = (
        len(_iso_dir_record(b"\x00", extent=root_lba, size=0, is_directory=True))
        + len(_iso_dir_record(b"\x01", extent=root_lba, size=0, is_directory=True))
        + len(root_child)
    )
    root_directory = b"".join(
        (
            _iso_dir_record(b"\x00", extent=root_lba, size=root_size, is_directory=True),
            _iso_dir_record(b"\x01", extent=root_lba, size=root_size, is_directory=True),
            root_child,
        )
    )

    pvd = bytearray(_SECTOR)
    pvd[0] = 1
    pvd[1:6] = b"CD001"
    pvd[6] = 1
    _both_endian_u32(pvd, 80, len(image.data) // _SECTOR)
    _both_endian_u16(pvd, 128, _SECTOR)
    root_record = _iso_dir_record(
        b"\x00", extent=root_lba, size=len(root_directory), is_directory=True
    )
    pvd[156 : 156 + len(root_record)] = root_record
    image.put(16, pvd)

    terminator = bytearray(_SECTOR)
    terminator[0] = 255
    terminator[1:6] = b"CD001"
    terminator[6] = 1
    image.put(17, terminator)
    image.put(root_lba, root_directory)
    image.put(video_ts_lba, video_directory)
    return image


def _udf_crc16(payload: bytes | bytearray) -> int:
    crc = 0
    for byte in payload:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def _finish_udf_tag(
    descriptor: bytearray,
    *,
    tag_id: int,
    tag_location: int,
    descriptor_length: int | None = None,
) -> bytes:
    """Populate a valid ECMA-167 descriptor tag in ``descriptor``."""

    total = len(descriptor) if descriptor_length is None else descriptor_length
    assert 16 <= total <= len(descriptor)
    crc_length = total - 16
    struct.pack_into("<HH", descriptor, 0, tag_id, 3)
    descriptor[4] = 0
    descriptor[5] = 0
    struct.pack_into("<H", descriptor, 6, 1)
    struct.pack_into("<HHI", descriptor, 8, _udf_crc16(descriptor[16:total]), crc_length, tag_location)
    descriptor[4] = sum(descriptor[:4] + descriptor[5:16]) & 0xFF
    return bytes(descriptor)


def _pack_long_ad(
    buf: bytearray,
    offset: int,
    *,
    length: int,
    lbn: int,
    partition_ref: int,
) -> None:
    struct.pack_into("<IIH6x", buf, offset, length, lbn, partition_ref)


def _udf_file_entry(
    *,
    lbn: int,
    file_type: int,
    information_length: int,
    extents: Sequence[tuple[int, int]],
    allocation_kind: int = 0,
    immediate_payload: bytes = b"",
) -> bytes:
    """Create a UDF File Entry using short or long allocation descriptors."""

    sector = bytearray(_SECTOR)
    sector[27] = file_type
    struct.pack_into("<H", sector, 34, allocation_kind)
    struct.pack_into("<Q", sector, 56, information_length)
    struct.pack_into("<Q", sector, 64, sum((length + _SECTOR - 1) // _SECTOR for length, _ in extents))
    struct.pack_into("<I", sector, 168, 0)

    if allocation_kind == 0:
        ad_length = 8 * len(extents)
        for index, (length, position) in enumerate(extents):
            struct.pack_into("<II", sector, 176 + 8 * index, length, position)
    elif allocation_kind == 1:
        ad_length = 16 * len(extents)
        for index, (length, position) in enumerate(extents):
            _pack_long_ad(
                sector,
                176 + 16 * index,
                length=length,
                lbn=position,
                partition_ref=0,
            )
    else:
        ad_length = len(immediate_payload)
        sector[176 : 176 + ad_length] = immediate_payload
    struct.pack_into("<I", sector, 172, ad_length)
    return _finish_udf_tag(
        sector,
        tag_id=_UDF_TAG_FILE_ENTRY,
        tag_location=lbn,
        descriptor_length=176 + ad_length,
    )


def _udf_fid(
    name: str,
    *,
    child_lbn: int,
    directory_lbn: int,
    characteristics: int = 0,
) -> bytes:
    encoded_name = b"\x08" + name.encode("latin-1")
    record_length = (38 + len(encoded_name) + 3) // 4 * 4
    record = bytearray(record_length)
    record[18] = characteristics
    record[19] = len(encoded_name)
    _pack_long_ad(
        record,
        20,
        length=_SECTOR,
        lbn=child_lbn,
        partition_ref=0,
    )
    struct.pack_into("<H", record, 36, 0)
    record[38 : 38 + len(encoded_name)] = encoded_name
    return _finish_udf_tag(
        record,
        tag_id=_UDF_TAG_FILE_ID,
        tag_location=directory_lbn,
    )


@dataclass(frozen=True)
class _UdfFile:
    name: str
    size: int = 2 * _SECTOR + 17
    characteristics: int = 0
    payload: bytes | None = None


def _plain_udf(
    files: Sequence[_UdfFile],
    *,
    visible_file_count: int | None = None,
) -> _MemoryImage:
    """Build a plain-partition UDF image with root-level files.

    ``visible_file_count`` controls the root directory InformationLength while
    all records remain present in the allocated sector.  This makes padding
    over-read regressions explicit without requiring malformed Range data.
    """

    image = _MemoryImage()
    partition_start = 400
    vds_lba = 300
    fsd_lbn = 10
    root_lbn = 20
    directory_lbn = 50

    anchor = bytearray(_SECTOR)
    struct.pack_into("<II", anchor, 16, 3 * _SECTOR, vds_lba)
    image.put(
        256,
        _finish_udf_tag(
            anchor, tag_id=2, tag_location=256, descriptor_length=512
        ),
    )

    lvd = bytearray(_SECTOR)
    struct.pack_into("<I", lvd, 212, _SECTOR)
    _pack_long_ad(
        lvd,
        248,
        length=_SECTOR,
        lbn=fsd_lbn,
        partition_ref=0,
    )
    struct.pack_into("<II", lvd, 264, 6, 1)
    lvd[440:446] = b"\x01\x06\x01\x00\x00\x00"
    image.put(
        vds_lba,
        _finish_udf_tag(
            lvd,
            tag_id=6,
            tag_location=vds_lba,
            descriptor_length=446,
        ),
    )

    partition = bytearray(_SECTOR)
    struct.pack_into("<H", partition, 22, 0)
    struct.pack_into("<II", partition, 188, partition_start, 300)
    image.put(
        vds_lba + 1,
        _finish_udf_tag(
            partition,
            tag_id=5,
            tag_location=vds_lba + 1,
            descriptor_length=512,
        ),
    )

    terminator = bytearray(_SECTOR)
    image.put(
        vds_lba + 2,
        _finish_udf_tag(
            terminator,
            tag_id=8,
            tag_location=vds_lba + 2,
            descriptor_length=512,
        ),
    )

    fsd = bytearray(_SECTOR)
    _pack_long_ad(
        fsd,
        400,
        length=_SECTOR,
        lbn=root_lbn,
        partition_ref=0,
    )
    image.put(
        partition_start + fsd_lbn,
        _finish_udf_tag(
            fsd,
            tag_id=256,
            tag_location=fsd_lbn,
            descriptor_length=512,
        ),
    )

    directory_records: list[bytes] = []
    for index, file in enumerate(files):
        child_lbn = 30 + index
        data_lbn = 100 + 4 * index
        payload = file.payload
        size = len(payload) if payload is not None else file.size
        directory_records.append(
            _udf_fid(
                file.name,
                child_lbn=child_lbn,
                directory_lbn=directory_lbn,
                characteristics=file.characteristics,
            )
        )
        image.put(
            partition_start + child_lbn,
            _udf_file_entry(
                lbn=child_lbn,
                file_type=_UDF_FILE,
                information_length=size,
                extents=((size, data_lbn),),
            ),
        )
        if payload is not None:
            image.put(partition_start + data_lbn, payload)

    visible_count = len(files) if visible_file_count is None else visible_file_count
    assert 0 <= visible_count <= len(files)
    information_length = sum(len(item) for item in directory_records[:visible_count])
    allocated_directory = b"".join(directory_records)
    assert len(allocated_directory) <= _SECTOR
    image.put(partition_start + directory_lbn, allocated_directory)
    image.put(
        partition_start + root_lbn,
        _udf_file_entry(
            lbn=root_lbn,
            file_type=_UDF_DIRECTORY,
            information_length=information_length,
            extents=((information_length, directory_lbn),),
        ),
    )
    return image


def _reader_for_sector(lba: int, sector: bytes) -> tuple[_MemoryImage, object]:
    image = _MemoryImage(sectors=max(600, lba + 2))
    image.put(lba, sector)
    return image, disc_image._SectorReader(image.read_range)


def _plain_udf_parser(reader: object, *, partition_start: int = 100) -> object:
    parser = disc_image._UdfImage(reader)
    parser._partition_starts = {0: partition_start}
    parser._partition_ref_to_number = {0: 0}
    return parser


def _mpls_single_clip(*, clip_id: str, seconds: int) -> bytes:
    playlist_start = 32
    item_length = 20
    data = bytearray(playlist_start + 10 + 2 + item_length)
    data[:4] = b"MPLS"
    data[4:8] = b"0200"
    struct.pack_into(">I", data, 8, playlist_start)
    struct.pack_into(">I", data, playlist_start, len(data) - playlist_start - 4)
    struct.pack_into(">H", data, playlist_start + 6, 1)
    item_offset = playlist_start + 10
    struct.pack_into(">H", data, item_offset, item_length)
    data[item_offset + 2 : item_offset + 7] = clip_id.encode("ascii")
    data[item_offset + 7 : item_offset + 11] = b"M2TS"
    struct.pack_into(">II", data, item_offset + 14, 0, seconds * 45_000)
    return bytes(data)


def _probe_limits(**overrides: int) -> object:
    values = {
        "max_total_range_bytes": 4 * 1024 * 1024,
        "max_requests": 256,
        "max_single_range_bytes": 64 * _SECTOR,
        "max_directory_bytes": 256 * 1024,
        "max_directories": 128,
        "max_files": 4096,
        "max_depth": 32,
        "max_extents": 8192,
        "max_playlists": 512,
        "max_playlist_bytes": 4 * 1024 * 1024,
    }
    values.update(overrides)
    return disc_image.DiscProbeLimits(**values)


def _probe(
    image: _MemoryImage,
    *,
    prefer: str,
    limits: object | None = None,
    image_size: int | None = None,
) -> disc_image.DiscInventory:
    kwargs: dict[str, object] = {
        "image_path": f"memory.{prefer}",
        "prefer": prefer,
    }
    if limits is not None:
        kwargs["limits"] = limits
    if image_size is not None:
        kwargs["image_size"] = image_size
    return disc_image.probe_disc_image(image.read_range, **kwargs)


def test_iso9660_minimal_nested_directory_is_walked_from_memory() -> None:
    image = _minimal_iso9660()

    inventory = _probe(image, prefer="iso9660")

    assert inventory.kind == "iso9660"
    assert inventory.structure == "video_ts"
    assert inventory.inner_files == (
        disc_image.InnerFile(
            "/VIDEO_TS/VTS_01_1.VOB",
            2 * _SECTOR + 17,
            ((30, 3),),
        ),
    )
    assert inventory.videos == inventory.inner_files
    assert all(length <= _SECTOR for _, length in image.requests)


def test_udf_plain_partition_minimal_file_is_walked_from_memory() -> None:
    image = _plain_udf((_UdfFile("00001.M2TS"),))

    inventory = _probe(image, prefer="udf")

    assert inventory.kind == "udf"
    assert inventory.inner_files == (
        disc_image.InnerFile(
            "/00001.M2TS",
            2 * _SECTOR + 17,
            ((500, 3),),
        ),
    )


def test_long_ad_preserves_32_bit_lbn_and_partition_reference() -> None:
    descriptor = bytearray(16)
    struct.pack_into("<IIH6x", descriptor, 0, 0x10203040, 0xFEDCBA98, 0xBEEF)

    assert disc_image._parse_long_ad(descriptor, 0) == (
        0x10203040,
        0xFEDCBA98,
        0xBEEF,
    )


def test_udf_metadata_partition_resolves_across_multiple_metadata_runs() -> None:
    _, reader = _reader_for_sector(0, bytes(_SECTOR))
    parser = _plain_udf_parser(reader, partition_start=1000)
    parser._metadata_refs = {1}
    parser._metadata_runs = [(7000, 2), (9000, 3)]

    assert parser._absolute_lbn(0, 1) == 7000
    assert parser._absolute_lbn(1, 1) == 7001
    assert parser._absolute_lbn(2, 1) == 9000
    assert parser._absolute_lbn(4, 1) == 9002
    with pytest.raises(disc_image.DiscImageError):
        parser._absolute_lbn(5, 1)


def test_udf_metadata_partition_never_falls_open_to_physical_partition() -> None:
    _, reader = _reader_for_sector(0, bytes(_SECTOR))
    parser = _plain_udf_parser(reader, partition_start=1000)
    parser._partition_ref_to_number[1] = 0
    parser._metadata_refs = {1}
    parser._metadata_runs = []

    with pytest.raises(disc_image.DiscImageError):
        parser._absolute_lbn(7, 1)


def test_udf_directory_ignores_valid_looking_fid_in_allocation_padding() -> None:
    image = _plain_udf(
        (
            _UdfFile("VISIBLE.M2TS"),
            _UdfFile("PADDING-GHOST.M2TS"),
        ),
        visible_file_count=1,
    )

    inventory = _probe(image, prefer="udf")

    assert tuple(item.inner_path for item in inventory.inner_files) == (
        "/VISIBLE.M2TS",
    )


def test_udf_unsupported_allocation_descriptor_fails_closed() -> None:
    partition_start = 100
    lbn = 3
    entry = _udf_file_entry(
        lbn=lbn,
        file_type=_UDF_FILE,
        information_length=4,
        extents=(),
        allocation_kind=2,
        immediate_payload=b"data",
    )
    _, reader = _reader_for_sector(partition_start + lbn, entry)
    parser = _plain_udf_parser(reader, partition_start=partition_start)

    with pytest.raises(disc_image.DiscImageError):
        parser._file_entry(lbn, 0)


def test_udf_file_entry_rejects_extents_shorter_than_information_length() -> None:
    partition_start = 100
    lbn = 3
    entry = _udf_file_entry(
        lbn=lbn,
        file_type=_UDF_FILE,
        information_length=2 * _SECTOR,
        extents=((_SECTOR, 20),),
    )
    _, reader = _reader_for_sector(partition_start + lbn, entry)
    parser = _plain_udf_parser(reader, partition_start=partition_start)

    with pytest.raises(disc_image.DiscImageError):
        parser._file_entry(lbn, 0)


@pytest.mark.parametrize(
    "limit_override",
    (
        {"max_requests": 1},
        {"max_total_range_bytes": _SECTOR},
    ),
    ids=("request-count", "total-range-bytes"),
)
def test_probe_stops_before_exceeding_global_range_budget(
    limit_override: Mapping[str, int],
) -> None:
    image = _minimal_iso9660()
    limits = _probe_limits(**limit_override)

    with pytest.raises(disc_image.DiscImageError):
        _probe(
            image,
            prefer="iso9660",
            image_size=image.size,
            limits=limits,
        )

    assert image.requests == [(16 * _SECTOR, _SECTOR)]


def test_probe_rejects_descriptor_range_beyond_declared_image_size() -> None:
    image = _minimal_iso9660()
    declared_size = 17 * _SECTOR

    with pytest.raises(disc_image.DiscImageError):
        _probe(
            image,
            prefer="iso9660",
            image_size=declared_size,
            limits=_probe_limits(),
        )

    assert all(offset + length <= declared_size for offset, length in image.requests)


def test_udf_deleted_fid_is_not_returned_or_followed() -> None:
    image = _plain_udf(
        (
            _UdfFile("LIVE.M2TS"),
            _UdfFile("DELETED.M2TS", characteristics=_UDF_FID_DELETED),
        )
    )

    inventory = _probe(image, prefer="udf")

    assert tuple(item.inner_path for item in inventory.inner_files) == (
        "/LIVE.M2TS",
    )


@pytest.mark.parametrize("unsafe_name", ("..", "../escape.m2ts", "BDMV/STREAM/00001.m2ts"))
def test_udf_dangerous_file_identifier_fails_closed(unsafe_name: str) -> None:
    image = _plain_udf((_UdfFile(unsafe_name),))

    with pytest.raises(disc_image.DiscImageError):
        _probe(image, prefer="udf")


def test_mpls_exact_duplicate_primary_playlists_collapse_to_first_name() -> None:
    first = disc_image.parse_mpls(
        _mpls_single_clip(clip_id="00005", seconds=55 * 60),
        inner_path="/BDMV/PLAYLIST/00051.mpls",
    )
    duplicate = disc_image.parse_mpls(
        _mpls_single_clip(clip_id="00005", seconds=55 * 60),
        inner_path="/BDMV/PLAYLIST/01001.mpls",
    )
    inventory = disc_image.DiscInventory(
        image_path="memory.udf",
        kind="udf",
        inner_files=(
            disc_image.InnerFile(
                "/BDMV/STREAM/00005.m2ts",
                3000,
                ((100, 2),),
            ),
        ),
        structure="bdmv",
        playlists=(first, duplicate),
    )

    selected = inventory.episode_playlists()

    assert [item.inner_path for item in selected] == [
        "/BDMV/PLAYLIST/00051.mpls"
    ]


def test_mpls_differing_duplicate_primary_playlists_are_ambiguous() -> None:
    first = disc_image.parse_mpls(
        _mpls_single_clip(clip_id="00005", seconds=55 * 60),
        inner_path="/BDMV/PLAYLIST/00051.mpls",
    )
    differing = disc_image.parse_mpls(
        _mpls_single_clip(clip_id="00005", seconds=54 * 60),
        inner_path="/BDMV/PLAYLIST/01001.mpls",
    )
    inventory = disc_image.DiscInventory(
        image_path="memory.udf",
        kind="udf",
        inner_files=(
            disc_image.InnerFile(
                "/BDMV/STREAM/00005.m2ts",
                3000,
                ((100, 2),),
            ),
        ),
        structure="bdmv",
        playlists=(first, differing),
    )

    with pytest.raises(disc_image.DiscImageError):
        inventory.episode_playlists()


def test_mpls_backup_copy_does_not_create_duplicate_clip_ambiguity() -> None:
    playlist = disc_image.parse_mpls(
        _mpls_single_clip(clip_id="00005", seconds=55 * 60),
        inner_path="/BDMV/PLAYLIST/00051.mpls",
    )
    backup = disc_image.parse_mpls(
        _mpls_single_clip(clip_id="00005", seconds=55 * 60),
        inner_path="/BDMV/BACKUP/PLAYLIST/00051.mpls",
    )
    inventory = disc_image.DiscInventory(
        image_path="memory.udf",
        kind="udf",
        inner_files=(
            disc_image.InnerFile(
                "/BDMV/STREAM/00005.m2ts",
                3000,
                ((100, 2),),
            ),
        ),
        structure="bdmv",
        playlists=(playlist, backup),
    )

    assert inventory.episode_playlists() == (playlist,)
    assert round(playlist.duration_seconds) == 55 * 60


def test_inner_file_ranges_cover_exact_size_across_extents() -> None:
    inner = disc_image.InnerFile(
        "/BDMV/STREAM/00005.m2ts",
        5 * _SECTOR + 17,
        ((100, 3), (200, 3)),
    )

    ranges = tuple(disc_image.iter_inner_file_ranges(inner, chunk_bytes=2 * _SECTOR))

    assert ranges == (
        (100 * _SECTOR, 2 * _SECTOR),
        (102 * _SECTOR, _SECTOR),
        (200 * _SECTOR, 2 * _SECTOR),
        (202 * _SECTOR, 17),
    )
    assert sum(length for _, length in ranges) == inner.size


def test_inner_file_ranges_reject_incomplete_extent_coverage() -> None:
    inner = disc_image.InnerFile(
        "/BDMV/STREAM/00005.m2ts",
        3 * _SECTOR,
        ((100, 2),),
    )

    with pytest.raises(disc_image.DiscImageError):
        tuple(disc_image.iter_inner_file_ranges(inner))


def _poisoned_first_fid_image(mutate) -> _MemoryImage:
    """A valid plain UDF image whose first FID tag carries one poisoned field.

    The tag checksum is re-fixed after the mutation so the failure lands on
    the intended field check, not on the checksum itself — the adversarial
    gates ① (CRC length binding) and ② (TagLocation) have no negative
    coverage otherwise: every fixture builder only ever emits valid tags.
    """
    image = _plain_udf((_UdfFile("A.M2TS"),))
    fid_lba = 400 + 50  # partition_start + directory_lbn
    sector = bytearray(image.data[fid_lba * _SECTOR : (fid_lba + 1) * _SECTOR])
    mutate(sector)
    sector[4] = (sum(sector[0:4]) + sum(sector[5:16])) & 0xFF
    image.put(fid_lba, sector)
    return image


def test_udf_fid_rejects_zero_crc_length() -> None:
    def mutate(sector: bytearray) -> None:
        struct.pack_into("<H", sector, 10, 0)

    with pytest.raises(disc_image.DiscImageError, match="DescriptorCRCLength"):
        _probe(_poisoned_first_fid_image(mutate), prefer="udf")


def test_udf_fid_rejects_crc_length_that_would_bleed_into_the_next_fid() -> None:
    # The first FID record for "A.M2TS" is 48 bytes; a crc_length of 48
    # would cover 16 bytes past the descriptor — into the next record.
    def mutate(sector: bytearray) -> None:
        struct.pack_into("<H", sector, 10, 48)

    with pytest.raises(disc_image.DiscImageError, match="DescriptorCRCLength"):
        _probe(_poisoned_first_fid_image(mutate), prefer="udf")


def test_udf_fid_rejects_wrong_tag_location() -> None:
    def mutate(sector: bytearray) -> None:
        struct.pack_into("<I", sector, 12, 449)  # not the directory LBN (50)

    with pytest.raises(disc_image.DiscImageError, match="TagLocation"):
        _probe(_poisoned_first_fid_image(mutate), prefer="udf")


def test_udf_file_entry_rejects_extents_longer_than_information_length() -> None:
    partition_start = 100
    lbn = 3
    entry = _udf_file_entry(
        lbn=lbn,
        file_type=_UDF_FILE,
        information_length=_SECTOR,
        extents=((2 * _SECTOR, 20),),
    )
    _, reader = _reader_for_sector(partition_start + lbn, entry)
    parser = _plain_udf_parser(reader, partition_start=partition_start)

    with pytest.raises(disc_image.DiscImageError):
        parser._file_entry(lbn, 0)
