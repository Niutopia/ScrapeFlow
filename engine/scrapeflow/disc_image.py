"""Read-only optical-disc image inspection via bounded range requests.

A disc image in the intake is opaque to B/W: the engine refuses to plan,
move or archive it until its content is proven. This module is that
read-only proof. It parses the image's own filesystem — ISO 9660 for DVD
layouts and UDF for Blu-ray layouts — through a sector-reader callable so
no code path ever downloads the whole multi-gigabyte image: only the
descriptor sectors and directory extents are fetched, and nothing is
written anywhere.

The result is a :class:`DiscInventory`: the inner video files with their
logical sizes and structure hints (BDMV/STREAM, VIDEO_TS), plus enough
playlist context for the caller to map episode coordinates.
"""

from __future__ import annotations

import binascii
import contextlib
import hashlib
import posixpath
import struct
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Callable, Mapping, Sequence

SECTOR_SIZE = 2048

# Descriptor tag identifiers (ECMA-167 / UDF).
_TAG_ANCHOR = 2
_TAG_PARTITION = 5
_TAG_LOGICAL_VOLUME = 6
_TAG_FILE_SET = 256
_TAG_FILE_ID = 257
_TAG_ALLOCATION_EXTENT = 258
_TAG_FILE_ENTRY = 261
_TAG_EXTENDED_FILE_ENTRY = 266

_ICB_FLAG_AD_MASK = 0x0007
_ICB_AD_SHORT = 0
_ICB_AD_LONG = 1
_ICB_AD_EXTENDED = 2
_ICB_AD_IMMEDIATE = 3

_FID_PARENT_FLAG = 0x08
_FID_DELETED_FLAG = 0x04

# UDF file types of interest (ICB tag fileType byte).
_FILE_TYPE_DIRECTORY = 4
_FILE_TYPE_FILE = 5
_FILE_TYPE_SYMLINK = 12

_VIDEO_SUFFIXES = (".m2ts", ".mts", ".vob", ".mpg", ".mpeg", ".m2v")


@dataclass(frozen=True)
class DiscProbeLimits:
    """Hard ceilings for one read-only image proof.

    The defaults are intentionally far above normal Blu-ray metadata usage
    (usually below a few MiB) while remaining tiny compared with a 40–100 GiB
    image.  A malformed image must fail closed instead of turning many small
    Range reads into an accidental whole-image scan.
    """

    max_total_range_bytes: int = 64 * 1024 * 1024
    max_requests: int = 8192
    max_single_range_bytes: int = 8 * 1024 * 1024
    max_directory_bytes: int = 16 * 1024 * 1024
    max_directories: int = 4096
    max_files: int = 100_000
    max_depth: int = 64
    max_extents: int = 16_384
    max_playlists: int = 4096
    max_playlist_bytes: int = 32 * 1024 * 1024

    def __post_init__(self) -> None:
        for name, value in self.__dict__.items():
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} 必须是正整数")


@dataclass(frozen=True)
class _ResolvedExtent:
    """One logical UDF extent segment mapped to an absolute image run."""

    logical_lbn: int
    partition_ref: int
    physical_lba: int
    byte_length: int

    @property
    def block_count(self) -> int:
        return (self.byte_length + SECTOR_SIZE - 1) // SECTOR_SIZE


@dataclass(frozen=True)
class InnerFile:
    """One concrete file inside the image, positioned by extent runs."""

    inner_path: str
    size: int
    extents: tuple[tuple[int, int], ...]  # (logical_block, block_count)

    @property
    def is_video(self) -> bool:
        return self.inner_path.lower().endswith(_VIDEO_SUFFIXES)


@dataclass(frozen=True)
class InnerFileDigest:
    """Content proof calculated without materialising the inner file."""

    size: int
    md5: str
    sha1: str


@dataclass(frozen=True)
class InnerFileTransferResult:
    """Remote-to-remote, zero-local-disk transfer acceptance evidence."""

    image_path: str
    inner_path: str
    target_path: str
    size: int
    md5: str
    sha1: str
    source_version: object
    target_version: object


@dataclass(frozen=True)
class DiscPlayItem:
    """One MPLS play item addressing a clip and an exact 45 kHz time span."""

    clip_id: str
    codec_id: str
    in_time: int
    out_time: int

    @property
    def duration_seconds(self) -> float:
        return max(0, self.out_time - self.in_time) / 45_000.0


@dataclass(frozen=True)
class DiscPlaylist:
    """One parsed Blu-ray playlist retained as read-only mapping evidence."""

    inner_path: str
    play_items: tuple[DiscPlayItem, ...]

    @property
    def duration_seconds(self) -> float:
        return sum(item.duration_seconds for item in self.play_items)


@dataclass(frozen=True)
class DiscInventory:
    """The read-only expansion result for one disc image."""

    image_path: str
    kind: str  # "udf" | "iso9660"
    inner_files: tuple[InnerFile, ...]
    structure: str  # "bdmv" | "video_ts" | "flat" | "unknown"
    playlists: tuple[DiscPlaylist, ...] = field(default_factory=tuple)

    @property
    def videos(self) -> tuple[InnerFile, ...]:
        return tuple(f for f in self.inner_files if f.is_video)

    def as_rows(self) -> list[dict[str, object]]:
        return [
            {
                "name": posixpath.basename(f.inner_path),
                "inner_path": f.inner_path,
                "size": f.size,
                "extents": [[lb, count] for lb, count in f.extents],
            }
            for f in self.inner_files
        ]

    def episode_playlists(
        self,
        *,
        minimum_duration_seconds: float = 18 * 60,
    ) -> tuple[DiscPlaylist, ...]:
        """Return unambiguous single-clip primary playlists in disc order.

        Backup copies are excluded.  A DIY-authored disc may publish exact
        duplicate playlists for the same clip — identical in/out spans, only
        differing in stream-selection tables a clip-level remux cannot see —
        and those collapse deterministically to the first playlist name.
        Any other repeat of a clip, or a branching playlist, remains
        fail-closed for the higher-level disc-to-episode mapper.
        """
        stream_ids = {
            posixpath.splitext(posixpath.basename(item.inner_path))[0]
            for item in self.inner_files
            if "/bdmv/stream/" in item.inner_path.casefold()
        }
        selected: list[DiscPlaylist] = []
        clips: dict[str, DiscPlayItem] = {}
        for playlist in sorted(
            self.playlists,
            key=lambda item: posixpath.basename(item.inner_path).casefold(),
        ):
            folded = playlist.inner_path.casefold()
            if "/bdmv/playlist/" not in folded or "/backup/" in folded:
                continue
            if len(playlist.play_items) != 1:
                continue
            play_item = playlist.play_items[0]
            if (
                play_item.duration_seconds < minimum_duration_seconds
                or play_item.clip_id not in stream_ids
            ):
                continue
            seen = clips.get(play_item.clip_id)
            if seen is not None:
                if (
                    seen.in_time != play_item.in_time
                    or seen.out_time != play_item.out_time
                    or seen.codec_id != play_item.codec_id
                ):
                    raise DiscImageError(
                        "Blu-ray 正片 playlist 对同一 clip 存在歧义"
                    )
                # An exact-duplicate playlist of an already-selected clip
                # describes the same remux; keep the first name only.
                continue
            clips[play_item.clip_id] = play_item
            selected.append(playlist)
        return tuple(selected)


class DiscImageError(Exception):
    """The image could not be proven with bounded read-only requests."""


class _BudgetExceeded(DiscImageError):
    """A hard proof budget was exhausted; do not try a fallback parser."""


class _NotThisFormat(DiscImageError):
    """The image does not contain this filesystem signature."""


class _RangeBudget:
    """Validate and account every physical image byte request."""

    def __init__(
        self,
        read_range: Callable[[int, int], bytes],
        *,
        image_size: int | None,
        limits: DiscProbeLimits,
    ) -> None:
        if image_size is not None and image_size <= 0:
            raise ValueError("image_size 必须大于 0")
        self._read_range = read_range
        self.image_size = image_size
        self.limits = limits
        self.requests = 0
        self.total_bytes = 0

    def read(self, offset: int, length: int) -> bytes:
        if offset < 0 or length <= 0:
            raise DiscImageError("镜像 Range 参数非法")
        end = offset + length
        if end <= offset:
            raise DiscImageError("镜像 Range 整数范围异常")
        if self.image_size is not None and end > self.image_size:
            raise DiscImageError(
                f"镜像 Range 越过文件末尾: offset={offset}, length={length}, "
                f"image_size={self.image_size}"
            )
        if length > self.limits.max_single_range_bytes:
            raise _BudgetExceeded(
                f"镜像单次 Range 超过上限: {length} > "
                f"{self.limits.max_single_range_bytes}"
            )
        if self.requests + 1 > self.limits.max_requests:
            raise _BudgetExceeded("镜像 Range 请求次数超过上限")
        if self.total_bytes + length > self.limits.max_total_range_bytes:
            raise _BudgetExceeded("镜像累计 Range 字节超过上限")
        self.requests += 1
        self.total_bytes += length
        try:
            data = self._read_range(offset, length)
        except DiscImageError:
            raise
        except Exception as exc:
            raise DiscImageError(f"镜像 Range 读取失败: offset={offset}") from exc
        if len(data) != length:
            raise DiscImageError(
                f"镜像 Range 短读: offset={offset}, expected={length}, actual={len(data)}"
            )
        return data


def _decode_dstring(raw: bytes) -> str:
    """Decode a fixed-size UDF d-string whose last byte stores its length."""
    if not raw:
        return ""
    length = raw[-1]
    if length == 0:
        return ""
    if length >= len(raw):
        raise DiscImageError("UDF d-string 长度越界")
    return _decode_osta_identifier(raw[:length])


def _decode_osta_identifier(raw: bytes) -> str:
    """Decode one OSTA Compressed Unicode identifier strictly."""
    if not raw:
        return ""
    compression_id = raw[0]
    payload = raw[1:]
    try:
        if compression_id == 0x08:
            return payload.decode("latin-1", errors="strict").rstrip("\x00")
        if compression_id == 0x10:
            if len(payload) % 2:
                raise DiscImageError("UDF 16-bit 文件名字节数为奇数")
            return payload.decode("utf-16-be", errors="strict").rstrip("\x00")
    except UnicodeDecodeError as exc:
        raise DiscImageError("UDF 文件名编码损坏") from exc
    raise DiscImageError(f"UDF 文件名压缩 ID 不受支持: {compression_id}")


def _safe_component(name: str) -> str:
    normalized = unicodedata.normalize("NFC", name)
    if (
        not normalized
        or normalized in {".", ".."}
        or "/" in normalized
        or "\\" in normalized
        or "\x00" in normalized
        or posixpath.basename(normalized) != normalized
        or any(unicodedata.category(char).startswith("C") for char in normalized)
    ):
        raise DiscImageError("UDF 文件名包含不安全路径组件")
    return normalized


def _validate_descriptor_tag(
    buf: bytes,
    *,
    offset: int = 0,
    expected_tags: int | Sequence[int] | None = None,
    expected_location: int | None = None,
    descriptor_length: int,
    allowed_crc_lengths: Sequence[int] | None = None,
    context: str = "UDF descriptor",
) -> int:
    """Validate the complete 16-byte ECMA-167 descriptor tag and body CRC."""
    if offset < 0 or offset + 16 > len(buf):
        raise DiscImageError(f"{context} 标签截断")
    tag, version = struct.unpack_from("<HH", buf, offset)
    checksum = buf[offset + 4]
    reserved = buf[offset + 5]
    descriptor_crc, crc_length = struct.unpack_from("<HH", buf, offset + 8)
    location = struct.unpack_from("<I", buf, offset + 12)[0]
    if version not in (2, 3) or reserved != 0:
        raise DiscImageError(f"{context} 标签版本或保留位异常")
    calculated_checksum = (sum(buf[offset : offset + 16]) - checksum) & 0xFF
    if calculated_checksum != checksum:
        raise DiscImageError(f"{context} 标签 checksum 不匹配")
    if descriptor_length < 16 or offset + descriptor_length > len(buf):
        raise DiscImageError(f"{context} 实际长度越界")
    allowed = (
        tuple(allowed_crc_lengths)
        if allowed_crc_lengths is not None
        else (descriptor_length - 16,)
    )
    if crc_length not in allowed or crc_length <= 0:
        raise DiscImageError(
            f"{context} DescriptorCRCLength 异常: {crc_length}"
        )
    body_start = offset + 16
    body_end = body_start + crc_length
    if body_end > offset + descriptor_length:
        raise DiscImageError(f"{context} CRC 长度越过 descriptor")
    calculated_crc = binascii.crc_hqx(buf[body_start:body_end], 0)
    if calculated_crc != descriptor_crc:
        raise DiscImageError(f"{context} descriptor CRC 不匹配")
    if expected_tags is not None:
        allowed = (expected_tags,) if isinstance(expected_tags, int) else tuple(expected_tags)
        if tag not in allowed:
            raise DiscImageError(f"{context} tag 异常: {tag}")
    if expected_location is not None and location != expected_location:
        raise DiscImageError(
            f"{context} TagLocation 异常: expected={expected_location}, actual={location}"
        )
    return tag


class _SectorReader:
    """2048-byte sector reads through a caller-supplied byte-range callable.

    ``read_range(offset, length) -> bytes`` must serve exact bytes of the
    image. Sectors are cached so a descriptor walk costs each physical
    sector at most once.
    """

    def __init__(
        self,
        read_range: Callable[[int, int], bytes],
        cache_limit: int = 4096,
        *,
        image_size: int | None = None,
    ) -> None:
        self._read_range = read_range
        self._cache: dict[int, bytes] = {}
        self._cache_limit = cache_limit
        self.image_size = image_size

    def sector(self, lba: int) -> bytes:
        if lba < 0:
            raise DiscImageError("镜像 LBA 不能为负数")
        if self.image_size is not None and (lba + 1) * SECTOR_SIZE > self.image_size:
            raise DiscImageError(f"镜像 LBA 越界: {lba}")
        cached = self._cache.get(lba)
        if cached is not None:
            return cached
        data = self._read_range(lba * SECTOR_SIZE, SECTOR_SIZE)
        if len(data) != SECTOR_SIZE:
            raise DiscImageError(f"镜像扇区读取不完整: LBA {lba}")
        if len(self._cache) >= self._cache_limit:
            self._cache.clear()
        self._cache[lba] = data
        return data

    def read_bytes(self, offset: int, length: int) -> bytes:
        """Read arbitrary exact bytes through the sector cache."""
        if offset < 0 or length <= 0:
            raise DiscImageError("镜像字节读取参数非法")
        if self.image_size is not None and offset + length > self.image_size:
            raise DiscImageError("镜像字节读取越过文件末尾")
        first_lba = offset // SECTOR_SIZE
        last_lba = (offset + length - 1) // SECTOR_SIZE
        joined = b"".join(self.sector(lba) for lba in range(first_lba, last_lba + 1))
        start = offset - first_lba * SECTOR_SIZE
        data = joined[start : start + length]
        if len(data) != length:
            raise DiscImageError("镜像缓存字节读取不完整")
        return data

    def prime(self, lba: int, count: int) -> bool:
        """Cache one proven contiguous metadata run with a single Range read.

        The caller supplies an extent obtained from the image's own metadata
        file descriptor.  Runs larger than the cache budget are deliberately
        left demand-paged so this optimization can never turn into an image
        download.
        """
        if lba < 0 or count <= 0 or count > self._cache_limit:
            return False
        if self.image_size is not None and (lba + count) * SECTOR_SIZE > self.image_size:
            raise DiscImageError("镜像 metadata extent 越界")
        if all((lba + index) in self._cache for index in range(count)):
            return True
        data = self._read_range(lba * SECTOR_SIZE, count * SECTOR_SIZE)
        if len(data) != count * SECTOR_SIZE:
            raise DiscImageError(
                f"镜像 metadata Range 读取不完整: LBA {lba}, sectors {count}"
            )
        if len(self._cache) + count > self._cache_limit:
            self._cache.clear()
        for index in range(count):
            start = index * SECTOR_SIZE
            self._cache[lba + index] = data[start : start + SECTOR_SIZE]
        return True


def _tag_identifier(sector: bytes) -> int:
    return struct.unpack_from("<H", sector, 0)[0]


def _parse_long_ad(buf: bytes, offset: int) -> tuple[int, int, int]:
    """Return (length, lbn, partition_reference) from a long_ad."""
    # ECMA-167 long_ad = uint32 extent length + lb_addr, whose logical block
    # number is uint32 and partition reference is uint16.  The trailing six
    # implementation-use bytes are deliberately ignored here.
    length, lbn, part = struct.unpack_from("<IIH", buf, offset)
    return length, lbn, part


def _parse_short_ad(buf: bytes, offset: int) -> tuple[int, int]:
    """Return (length, position) from a short_ad."""
    length, position = struct.unpack_from("<II", buf, offset)
    return length, position


class _UdfImage:
    """A bounded UDF walker over a sector reader.

    Supports the plain layout (FSD in a physical partition) and the UDF
    2.50 metadata partition (FSD and all descriptors in the metadata
    file's extents — the common Blu-ray shape).
    """

    def __init__(
        self,
        reader: _SectorReader,
        limits: DiscProbeLimits | None = None,
    ) -> None:
        self._reader = reader
        self._limits = limits or DiscProbeLimits()
        self._partition_starts: dict[int, int] = {}
        self._partition_lengths: dict[int, int] = {}
        self._partition_ref_to_number: dict[int, int] = {}
        self._fsd_lbn: int | None = None
        self._fsd_partition = 0
        self._metadata_runs_by_ref: dict[int, tuple[tuple[int, int], ...]] = {}
        self._metadata_refs: set[int] = set()
        self._metadata_map_offsets: dict[int, int] = {}
        self._vds_start = 0
        self._extent_count = 0

    @property
    def _metadata_runs(self) -> list[tuple[int, int]]:
        """Compatibility view for focused parser fixtures."""
        if not self._metadata_refs:
            return []
        ref = min(self._metadata_refs)
        return list(self._metadata_runs_by_ref.get(ref, ()))

    @_metadata_runs.setter
    def _metadata_runs(self, value: Sequence[tuple[int, int]]) -> None:
        ref = min(self._metadata_refs) if self._metadata_refs else 1
        self._metadata_runs_by_ref[ref] = tuple(value)

    # -- volume recognition -------------------------------------------
    def open(self) -> None:
        candidates = [256, 512]
        if self._reader.image_size is not None:
            sectors = self._reader.image_size // SECTOR_SIZE
            candidates.extend((sectors - 257, sectors - 1))
        anchor: bytes | None = None
        anchor_lba: int | None = None
        for candidate in dict.fromkeys(value for value in candidates if value >= 0):
            try:
                sector = self._reader.sector(candidate)
            except DiscImageError:
                continue
            if _tag_identifier(sector) != _TAG_ANCHOR:
                continue
            _validate_descriptor_tag(
                sector,
                expected_tags=_TAG_ANCHOR,
                expected_location=candidate,
                descriptor_length=512,
                context="UDF Anchor",
            )
            anchor = sector
            anchor_lba = candidate
            break
        if anchor is None or anchor_lba is None:
            raise _NotThisFormat("未找到 UDF Anchor 卷描述符")
        vds_length, vds_lbn = struct.unpack_from("<II", anchor, 16)
        if vds_length <= 0 or vds_length % SECTOR_SIZE:
            raise DiscImageError("UDF 主卷描述符序列长度异常")
        vds_sectors = vds_length // SECTOR_SIZE
        if vds_sectors > 64:
            raise DiscImageError("UDF 主卷描述符序列超过 64 扇区上限")
        self._vds_start = vds_lbn
        self._read_volume_descriptors(vds_lbn, vds_sectors)
        if self._fsd_lbn is None or not self._partition_starts:
            raise DiscImageError("UDF 卷描述符缺少文件集或分区")
        if self._metadata_refs:
            lvd_sector = getattr(self, "_lvd_sector", None)
            if lvd_sector is None:
                raise DiscImageError("UDF metadata map 缺少逻辑卷描述符")
            for map_ref in sorted(self._metadata_refs):
                self._load_metadata_extent(
                    lvd_sector,
                    map_ref,
                    self._metadata_map_offsets[map_ref],
                )

    def _read_volume_descriptors(self, start_lbn: int, count: int) -> None:
        terminating = False
        for index in range(count):
            sector = self._reader.sector(start_lbn + index)
            tag = _tag_identifier(sector)
            if tag == 0:
                continue
            if tag == _TAG_LOGICAL_VOLUME:
                descriptor_length = 440 + struct.unpack_from("<I", sector, 264)[0]
            elif tag == 7:
                descriptor_length = 24 + 8 * struct.unpack_from("<I", sector, 20)[0]
            else:
                descriptor_length = 512
            tag = _validate_descriptor_tag(
                sector,
                expected_location=start_lbn + index,
                descriptor_length=descriptor_length,
                context="UDF 卷描述符",
            )
            if tag == _TAG_LOGICAL_VOLUME:
                if struct.unpack_from("<I", sector, 212)[0] != SECTOR_SIZE:
                    raise DiscImageError("UDF logical block size 不是 2048")
                _, fsd_lbn, part_ref = _parse_long_ad(sector, 248)
                self._fsd_lbn = fsd_lbn
                self._fsd_partition = part_ref
                self._lvd_sector = sector
                map_count = struct.unpack_from("<I", sector, 268)[0]
                offset = 440
                map_table_length = struct.unpack_from("<I", sector, 264)[0]
                map_table_end = 440 + map_table_length
                if map_table_end > SECTOR_SIZE:
                    raise DiscImageError("UDF 分区映射表跨越逻辑卷描述符扇区")
                for map_index in range(map_count):
                    if offset + 2 > map_table_end:
                        raise DiscImageError("UDF 分区映射表越界")
                    map_type = sector[offset]
                    map_len = sector[offset + 1]
                    if map_len <= 0 or offset + map_len > map_table_end:
                        raise DiscImageError("UDF 分区映射记录异常")
                    if map_type == 1 and map_len >= 6:
                        part_number = struct.unpack_from("<H", sector, offset + 4)[0]
                        self._partition_ref_to_number[map_index] = part_number
                    elif (
                        map_type == 2
                        and map_len == 64
                        and b"*UDF Metadata Partition" in sector[offset + 4 : offset + 36]
                    ):
                        part_number = struct.unpack_from("<H", sector, offset + 38)[0]
                        self._partition_ref_to_number[map_index] = part_number
                        self._metadata_refs.add(map_index)
                        self._metadata_map_offsets[map_index] = offset
                        if not hasattr(self, "_metadata_map_offset"):
                            self._metadata_map_offset = offset
                    offset += map_len
                if offset != map_table_end:
                    raise DiscImageError("UDF 分区映射表长度不匹配")
            elif tag == _TAG_PARTITION:
                part_number = struct.unpack_from("<H", sector, 22)[0]
                part_start = struct.unpack_from("<I", sector, 188)[0]
                part_length = struct.unpack_from("<I", sector, 192)[0]
                if part_length <= 0:
                    raise DiscImageError("UDF PartitionLength 非法")
                self._partition_starts[part_number] = part_start
                self._partition_lengths[part_number] = part_length
            elif tag == 8:  # terminating descriptor
                terminating = True
                break
            elif tag not in {1, 3, 4, 7}:
                raise DiscImageError(f"UDF 主卷描述符 tag 不受支持: {tag}")
        if not terminating:
            raise DiscImageError("UDF 主卷描述符序列未正常终止")

    def _load_metadata_extent(
        self,
        lvd_sector: bytes,
        map_ref: int,
        map_offset: int,
    ) -> None:
        """Resolve one UDF 2.50 metadata map through its file or mirror."""
        part_number = struct.unpack_from("<H", lvd_sector, map_offset + 38)[0]
        partition_start = self._partition_starts.get(part_number)
        if partition_start is None:
            raise DiscImageError("UDF metadata map 引用了不存在的物理分区")
        primary_location = struct.unpack_from("<I", lvd_sector, map_offset + 40)[0]
        mirror_location = struct.unpack_from("<I", lvd_sector, map_offset + 44)[0]
        errors: list[str] = []
        for location, expected_file_type, label in (
            (primary_location, 250, "Metadata File"),
            (mirror_location, 251, "Metadata Mirror File"),
        ):
            if location == 0xFFFFFFFF:
                continue
            try:
                self._validate_partition_extent(
                    partition_number=part_number,
                    position=location,
                    block_count=1,
                )
                sector = self._reader.sector(partition_start + location)
                ea_length = struct.unpack_from("<I", sector, 208)[0]
                ad_length = struct.unpack_from("<I", sector, 212)[0]
                descriptor_length = 216 + ea_length + ad_length
                _validate_descriptor_tag(
                    sector,
                    expected_tags=_TAG_EXTENDED_FILE_ENTRY,
                    expected_location=location,
                    descriptor_length=descriptor_length,
                    context=f"UDF {label}",
                )
                if sector[27] != expected_file_type:
                    raise DiscImageError(f"UDF {label} file type 异常")
                icb_flags = struct.unpack_from("<H", sector, 34)[0]
                if (icb_flags & _ICB_FLAG_AD_MASK) != _ICB_AD_SHORT:
                    raise DiscImageError(f"UDF {label} 必须使用 short_ad")
                info_length = struct.unpack_from("<Q", sector, 56)[0]
                ad_offset = 216 + ea_length
                ad_end = ad_offset + ad_length
                if ad_offset < 216 or ad_end > SECTOR_SIZE or ad_length % 8:
                    raise DiscImageError(f"UDF {label} allocation descriptor 越界")
                runs: list[tuple[int, int]] = []
                extent_lengths: list[int] = []
                for offset in range(ad_offset, ad_end, 8):
                    length, position = _parse_short_ad(sector, offset)
                    if length == 0:
                        if any(sector[offset:ad_end]):
                            raise DiscImageError(f"UDF {label} AD 提前终止")
                        break
                    extent_type = length & 0xC0000000
                    if extent_type != 0:
                        raise DiscImageError(f"UDF {label} 包含不支持的 extent 类型")
                    record_length = length & 0x3FFFFFFF
                    if record_length <= 0:
                        raise DiscImageError(f"UDF {label} extent 长度非法")
                    block_count = (record_length + SECTOR_SIZE - 1) // SECTOR_SIZE
                    self._validate_partition_extent(
                        partition_number=part_number,
                        position=position,
                        block_count=block_count,
                    )
                    absolute_position = partition_start + position
                    runs.append((absolute_position, block_count))
                    extent_lengths.append(record_length)
                self._validate_extent_coverage(
                    info_length,
                    extent_lengths,
                    context=f"UDF {label}",
                )
                if not runs:
                    raise DiscImageError(f"UDF {label} 未提供可用 extent")
                self._note_extents(len(runs))
                self._metadata_runs_by_ref[map_ref] = tuple(runs)
                for position, count in runs:
                    self._reader.prime(position, count)
                return
            except DiscImageError as exc:
                errors.append(f"{label}: {exc}")
        raise DiscImageError(
            "UDF metadata partition 无法通过主文件或镜像解析: "
            + "; ".join(errors)
        )

    def _note_extents(self, count: int) -> None:
        self._extent_count += count
        if self._extent_count > self._limits.max_extents:
            raise _BudgetExceeded("UDF extent 数量超过上限")

    def _validate_extent_bounds(self, lba: int, block_count: int) -> None:
        if lba < 0 or block_count <= 0:
            raise DiscImageError("UDF extent 地址或长度非法")
        if (
            self._reader.image_size is not None
            and (lba + block_count) * SECTOR_SIZE > self._reader.image_size
        ):
            raise DiscImageError("UDF extent 越过镜像文件末尾")

    def _partition_number(self, partition_ref: int) -> int:
        part_number = self._partition_ref_to_number.get(partition_ref)
        if part_number is None and partition_ref in self._partition_starts:
            part_number = partition_ref
        if part_number is None and partition_ref == 0 and len(self._partition_starts) == 1:
            part_number = next(iter(self._partition_starts))
        if part_number is None:
            raise DiscImageError("UDF 分区引用无法解析")
        return part_number

    def _validate_partition_extent(
        self,
        *,
        partition_number: int,
        position: int,
        block_count: int,
    ) -> None:
        if position < 0 or block_count <= 0:
            raise DiscImageError("UDF partition extent 参数非法")
        partition_length = self._partition_lengths.get(partition_number)
        if partition_length is not None and position + block_count > partition_length:
            raise DiscImageError("UDF extent 越过 PartitionLength")
        partition_start = self._partition_starts.get(partition_number)
        if partition_start is None:
            raise DiscImageError("UDF extent 引用了不存在的分区")
        self._validate_extent_bounds(partition_start + position, block_count)

    def _resolve_extent_runs(
        self,
        *,
        lbn: int,
        partition_ref: int,
        byte_length: int,
    ) -> tuple[_ResolvedExtent, ...]:
        if lbn < 0 or byte_length <= 0:
            raise DiscImageError("UDF extent 逻辑地址或字节长度非法")
        block_count = (byte_length + SECTOR_SIZE - 1) // SECTOR_SIZE
        if partition_ref not in self._metadata_refs:
            part_number = self._partition_number(partition_ref)
            self._validate_partition_extent(
                partition_number=part_number,
                position=lbn,
                block_count=block_count,
            )
            physical = self._partition_starts[part_number] + lbn
            return (
                _ResolvedExtent(lbn, partition_ref, physical, byte_length),
            )

        metadata_runs = self._metadata_runs_by_ref.get(partition_ref)
        if not metadata_runs:
            raise DiscImageError("UDF metadata partition 尚未安全映射")
        capacity = sum(count for _position, count in metadata_runs)
        if lbn + block_count > capacity:
            raise DiscImageError("UDF metadata extent 越过 metadata partition")
        logical_cursor = lbn
        remaining_blocks = block_count
        remaining_bytes = byte_length
        skipped = lbn
        resolved: list[_ResolvedExtent] = []
        for physical_start, physical_count in metadata_runs:
            if skipped >= physical_count:
                skipped -= physical_count
                continue
            available = physical_count - skipped
            take_blocks = min(available, remaining_blocks)
            segment_bytes = min(remaining_bytes, take_blocks * SECTOR_SIZE)
            physical_lba = physical_start + skipped
            self._validate_extent_bounds(physical_lba, take_blocks)
            resolved.append(
                _ResolvedExtent(
                    logical_cursor,
                    partition_ref,
                    physical_lba,
                    segment_bytes,
                )
            )
            logical_cursor += take_blocks
            remaining_blocks -= take_blocks
            remaining_bytes -= segment_bytes
            skipped = 0
            if remaining_blocks == 0:
                break
        if remaining_blocks or remaining_bytes:
            raise DiscImageError("UDF metadata extent 映射不完整")
        return tuple(resolved)

    @staticmethod
    def _validate_extent_coverage(
        information_length: int,
        extent_lengths: Sequence[int],
        *,
        context: str,
    ) -> None:
        if information_length < 0:
            raise DiscImageError(f"{context} InformationLength 非法")
        if any(length <= 0 or length > 0x3FFFFFFF for length in extent_lengths):
            raise DiscImageError(f"{context} extent 长度非法")
        if any(length % SECTOR_SIZE for length in extent_lengths[:-1]):
            raise DiscImageError(f"{context} 非最终 extent 未按逻辑块对齐")
        recorded_bytes = sum(extent_lengths)
        if recorded_bytes != information_length:
            raise DiscImageError(
                f"{context} extent 覆盖不精确: expected={information_length}, "
                f"recorded={recorded_bytes}"
            )

    def _absolute_lbn(self, lbn: int, partition_ref: int) -> int:
        """Resolve a partition-relative lbn to an absolute sector LBA.

        With a UDF 2.50 metadata partition, partition reference 1 (the
        metadata map index) resolves through the metadata file's extents.
        """
        if lbn < 0:
            raise DiscImageError("UDF logical block number 不能为负数")
        if partition_ref in self._metadata_refs:
            metadata_runs = self._metadata_runs_by_ref.get(partition_ref)
            if not metadata_runs:
                raise DiscImageError("UDF metadata partition 尚未安全映射")
            remaining = lbn
            for position, count in metadata_runs:
                if remaining < count:
                    return position + remaining
                remaining -= count
            raise DiscImageError("UDF metadata 分区块号越界")
        part_number = self._partition_number(partition_ref)
        partition_length = self._partition_lengths.get(part_number)
        if partition_length is not None and lbn >= partition_length:
            raise DiscImageError("UDF LBN 越过 PartitionLength")
        start = self._partition_starts[part_number]
        return start + lbn

    def _file_entry(self, lbn: int, partition_ref: int) -> dict[str, object]:
        absolute_lba = self._absolute_lbn(lbn, partition_ref)
        sector = self._reader.sector(absolute_lba)
        raw_tag = _tag_identifier(sector)
        if raw_tag not in (_TAG_FILE_ENTRY, _TAG_EXTENDED_FILE_ENTRY):
            raise DiscImageError(f"UDF 文件入口 tag 异常: {raw_tag}")
        extended = raw_tag == _TAG_EXTENDED_FILE_ENTRY
        ea_length_offset = 208 if extended else 168
        ad_length_offset = 212 if extended else 172
        ad_base = 216 if extended else 176
        ea_length = struct.unpack_from("<I", sector, ea_length_offset)[0]
        ad_length = struct.unpack_from("<I", sector, ad_length_offset)[0]
        descriptor_length = ad_base + ea_length + ad_length
        tag = _validate_descriptor_tag(
            sector,
            expected_tags=(_TAG_FILE_ENTRY, _TAG_EXTENDED_FILE_ENTRY),
            expected_location=lbn,
            descriptor_length=descriptor_length,
            context="UDF 文件入口",
        )
        if tag != raw_tag:
            raise DiscImageError("UDF 文件入口 tag 在校验期间发生变化")
        icb_flags = struct.unpack_from("<H", sector, 34)[0]
        file_type = sector[27]
        info_length = struct.unpack_from("<Q", sector, 56)[0]
        ad_offset = ad_base + ea_length
        if ad_offset < ad_base or ad_offset > SECTOR_SIZE:
            raise DiscImageError("UDF 文件入口 extended attributes 越界")
        ad_kind = icb_flags & _ICB_FLAG_AD_MASK
        if ad_kind in (_ICB_AD_EXTENDED, _ICB_AD_IMMEDIATE):
            label = "extended_ad" if ad_kind == _ICB_AD_EXTENDED else "immediate allocation"
            raise DiscImageError(f"UDF 文件入口使用不支持的 {label}")
        if ad_kind not in (_ICB_AD_SHORT, _ICB_AD_LONG):
            raise DiscImageError(f"UDF allocation descriptor 类型异常: {ad_kind}")
        descriptor_size = 8 if ad_kind == _ICB_AD_SHORT else 16
        ad_end = ad_offset + ad_length
        if ad_end > SECTOR_SIZE or ad_length % descriptor_size:
            raise DiscImageError("UDF 文件入口 allocation descriptor 越界或未对齐")

        resolved_runs: list[_ResolvedExtent] = []
        extent_lengths: list[int] = []
        current_partition_ref = partition_ref
        current = sector
        offset = ad_offset
        end = ad_end
        continuations: set[tuple[int, int]] = set()
        while offset < end:
            if offset + descriptor_size > end:
                raise DiscImageError("UDF allocation descriptor 截断")
            if ad_kind == _ICB_AD_SHORT:
                length, position = _parse_short_ad(current, offset)
                extent_partition_ref = current_partition_ref
            else:
                length, position, extent_partition_ref = _parse_long_ad(current, offset)
            offset += descriptor_size
            if length == 0:
                if any(current[offset - descriptor_size : end]):
                    raise DiscImageError("UDF allocation descriptor 提前终止")
                break
            extent_type = length & 0xC0000000
            extent_length = length & 0x3FFFFFFF
            if extent_type == 0xC0000000:
                if any(current[offset:end]):
                    raise DiscImageError("UDF continuation AD 之后仍有非零描述符")
                if extent_partition_ref != current_partition_ref:
                    raise DiscImageError("UDF continuation AD 跨越了逻辑分区")
                if extent_length <= 0 or extent_length > SECTOR_SIZE:
                    raise DiscImageError("UDF 顺延分配描述符长度异常")
                continuation_key = (position, extent_partition_ref)
                if continuation_key in continuations:
                    raise DiscImageError("UDF 顺延分配描述符形成循环")
                continuations.add(continuation_key)
                if len(continuations) > 64:
                    raise DiscImageError("UDF 顺延分配描述符过多")
                current_partition_ref = extent_partition_ref
                current = self._reader.sector(
                    self._absolute_lbn(position, extent_partition_ref)
                )
                continuation_length = struct.unpack_from("<I", current, 20)[0]
                descriptor_length = 24 + continuation_length
                if descriptor_length > extent_length:
                    raise DiscImageError("UDF AED 内容超过 continuation extent 长度")
                _validate_descriptor_tag(
                    current,
                    expected_tags=_TAG_ALLOCATION_EXTENT,
                    expected_location=position,
                    descriptor_length=descriptor_length,
                    allowed_crc_lengths=(8, 8 + continuation_length),
                    context="UDF 顺延分配描述符",
                )
                offset = 24
                end = descriptor_length
                if continuation_length % descriptor_size:
                    raise DiscImageError("UDF 顺延分配描述符内容未对齐")
                continue
            if extent_type != 0:
                raise DiscImageError("UDF 文件包含未记录或未分配区段")
            if extent_length <= 0:
                raise DiscImageError("UDF 文件 extent 长度非法")
            resolved = self._resolve_extent_runs(
                lbn=position,
                partition_ref=extent_partition_ref,
                byte_length=extent_length,
            )
            resolved_runs.extend(resolved)
            extent_lengths.append(extent_length)

        self._validate_extent_coverage(
            info_length,
            extent_lengths,
            context="UDF 文件入口",
        )
        self._note_extents(len(resolved_runs))
        return {
            "file_type": file_type,
            "size": info_length,
            "runs": tuple(
                (item.physical_lba, item.block_count) for item in resolved_runs
            ),
            "resolved_runs": tuple(resolved_runs),
        }

    def _read_directory(
        self,
        runs: Sequence[_ResolvedExtent],
        information_length: int,
    ) -> list[tuple[str, int, int]]:
        """Parse exact logical directory bytes and validate split-FID locations."""
        if information_length < 0:
            raise DiscImageError("UDF 目录 InformationLength 非法")
        if information_length > self._limits.max_directory_bytes:
            raise _BudgetExceeded("UDF 单目录逻辑字节超过上限")
        if sum(item.byte_length for item in runs) != information_length:
            raise DiscImageError("UDF 目录 resolved extent 覆盖不精确")

        chunks: list[bytes] = []
        logical_spans: list[tuple[int, int, int]] = []
        stream_cursor = 0
        for extent in runs:
            self._validate_extent_bounds(extent.physical_lba, extent.block_count)
            segment_remaining = extent.byte_length
            for block in range(extent.block_count):
                sector = self._reader.sector(extent.physical_lba + block)
                useful = min(segment_remaining, SECTOR_SIZE)
                chunks.append(sector[:useful])
                segment_remaining -= useful
            if segment_remaining:
                raise DiscImageError("UDF 目录物理 extent 读取不完整")
            logical_spans.append(
                (
                    stream_cursor,
                    stream_cursor + extent.byte_length,
                    extent.logical_lbn,
                )
            )
            stream_cursor += extent.byte_length
        data = b"".join(chunks)
        if len(data) != information_length:
            raise DiscImageError("UDF 目录逻辑字节读取不完整")

        def tag_location_for(stream_offset: int) -> int:
            for span_start, span_end, logical_lbn in logical_spans:
                if span_start <= stream_offset < span_end:
                    return logical_lbn + (stream_offset - span_start) // SECTOR_SIZE
            raise DiscImageError("UDF FID 起点不在任何逻辑 extent 内")

        children: list[tuple[str, int, int]] = []
        seen_names: set[str] = set()
        offset = 0
        while offset < len(data):
            if data[offset] == 0:
                next_block = min(
                    len(data),
                    ((offset // SECTOR_SIZE) + 1) * SECTOR_SIZE,
                )
                if any(data[offset:next_block]):
                    raise DiscImageError("UDF 目录 padding 中包含非零字节")
                offset = next_block
                continue
            if offset + 38 > len(data):
                raise DiscImageError("UDF File Identifier Descriptor 头部截断")
            characteristics = data[offset + 18]
            l_fi = data[offset + 19]
            l_iu = struct.unpack_from("<H", data, offset + 36)[0]
            name_start = offset + 38 + l_iu
            fid_end = name_start + l_fi
            advance = (38 + l_iu + l_fi + 3) // 4 * 4
            record_end = offset + advance
            if (
                advance < 38
                or name_start < offset + 38
                or fid_end > record_end
                or record_end > len(data)
            ):
                raise DiscImageError("UDF File Identifier Descriptor 长度越界")
            _validate_descriptor_tag(
                data,
                offset=offset,
                expected_tags=_TAG_FILE_ID,
                expected_location=tag_location_for(offset),
                descriptor_length=advance,
                context="UDF File Identifier Descriptor",
            )
            _, icb_lbn, icb_part = _parse_long_ad(data, offset + 20)
            if not (characteristics & (_FID_PARENT_FLAG | _FID_DELETED_FLAG)):
                if l_fi <= 0:
                    raise DiscImageError("UDF 非 parent FID 缺少文件名")
                name = _safe_component(_decode_osta_identifier(data[name_start:fid_end]))
                name_key = name.casefold()
                if name_key in seen_names:
                    raise DiscImageError("UDF 目录包含重复或大小写冲突文件名")
                seen_names.add(name_key)
                children.append((name, icb_lbn, icb_part))
            offset = record_end
        if offset != information_length:
            raise DiscImageError("UDF 目录未精确消费 InformationLength")
        return children

    def walk(self) -> list[InnerFile]:
        if self._fsd_lbn is None:
            raise DiscImageError("UDF 文件集位置未初始化")
        fsd = self._reader.sector(
            self._absolute_lbn(self._fsd_lbn, self._fsd_partition)
        )
        _validate_descriptor_tag(
            fsd,
            expected_tags=_TAG_FILE_SET,
            expected_location=self._fsd_lbn,
            descriptor_length=512,
            context="UDF 文件集描述符",
        )
        root_length, root_lbn, root_part = _parse_long_ad(fsd, 400)
        if root_length == 0 or root_length & 0xC0000000:
            raise DiscImageError("UDF 根目录 ICB extent 异常")
        files: list[InnerFile] = []
        stack: list[tuple[str, int, int, int]] = [("/", root_lbn, root_part, 0)]
        seen: set[tuple[int, int]] = set()
        seen_paths: set[str] = set()
        directories = 0
        while stack:
            prefix, lbn, part, depth = stack.pop()
            if depth > self._limits.max_depth:
                raise _BudgetExceeded("UDF 目录深度超过上限")
            if (lbn, part) in seen:
                continue
            seen.add((lbn, part))
            directories += 1
            if directories > self._limits.max_directories:
                raise _BudgetExceeded("UDF 目录数量超过上限")
            entry = self._file_entry(lbn, part)
            if entry["file_type"] != _FILE_TYPE_DIRECTORY:
                raise DiscImageError("UDF 目录 FID 指向了非目录入口")
            children = self._read_directory(
                entry["resolved_runs"],
                int(entry["size"]),
            )
            for name, child_lbn, child_part in children:
                child = self._file_entry(child_lbn, child_part)
                path = posixpath.join(prefix, name)
                path_key = unicodedata.normalize("NFC", path).casefold()
                if path_key in seen_paths:
                    raise DiscImageError("UDF 文件树包含重复或大小写冲突路径")
                seen_paths.add(path_key)
                if child["file_type"] == _FILE_TYPE_DIRECTORY:
                    stack.append((path, child_lbn, child_part, depth + 1))
                elif child["file_type"] == _FILE_TYPE_FILE:
                    files.append(
                        InnerFile(path, int(child["size"]), tuple(child["runs"]))
                    )
                    if len(files) > self._limits.max_files:
                        raise _BudgetExceeded("UDF 文件数量超过上限")
                elif child["file_type"] == _FILE_TYPE_SYMLINK:
                    raise DiscImageError("UDF 文件树包含不允许的符号链接")
                else:
                    raise DiscImageError(
                        f"UDF 文件树包含不支持的 file type: {child['file_type']}"
                    )
        return files


class _Iso9660Image:
    """A bounded ISO 9660 walker (primary volume descriptor only)."""

    def __init__(
        self,
        reader: _SectorReader,
        limits: DiscProbeLimits | None = None,
    ) -> None:
        self._reader = reader
        self._limits = limits or DiscProbeLimits()
        self._pvd: bytes | None = None
        self._volume_sectors: int | None = None
        self._extent_count = 0

    def open(self) -> None:
        found_pvd = False
        for index in range(64):
            lba = 16 + index
            descriptor = self._reader.sector(lba)
            if descriptor[1:6] != b"CD001":
                if index == 0:
                    raise _NotThisFormat("未找到 ISO 9660 卷描述符")
                raise DiscImageError("ISO 9660 卷描述符标识异常")
            descriptor_type = descriptor[0]
            if descriptor[6] != 1:
                raise DiscImageError("ISO 9660 卷描述符版本异常")
            if descriptor_type == 1:
                if found_pvd:
                    raise DiscImageError("ISO 9660 存在多个主卷描述符")
                found_pvd = True
                self._pvd = descriptor
                block_size_le = struct.unpack_from("<H", descriptor, 128)[0]
                block_size_be = struct.unpack_from(">H", descriptor, 130)[0]
                if block_size_le != SECTOR_SIZE or block_size_be != SECTOR_SIZE:
                    raise DiscImageError("ISO 9660 logical block size 不是 2048")
                sectors_le = struct.unpack_from("<I", descriptor, 80)[0]
                sectors_be = struct.unpack_from(">I", descriptor, 84)[0]
                if sectors_le <= 0 or sectors_le != sectors_be:
                    raise DiscImageError("ISO 9660 卷空间大小字段异常")
                self._volume_sectors = sectors_le
                if (
                    self._reader.image_size is not None
                    and sectors_le * SECTOR_SIZE > self._reader.image_size
                ):
                    raise DiscImageError("ISO 9660 卷空间越过镜像文件末尾")
            elif descriptor_type == 255:
                if not found_pvd:
                    raise DiscImageError("ISO 9660 终止描述符早于主卷描述符")
                return
        raise DiscImageError("ISO 9660 卷描述符序列未终止")

    def _validate_extent(self, extent: int, size: int) -> int:
        if extent < 0 or size < 0:
            raise DiscImageError("ISO 9660 extent 参数非法")
        blocks = (size + SECTOR_SIZE - 1) // SECTOR_SIZE
        if blocks == 0:
            return 0
        if self._volume_sectors is not None and extent + blocks > self._volume_sectors:
            raise DiscImageError("ISO 9660 extent 越过卷空间")
        if (
            self._reader.image_size is not None
            and (extent + blocks) * SECTOR_SIZE > self._reader.image_size
        ):
            raise DiscImageError("ISO 9660 extent 越过镜像文件末尾")
        self._extent_count += 1
        if self._extent_count > self._limits.max_extents:
            raise _BudgetExceeded("ISO 9660 extent 数量超过上限")
        return blocks

    @staticmethod
    def _dir_records(buf: bytes, start: int, end: int):
        if start < 0 or end < start or end > len(buf):
            raise DiscImageError("ISO 9660 目录缓冲区边界异常")
        offset = start
        while offset < end:
            length = buf[offset]
            if length == 0:
                if any(buf[offset:end]):
                    raise DiscImageError("ISO 9660 目录 padding 中包含非零字节")
                break
            if length < 34 or offset + length > end:
                raise DiscImageError("ISO 9660 目录记录长度越界")
            flags = buf[offset + 25]
            if flags & 0x80:
                raise DiscImageError("ISO 9660 multi-extent 文件不受支持")
            extent_le = struct.unpack_from("<I", buf, offset + 2)[0]
            extent_be = struct.unpack_from(">I", buf, offset + 6)[0]
            size_le = struct.unpack_from("<I", buf, offset + 10)[0]
            size_be = struct.unpack_from(">I", buf, offset + 14)[0]
            if extent_le != extent_be or size_le != size_be:
                raise DiscImageError("ISO 9660 both-endian 字段不一致")
            name_len = buf[offset + 32]
            if name_len == 0 or 33 + name_len > length:
                raise DiscImageError("ISO 9660 文件标识长度异常")
            raw_name = buf[offset + 33 : offset + 33 + name_len]
            if raw_name not in (b"\x00", b"\x01"):
                try:
                    name = raw_name.split(b";", 1)[0].decode(
                        "ascii", errors="strict"
                    )
                except UnicodeDecodeError as exc:
                    raise DiscImageError("ISO 9660 文件名不是 ASCII") from exc
                yield _safe_component(name), extent_le, size_le, bool(flags & 0x02)
            offset += length

    def walk(self) -> list[InnerFile]:
        if self._pvd is None:
            raise DiscImageError("ISO 9660 主卷描述符未初始化")
        root_extent = struct.unpack_from("<I", self._pvd, 158)[0]
        root_size = struct.unpack_from("<I", self._pvd, 166)[0]
        files: list[InnerFile] = []
        stack: list[tuple[str, int, int, int]] = [("/", root_extent, root_size, 0)]
        seen: set[int] = set()
        seen_paths: set[str] = set()
        directories = 0
        while stack:
            prefix, extent, size, depth = stack.pop()
            if depth > self._limits.max_depth:
                raise _BudgetExceeded("ISO 9660 目录深度超过上限")
            if extent in seen:
                continue
            seen.add(extent)
            directories += 1
            if directories > self._limits.max_directories:
                raise _BudgetExceeded("ISO 9660 目录数量超过上限")
            if size > self._limits.max_directory_bytes:
                raise _BudgetExceeded("ISO 9660 单目录逻辑字节超过上限")
            block_count = self._validate_extent(extent, size)
            remaining = size
            for block in range(block_count):
                sector = self._reader.sector(extent + block)
                useful = min(remaining, SECTOR_SIZE)
                for name, child_extent, child_size, is_dir in self._dir_records(
                    sector, 0, useful
                ):
                    path = posixpath.join(prefix, name)
                    path_key = unicodedata.normalize("NFC", path).casefold()
                    if path_key in seen_paths:
                        raise DiscImageError("ISO 9660 文件树包含重复路径")
                    seen_paths.add(path_key)
                    if is_dir:
                        stack.append((path, child_extent, child_size, depth + 1))
                    else:
                        child_blocks = self._validate_extent(child_extent, child_size)
                        runs = ((child_extent, child_blocks),) if child_blocks else ()
                        files.append(InnerFile(path, child_size, runs))
                        if len(files) > self._limits.max_files:
                            raise _BudgetExceeded("ISO 9660 文件数量超过上限")
                remaining -= useful
            if remaining != 0:
                raise DiscImageError("ISO 9660 目录 extent 覆盖不足")
        return files


def _classify(files: Sequence[InnerFile]) -> str:
    paths = {f.inner_path.lower() for f in files}
    if any("/bdmv/" in p for p in paths):
        return "bdmv"
    if any("/video_ts/" in p for p in paths):
        return "video_ts"
    return "flat" if files else "unknown"


def iter_inner_file_ranges(
    inner_file: InnerFile,
    *,
    chunk_bytes: int = 8 * 1024 * 1024,
):
    """Yield exact image byte ranges for one inner file, never the full ISO."""
    if chunk_bytes <= 0:
        raise ValueError("chunk_bytes 必须大于 0")
    remaining = inner_file.size
    for lba, block_count in inner_file.extents:
        if lba < 0 or block_count <= 0:
            raise DiscImageError("镜像内部文件 extent 非法")
        extent_bytes = block_count * SECTOR_SIZE
        useful_bytes = min(remaining, extent_bytes)
        offset = lba * SECTOR_SIZE
        while useful_bytes > 0:
            length = min(useful_bytes, chunk_bytes)
            yield offset, length
            offset += length
            useful_bytes -= length
            remaining -= length
        if remaining == 0:
            break
    if remaining != 0:
        raise DiscImageError(
            f"镜像内部文件 extent 不完整: {inner_file.inner_path}, missing={remaining}"
        )


def read_inner_file(
    read_range: Callable[[int, int], bytes],
    inner_file: InnerFile,
    *,
    max_bytes: int = 4 * 1024 * 1024,
) -> bytes:
    """Read one bounded small inner file such as MPLS/CLPI metadata."""
    if inner_file.size < 0 or inner_file.size > max_bytes:
        raise DiscImageError(
            f"镜像内部元数据超过 {max_bytes} 字节上限: {inner_file.inner_path}"
        )
    chunks: list[bytes] = []
    for offset, length in iter_inner_file_ranges(
        inner_file,
        chunk_bytes=max(1, min(max_bytes, 1024 * 1024)),
    ):
        data = read_range(offset, length)
        if len(data) != length:
            raise DiscImageError(
                f"镜像内部文件短读: {inner_file.inner_path}, expected={length}, actual={len(data)}"
            )
        chunks.append(data)
    return b"".join(chunks)


def iter_inner_file_bytes(
    read_range: Callable[[int, int], bytes],
    inner_file: InnerFile,
    *,
    image_size: int | None = None,
    chunk_bytes: int = 32 * 1024 * 1024,
):
    """Yield one inner file as bounded exact-Range chunks.

    The caller owns the destination.  This iterator never creates a local
    file and never requests unrelated sectors from the surrounding image.
    """
    if chunk_bytes <= 0 or chunk_bytes > 128 * 1024 * 1024:
        raise ValueError("chunk_bytes 必须位于 1..134217728")
    if image_size is not None and (
        isinstance(image_size, bool)
        or not isinstance(image_size, int)
        or image_size <= 0
    ):
        raise ValueError("image_size 必须是正整数")
    yielded = 0
    for offset, length in iter_inner_file_ranges(
        inner_file,
        chunk_bytes=chunk_bytes,
    ):
        if image_size is not None and offset + length > image_size:
            raise DiscImageError(
                f"镜像内部文件 Range 越过镜像末尾: {inner_file.inner_path}"
            )
        data = read_range(offset, length)
        if not isinstance(data, bytes) or len(data) != length:
            actual = len(data) if isinstance(data, (bytes, bytearray)) else -1
            raise DiscImageError(
                f"镜像内部文件短读: {inner_file.inner_path}, "
                f"expected={length}, actual={actual}"
            )
        yielded += len(data)
        yield data
    if yielded != inner_file.size:
        raise DiscImageError(
            f"镜像内部文件读取大小不匹配: {inner_file.inner_path}, "
            f"expected={inner_file.size}, actual={yielded}"
        )


def digest_inner_file(
    read_range: Callable[[int, int], bytes],
    inner_file: InnerFile,
    *,
    image_size: int | None = None,
    chunk_bytes: int = 32 * 1024 * 1024,
) -> InnerFileDigest:
    """Calculate Quark's required hashes in a read-only first pass."""
    md5 = hashlib.md5(usedforsecurity=False)
    sha1 = hashlib.sha1(usedforsecurity=False)
    size = 0
    for chunk in iter_inner_file_bytes(
        read_range,
        inner_file,
        image_size=image_size,
        chunk_bytes=chunk_bytes,
    ):
        md5.update(chunk)
        sha1.update(chunk)
        size += len(chunk)
    return InnerFileDigest(size=size, md5=md5.hexdigest(), sha1=sha1.hexdigest())


def _alist_image_fingerprint(
    alist_client: object,
    image_path: str,
) -> tuple[int, object]:
    statter = getattr(alist_client, "exact_file_info", None)
    if not callable(statter):
        raise DiscImageError("AList 客户端缺少 exact_file_info 安全接口")
    try:
        info = statter(image_path)
    except Exception as exc:
        raise DiscImageError("AList 镜像精确回读失败") from exc
    if not isinstance(info, Mapping):
        raise DiscImageError("AList 镜像精确回读格式异常")
    size = info.get("size")
    version = info.get("version")
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise DiscImageError("AList 未返回有效镜像大小")
    if version is None or version == "":
        raise DiscImageError("AList 未返回可用于稳定读取的镜像版本")
    return size, version


def retrying_alist_range_reader(
    alist_client: object,
    *,
    image_path: str,
    image_size: int,
    attempts: int = 6,
    base_delay: float = 2.0,
    max_delay: float = 60.0,
    sleep: Callable[[float], None] | None = None,
):
    """Open an exact-Range reader that survives transient transport faults.

    A proxy between the engine and the provider can truncate one Range
    response mid-transfer (observed in production: a 16 MiB request returned
    ~2 MiB while re-requesting the same extent succeeded) and a long transfer
    can outlive the provider read link cached by the plain reader.  This
    helper wraps the strict reader with a bounded per-extent retry that
    re-opens the reader (fresh link) before each retry, re-checks the exact
    length locally, and re-verifies that the image fingerprint still matches
    so a replaced image can never be spliced into an in-flight transfer.

    Every attempt reads the same extent, so a persistently failing extent
    still propagates the strict underlying failure after ``attempts`` tries.
    The fail-closed guarantees of the strict reader are unchanged.
    """
    if isinstance(attempts, bool) or attempts <= 0:
        raise ValueError("attempts 必须为正")
    if base_delay < 0 or max_delay < 0 or max_delay < base_delay:
        raise ValueError("重试延迟参数非法")
    pause = sleep if sleep is not None else time.sleep

    opener = getattr(alist_client, "open_file_range_reader", None)
    single_reader = getattr(alist_client, "read_file_range", None)
    if not callable(opener) and not callable(single_reader):
        raise DiscImageError("AList 客户端缺少严格 HTTP Range 安全接口")
    statter = getattr(alist_client, "exact_file_info", None)
    fingerprint = (
        statter(image_path)
        if callable(statter)
        else {"size": image_size, "version": None}
    )
    if not isinstance(fingerprint, Mapping) or fingerprint.get("size") != image_size:
        raise DiscImageError("AList 镜像指纹与展开声明不一致")

    def _open():
        if callable(opener):
            return opener(
                image_path,
                expected_size=image_size,
                refresh=True,
            )
        return contextlib.nullcontext(
            lambda offset, length: single_reader(
                image_path,
                offset,
                length,
                expected_size=image_size,
                refresh=True,
            )
        )

    current: dict[str, object] = {"ctx": None, "reader": None}

    def _drop_reader() -> None:
        ctx = current["ctx"]
        current["ctx"] = None
        current["reader"] = None
        if ctx is not None:
            try:
                ctx.__exit__(None, None, None)
            except Exception:
                pass

    @contextlib.contextmanager
    def managed_reader():
        try:
            yield _retrying_read
        finally:
            _drop_reader()

    def _retrying_read(offset: int, length: int) -> bytes:
        if (
            isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
            or isinstance(length, bool)
            or not isinstance(length, int)
            or length <= 0
            or offset + length > image_size
        ):
            raise DiscImageError("重试 Range 请求越过镜像边界")
        delay = base_delay
        last_error: Exception | None = None
        for attempt in range(attempts):
            if attempt:
                pause(delay)
                delay = min(delay * 2.0, max_delay)
                if callable(statter):
                    fresh = statter(image_path)
                    if (
                        not isinstance(fresh, Mapping)
                        or fresh.get("size") != image_size
                        or fresh.get("version") != fingerprint.get("version")
                    ):
                        raise DiscImageError("AList 镜像在重试期间发生变化")
            if current["reader"] is None:
                ctx = _open()
                try:
                    current["reader"] = ctx.__enter__()
                    current["ctx"] = ctx
                except Exception as exc:
                    last_error = exc
                    continue
            try:
                data = current["reader"](offset, length)
            except DiscImageError:
                raise
            except Exception as exc:  # bounded transport retry, then propagate
                last_error = exc
                _drop_reader()
                continue
            if isinstance(data, bytes) and len(data) == length:
                return data
            if not isinstance(data, (bytes, bytearray)):
                _drop_reader()
                raise DiscImageError("Range 读取返回了非字节载荷")
            actual = len(data)
            last_error = DiscImageError(
                f"Range 响应长度不匹配（重试后仍失败）: "
                f"offset={offset}, expected={length}, actual={actual}"
            )
            _drop_reader()
        assert last_error is not None
        raise last_error

    return managed_reader()


def _alist_range_context(
    alist_client: object,
    image_path: str,
    image_size: int,
):
    opener = getattr(alist_client, "open_file_range_reader", None)
    single_reader = getattr(alist_client, "read_file_range", None)
    if callable(opener):
        return opener(
            image_path,
            expected_size=image_size,
            refresh=True,
        )
    if callable(single_reader):
        def read_range(offset: int, length: int) -> bytes:
            return single_reader(
                image_path,
                offset,
                length,
                expected_size=image_size,
                refresh=False,
            )

        return contextlib.nullcontext(read_range)
    raise DiscImageError("AList 客户端缺少严格 HTTP Range 安全接口")


def stream_inner_file_via_alist(
    alist_client: object,
    *,
    image_path: str,
    inner_file: InnerFile,
    target_path: str,
    content_type: str = "video/mp2t",
    chunk_bytes: int = 32 * 1024 * 1024,
) -> InnerFileTransferResult:
    """Transfer one image member to AList staging without local disk use.

    Pass one calculates MD5/SHA1 while discarding the bytes.  Pass two sends
    the exact same extents to AList with those hashes so Quark can multipart
    upload directly instead of asking AList to spool the complete object.
    Stable source fingerprints and a second-pass digest make a changed image
    fail closed even if a provider URL remains readable.
    """
    if inner_file.size <= 0:
        raise DiscImageError("拒绝传输空的镜像内部文件")
    statter = getattr(alist_client, "exact_file_info", None)
    uploader = getattr(alist_client, "upload_stream", None)
    if not callable(statter) or not callable(uploader):
        raise DiscImageError("AList 客户端缺少零落盘上传安全接口")

    image_size, source_version = _alist_image_fingerprint(alist_client, image_path)
    try:
        existing = statter(target_path)
    except Exception as exc:
        raise DiscImageError("AList staging 上传前精确回读失败") from exc
    if existing is not None:
        raise DiscImageError(f"AList staging 目标已存在，拒绝覆盖: {target_path}")

    try:
        with _alist_range_context(alist_client, image_path, image_size) as read_range:
            digest = digest_inner_file(
                read_range,
                inner_file,
                image_size=image_size,
                chunk_bytes=chunk_bytes,
            )
    except DiscImageError:
        raise
    except Exception as exc:
        raise DiscImageError("镜像内部文件第一遍哈希失败") from exc
    if digest.size != inner_file.size:
        raise DiscImageError("镜像内部文件第一遍大小证明失败")
    if _alist_image_fingerprint(alist_client, image_path) != (
        image_size,
        source_version,
    ):
        raise DiscImageError("AList 镜像在第一遍哈希期间发生变化")

    second_md5 = hashlib.md5(usedforsecurity=False)
    second_sha1 = hashlib.sha1(usedforsecurity=False)
    second_size = 0
    try:
        with _alist_range_context(alist_client, image_path, image_size) as read_range:
            def upload_chunks():
                nonlocal second_size
                for chunk in iter_inner_file_bytes(
                    read_range,
                    inner_file,
                    image_size=image_size,
                    chunk_bytes=chunk_bytes,
                ):
                    second_md5.update(chunk)
                    second_sha1.update(chunk)
                    second_size += len(chunk)
                    yield chunk

            uploader(
                target_path,
                upload_chunks(),
                size=inner_file.size,
                md5=digest.md5,
                sha1=digest.sha1,
                content_type=content_type,
            )
    except DiscImageError:
        raise
    except Exception as exc:
        raise DiscImageError("镜像内部文件第二遍流式上传失败") from exc

    second_digest = InnerFileDigest(
        size=second_size,
        md5=second_md5.hexdigest(),
        sha1=second_sha1.hexdigest(),
    )
    if second_digest != digest:
        raise DiscImageError("镜像内部文件两遍内容摘要不一致")
    if _alist_image_fingerprint(alist_client, image_path) != (
        image_size,
        source_version,
    ):
        raise DiscImageError("AList 镜像在第二遍上传期间发生变化")

    try:
        target_info = statter(target_path)
    except Exception as exc:
        raise DiscImageError("AList staging 上传后精确回读失败") from exc
    if not isinstance(target_info, Mapping):
        raise DiscImageError("AList staging 上传后目标不存在")
    if target_info.get("size") != inner_file.size:
        raise DiscImageError(
            f"AList staging 上传后大小不匹配: expected={inner_file.size}, "
            f"actual={target_info.get('size')}"
        )
    return InnerFileTransferResult(
        image_path=image_path,
        inner_path=inner_file.inner_path,
        target_path=target_path,
        size=inner_file.size,
        md5=digest.md5,
        sha1=digest.sha1,
        source_version=source_version,
        target_version=target_info.get("version"),
    )


def parse_mpls(data: bytes, *, inner_path: str = "") -> DiscPlaylist:
    """Parse the bounded play-item table needed for episode evidence."""
    if len(data) < 32 or data[:4] != b"MPLS":
        raise DiscImageError("Blu-ray playlist 头部异常")
    playlist_start = struct.unpack_from(">I", data, 8)[0]
    if playlist_start + 10 > len(data):
        raise DiscImageError("Blu-ray playlist 区段越界")
    item_count = struct.unpack_from(">H", data, playlist_start + 6)[0]
    if item_count > 4096:
        raise DiscImageError("Blu-ray playlist 项目过多")
    offset = playlist_start + 10
    items: list[DiscPlayItem] = []
    for _ in range(item_count):
        if offset + 22 > len(data):
            raise DiscImageError("Blu-ray play item 截断")
        item_length = struct.unpack_from(">H", data, offset)[0]
        item_end = offset + 2 + item_length
        if item_length < 20 or item_end > len(data):
            raise DiscImageError("Blu-ray play item 长度异常")
        clip_id = data[offset + 2 : offset + 7].decode("ascii", errors="strict")
        codec_id = data[offset + 7 : offset + 11].decode("ascii", errors="strict")
        in_time, out_time = struct.unpack_from(">II", data, offset + 14)
        if out_time < in_time:
            raise DiscImageError("Blu-ray play item 时间范围异常")
        items.append(DiscPlayItem(clip_id, codec_id, in_time, out_time))
        offset = item_end
    return DiscPlaylist(inner_path=inner_path, play_items=tuple(items))


def probe_disc_image(
    read_range: Callable[[int, int], bytes],
    *,
    image_path: str = "",
    prefer: str | None = None,
    image_size: int | None = None,
    limits: DiscProbeLimits | None = None,
) -> DiscInventory:
    """Expand one disc image read-only into its inner file inventory.

    ``read_range(offset, length)`` must serve exact bytes of the image.
    UDF is tried first — Blu-ray images are the common case and DVD images
    with UDF bridges only expose ``BEA``/``NSR`` markers at LBA 16 — then
    plain ISO 9660. Any parse failure is a bounded, read-only error: the
    image stays opaque to the engine.
    """
    active_limits = limits or DiscProbeLimits()
    if prefer not in (None, "udf", "iso9660"):
        raise ValueError("prefer 必须是 udf、iso9660 或 None")
    budget = _RangeBudget(
        read_range,
        image_size=image_size,
        limits=active_limits,
    )
    reader = _SectorReader(
        budget.read,
        image_size=image_size,
    )
    errors: list[str] = []
    for kind, parser_cls in (
        ("udf", _UdfImage),
        ("iso9660", _Iso9660Image),
    ):
        if prefer is not None and kind != prefer:
            continue
        try:
            parser = parser_cls(reader, active_limits)
            parser.open()
            files = parser.walk()
        except _NotThisFormat as exc:
            errors.append(f"{kind}: {exc}")
            continue
        except _BudgetExceeded:
            raise
        except DiscImageError as exc:
            # Once a UDF Anchor or ISO PVD was recognized, a structural or
            # integrity failure must keep the image opaque. Falling back to a
            # bridge filesystem could silently expose only part of the disc.
            raise DiscImageError(f"{kind}: {exc}") from exc
        playlist_files = [
            inner_file
            for inner_file in files
            if inner_file.inner_path.casefold().endswith(".mpls")
        ]
        if len(playlist_files) > active_limits.max_playlists:
            raise _BudgetExceeded("Blu-ray playlist 数量超过上限")
        if any(item.size > 4 * 1024 * 1024 for item in playlist_files):
            raise DiscImageError("Blu-ray playlist 单文件超过 4 MiB 上限")
        playlist_bytes = sum(item.size for item in playlist_files)
        if playlist_bytes > active_limits.max_playlist_bytes:
            raise _BudgetExceeded("Blu-ray playlist 累计字节超过上限")

        # Playlist files are normally one-sector records packed into one
        # contiguous Blu-ray metadata area. Prime only the exact sectors that
        # cover those small files, merging adjacent LBAs into bounded reads.
        playlist_lbas: set[int] = set()
        for item in playlist_files:
            remaining = item.size
            for lba, block_count in item.extents:
                useful_blocks = min(
                    block_count,
                    (remaining + SECTOR_SIZE - 1) // SECTOR_SIZE,
                )
                playlist_lbas.update(range(lba, lba + useful_blocks))
                remaining -= min(remaining, useful_blocks * SECTOR_SIZE)
                if remaining == 0:
                    break
            if remaining:
                raise DiscImageError(
                    f"Blu-ray playlist extent 不完整: {item.inner_path}"
                )
        ordered_lbas = sorted(playlist_lbas)
        if ordered_lbas:
            run_start = ordered_lbas[0]
            run_end = run_start
            for lba in ordered_lbas[1:] + [ordered_lbas[-1] + 2]:
                if lba == run_end + 1:
                    run_end = lba
                    continue
                reader.prime(run_start, run_end - run_start + 1)
                run_start = run_end = lba

        playlists: list[DiscPlaylist] = []
        for inner_file in playlist_files:
            playlist = parse_mpls(
                read_inner_file(reader.read_bytes, inner_file),
                inner_path=inner_file.inner_path,
            )
            playlists.append(playlist)
        return DiscInventory(
            image_path=image_path,
            kind=kind,
            inner_files=tuple(sorted(files, key=lambda f: f.inner_path)),
            structure=_classify(files),
            playlists=tuple(sorted(playlists, key=lambda item: item.inner_path)),
        )
    raise DiscImageError("; ".join(errors) or "镜像格式无法识别")


def probe_disc_image_via_alist(
    alist_client: object,
    image_path: str,
    *,
    limits: DiscProbeLimits | None = None,
) -> DiscInventory:
    """Probe one remote image with a stable AList fingerprint and exact Range."""
    import contextlib

    statter = getattr(alist_client, "exact_file_info", None)
    if not callable(statter):
        raise DiscImageError("AList 客户端缺少 exact_file_info 安全接口")
    try:
        before = statter(image_path)
    except Exception as exc:
        raise DiscImageError("AList 镜像读取前精确回读失败") from exc
    if not isinstance(before, Mapping):
        raise DiscImageError("AList 镜像读取前精确回读格式异常")
    image_size = before.get("size")
    version = before.get("version")
    if isinstance(image_size, bool) or not isinstance(image_size, int) or image_size <= 0:
        raise DiscImageError("AList 未返回有效镜像大小")
    if version is None or version == "":
        raise DiscImageError("AList 未返回可用于稳定读取的镜像版本")

    opener = getattr(alist_client, "open_file_range_reader", None)
    single_reader = getattr(alist_client, "read_file_range", None)
    if callable(opener):
        range_context = opener(
            image_path,
            expected_size=image_size,
            refresh=True,
        )
    elif callable(single_reader):
        def read_range(offset: int, length: int) -> bytes:
            return single_reader(
                image_path,
                offset,
                length,
                expected_size=image_size,
                refresh=False,
            )

        range_context = contextlib.nullcontext(read_range)
    else:
        raise DiscImageError("AList 客户端缺少严格 HTTP Range 安全接口")

    try:
        with range_context as read_range:
            inventory = probe_disc_image(
                read_range,
                image_path=image_path,
                image_size=image_size,
                limits=limits,
            )
    except DiscImageError:
        raise
    except Exception as exc:
        raise DiscImageError("AList 镜像严格 Range 探测失败") from exc

    try:
        after = statter(image_path)
    except Exception as exc:
        raise DiscImageError("AList 镜像读取后精确回读失败") from exc
    if not isinstance(after, Mapping):
        raise DiscImageError("AList 镜像读取后精确回读格式异常")
    if after.get("size") != image_size or after.get("version") != version:
        raise DiscImageError("AList 镜像在 Range 探测期间发生变化")
    return inventory


__all__ = [
    "DiscImageError",
    "DiscInventory",
    "DiscPlayItem",
    "DiscPlaylist",
    "DiscProbeLimits",
    "InnerFile",
    "InnerFileDigest",
    "InnerFileTransferResult",
    "digest_inner_file",
    "iter_inner_file_ranges",
    "iter_inner_file_bytes",
    "parse_mpls",
    "probe_disc_image",
    "probe_disc_image_via_alist",
    "read_inner_file",
    "retrying_alist_range_reader",
    "stream_inner_file_via_alist",
]
