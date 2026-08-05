"""Deterministic canonical layout for independently identified works.

The planner deliberately separates two questions which the legacy scraper
mixed together:

* identity is *only* ``(metadata namespace, id)``;
* a title boundary may propose a franchise/family container, but can never
  merge two identities into one TV leaf or one season tree.

All inputs are already-confirmed metadata records.  Source folder names and
search confidence do not participate in identity here.
"""

from __future__ import annotations

import posixpath
import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable


class CanonicalTreeError(ValueError):
    """The confirmed work set cannot form one unambiguous canonical tree."""


@dataclass(frozen=True, order=True)
class WorkIdentity:
    namespace: str
    metadata_id: int

    def __post_init__(self) -> None:
        if self.namespace not in {"tmdb.tv", "tmdb.movie", "tmdb.collection"}:
            raise CanonicalTreeError(f"unsupported identity namespace: {self.namespace}")
        if isinstance(self.metadata_id, bool) or self.metadata_id <= 0:
            raise CanonicalTreeError("metadata id must be a positive integer")


@dataclass(frozen=True)
class CanonicalWork:
    """One already-confirmed sub-plan/version presented to the tree planner."""

    member_key: str
    identity: WorkIdentity
    title: str
    leaf_name: str
    poster_path: str | None = None


@dataclass(frozen=True)
class CanonicalPlacement:
    member_key: str
    identity: WorkIdentity
    target_root: str


@dataclass(frozen=True)
class CanonicalTree:
    container_root: str
    root_identity: WorkIdentity | None
    placements: tuple[CanonicalPlacement, ...]
    identity_roots: tuple[tuple[WorkIdentity, str], ...]
    family_roots: tuple[str, ...]


def _normalize_path(path: str) -> str:
    normalized = posixpath.normpath("/" + str(path).lstrip("/"))
    return normalized.rstrip("/") or "/"


def _title_key(value: str) -> str:
    return "".join(
        character
        for character in unicodedata.normalize("NFKC", value).casefold()
        if character.isalnum()
    )


def _family_title(title: str) -> str | None:
    """Return a bounded, exact-boundary family candidate.

    This is hierarchy evidence only.  It is never returned as a work identity
    and is ignored unless at least two *different confirmed identities* share
    the same candidate/base title inside the same verified batch.
    """
    normalized = unicodedata.normalize("NFC", str(title)).strip()
    boundaries = (
        r"^(.+?)(?:[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]|(?<![A-Za-z])[IVX]{1,4})(?=[：:])",
        r"^(.+?)[：:](?=\S)",
        r"^(.+?)\s+(?=[-–—]{1,2}\S)",
        r"^(.+?)\s+(?=(?:第\s*[一二三四五六七八九十百0-9]+\s*(?:章|部|篇)|"
        r"前篇|后篇|後篇|上篇|下篇|剧场版|劇場版|电影|電影))",
        r"^([^\s]{3,})\s+\S+",
    )
    for pattern in boundaries:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match is None:
            continue
        candidate = match.group(1).strip(" -–—:：")
        cjk_count = len(re.findall(r"[\u3400-\u9fff]", candidate))
        if cjk_count >= 3:
            return candidate
        if ":" in normalized and cjk_count == 0 and len(candidate) >= 6:
            return candidate
    return None


def _canonical_identity_rows(
    works: Iterable[CanonicalWork],
) -> tuple[list[CanonicalWork], dict[WorkIdentity, list[CanonicalWork]]]:
    ordered = sorted(
        works,
        key=lambda row: (
            row.identity,
            _title_key(row.title),
            _title_key(row.leaf_name),
            row.member_key,
        ),
    )
    if not ordered:
        raise CanonicalTreeError("canonical tree requires at least one work")
    keys: set[str] = set()
    grouped: dict[WorkIdentity, list[CanonicalWork]] = {}
    for row in ordered:
        if not row.member_key or row.member_key in keys:
            raise CanonicalTreeError(f"duplicate or empty member key: {row.member_key!r}")
        if not row.title.strip() or not row.leaf_name.strip():
            raise CanonicalTreeError("confirmed work title and leaf name are required")
        if "/" in row.leaf_name or row.leaf_name in {".", ".."}:
            raise CanonicalTreeError(f"leaf name is not a basename: {row.leaf_name!r}")
        keys.add(row.member_key)
        grouped.setdefault(row.identity, []).append(row)
    for identity, members in grouped.items():
        title_keys = {_title_key(row.title) for row in members}
        leaf_keys = {_title_key(row.leaf_name) for row in members}
        if len(title_keys) != 1 or len(leaf_keys) != 1:
            raise CanonicalTreeError(
                f"conflicting canonical metadata for {identity.namespace}/{identity.metadata_id}"
            )
    return ordered, grouped


def plan_canonical_work_tree(
    works: Iterable[CanonicalWork],
    *,
    container_root: str,
    root_identity: WorkIdentity | None = None,
    allow_family_boundaries: bool = True,
) -> CanonicalTree:
    """Plan one deterministic canonical franchise tree.

    ``root_identity`` is explicit structural evidence supplied by the caller:
    for example, the TV id selected for a smart TV plan, or the unique id
    represented by generic season children in a verified batch.  When set,
    that work owns the container root and every other identity remains a
    distinct child leaf.  When absent, the container is directory-only and
    every identity starts as a child leaf; bounded official-title families may
    introduce deeper directory-only containers.
    """
    ordered, grouped = _canonical_identity_rows(works)
    requested_root = _normalize_path(container_root)
    if root_identity is not None and root_identity not in grouped:
        raise CanonicalTreeError(
            f"root identity is not present: {root_identity.namespace}/{root_identity.metadata_id}"
        )

    representative = {identity: rows[0] for identity, rows in grouped.items()}
    if root_identity is not None:
        parent = posixpath.dirname(requested_root) or "/"
        root_label = representative[root_identity].leaf_name
        canonical_root = _normalize_path(posixpath.join(parent, root_label))
    else:
        canonical_root = requested_root

    identity_roots: dict[WorkIdentity, str] = {}
    for identity, row in sorted(representative.items()):
        identity_roots[identity] = (
            canonical_root
            if identity == root_identity
            else _normalize_path(posixpath.join(canonical_root, row.leaf_name))
        )

    family_roots: set[str] = set()
    if root_identity is None and allow_family_boundaries:
        family_candidates = {
            family
            for row in representative.values()
            if (family := _family_title(row.title)) is not None
        }
        assigned: set[WorkIdentity] = set()
        for family in sorted(family_candidates, key=lambda value: (-len(value), _title_key(value))):
            family_key = _title_key(family)
            members = [
                identity
                for identity, row in representative.items()
                if identity not in assigned
                and (
                    _title_key(row.title) == family_key
                    or _title_key(_family_title(row.title) or "") == family_key
                )
            ]
            if len(members) < 2:
                continue
            family_root = (
                canonical_root
                if _title_key(posixpath.basename(canonical_root)) == family_key
                else _normalize_path(posixpath.join(canonical_root, family))
            )
            for identity in members:
                row = representative[identity]
                identity_roots[identity] = (
                    family_root
                    if _title_key(row.title) == family_key
                    else _normalize_path(posixpath.join(family_root, row.leaf_name))
                )
                assigned.add(identity)
            family_roots.add(family_root)

    by_path: dict[str, WorkIdentity] = {}
    for identity, target_root in identity_roots.items():
        key = unicodedata.normalize("NFKC", target_root).casefold()
        previous = by_path.get(key)
        if previous is not None and previous != identity:
            raise CanonicalTreeError(
                "different metadata identities resolve to the same canonical leaf: "
                f"{previous.namespace}/{previous.metadata_id} and "
                f"{identity.namespace}/{identity.metadata_id}: {target_root}"
            )
        by_path[key] = identity

    placements = tuple(
        CanonicalPlacement(row.member_key, row.identity, identity_roots[row.identity])
        for row in ordered
    )
    return CanonicalTree(
        container_root=canonical_root,
        root_identity=root_identity,
        placements=placements,
        identity_roots=tuple(sorted(identity_roots.items())),
        family_roots=tuple(sorted(family_roots, key=lambda path: path.casefold())),
    )


__all__ = [
    "CanonicalPlacement",
    "CanonicalTree",
    "CanonicalTreeError",
    "CanonicalWork",
    "WorkIdentity",
    "plan_canonical_work_tree",
]
