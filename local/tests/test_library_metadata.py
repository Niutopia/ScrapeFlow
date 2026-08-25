"""Coverage for the bounded NFO identity reader used by the D-step index.

``read_nfo_identity`` reads one remote sidecar per formal work root while the
library index is built.  It is deliberately fail-closed: an NFO is untrusted
remote input, so anything ambiguous, oversized, entity-bearing, malformed, or
carrying more than one TMDB id yields ``None`` instead of a guessed identity.
"""

from __future__ import annotations

import unittest

from local.scrapeflow_api.library_metadata import read_nfo_identity


class RecordingReader:
    """Minimal stand-in exposing only the ``read_file_bytes`` surface."""

    def __init__(self, payload: object, *, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error
        self.calls: list[tuple[str, int]] = []

    def read_file_bytes(self, path: str, *, max_bytes: int) -> object:
        self.calls.append((path, max_bytes))
        if self.error is not None:
            raise self.error
        return self.payload


def _nfo(body: str) -> bytes:
    return body.encode("utf-8")


TVSHOW = _nfo(
    "<tvshow>"
    "<title>  葬送的\t芙莉莲 </title>"
    "<originaltitle>Sousou no Frieren</originaltitle>"
    "<premiered>2023-09-29</premiered>"
    "<tmdbid>209867</tmdbid>"
    "</tvshow>"
)

MOVIE = _nfo(
    "<movie>"
    "<title>紫罗兰永恒花园 剧场版</title>"
    "<year>2020</year>"
    '<uniqueid type="tmdb">533514</uniqueid>'
    '<uniqueid type="imdb">tt8652818</uniqueid>'
    "</movie>"
)


class ReadNfoIdentityTests(unittest.TestCase):
    def test_tvshow_nfo_yields_tv_identity_with_compacted_text(self) -> None:
        reader = RecordingReader(TVSHOW)
        result = read_nfo_identity(reader, "/library/番剧/葬送的芙莉莲/tvshow.nfo")
        self.assertEqual(result, {
            "tmdb_id": 209867,
            "media_type": "tv",
            "title": "葬送的 芙莉莲",
            "original_title": "Sousou no Frieren",
            "year": "2023",
        })
        self.assertEqual(
            reader.calls, [("/library/番剧/葬送的芙莉莲/tvshow.nfo", 1024 * 1024)],
        )

    def test_movie_nfo_reads_tmdb_uniqueid_and_ignores_other_providers(self) -> None:
        result = read_nfo_identity(RecordingReader(MOVIE), "/library/番剧/x/movie.nfo")
        assert result is not None
        self.assertEqual(result["media_type"], "movie")
        self.assertEqual(result["tmdb_id"], 533514)
        self.assertEqual(result["year"], "2020")
        self.assertIsNone(result["original_title"])

    def test_same_id_in_both_tmdbid_and_uniqueid_is_not_a_conflict(self) -> None:
        payload = _nfo(
            "<tvshow><title>A</title><tmdbid>42</tmdbid>"
            '<uniqueid type="TMDB">42</uniqueid></tvshow>'
        )
        result = read_nfo_identity(RecordingReader(payload), "/x/tvshow.nfo")
        assert result is not None
        self.assertEqual(result["tmdb_id"], 42)

    def test_two_different_tmdb_ids_fail_closed(self) -> None:
        payload = _nfo(
            "<tvshow><title>A</title><tmdbid>42</tmdbid>"
            '<uniqueid type="tmdb">997317</uniqueid></tvshow>'
        )
        self.assertIsNone(read_nfo_identity(RecordingReader(payload), "/x/tvshow.nfo"))

    def test_missing_or_non_positive_tmdb_id_fails_closed(self) -> None:
        for body in (
            "<tvshow><title>A</title></tvshow>",
            "<tvshow><title>A</title><tmdbid>0</tmdbid></tvshow>",
            "<tvshow><title>A</title><tmdbid>-7</tmdbid></tvshow>",
            "<tvshow><title>A</title><tmdbid>12x</tmdbid></tvshow>",
        ):
            with self.subTest(body=body):
                self.assertIsNone(
                    read_nfo_identity(RecordingReader(_nfo(body)), "/x/tvshow.nfo"),
                )

    def test_unknown_root_tag_is_not_an_identity(self) -> None:
        for body in (
            "<episodedetails><tmdbid>42</tmdbid></episodedetails>",
            "<musicvideo><tmdbid>42</tmdbid></musicvideo>",
        ):
            with self.subTest(body=body):
                self.assertIsNone(
                    read_nfo_identity(RecordingReader(_nfo(body)), "/x/a.nfo"),
                )

    def test_namespaced_root_and_children_still_resolve(self) -> None:
        payload = _nfo(
            '<tvshow xmlns="urn:kodi:nfo"><title>B</title>'
            "<tmdbid>1399</tmdbid></tvshow>"
        )
        result = read_nfo_identity(RecordingReader(payload), "/x/tvshow.nfo")
        assert result is not None
        self.assertEqual((result["media_type"], result["tmdb_id"]), ("tv", 1399))

    def test_doctype_and_entity_payloads_are_rejected_before_parsing(self) -> None:
        for body in (
            '<!DOCTYPE tvshow [<!ENTITY x "y">]><tvshow><tmdbid>42</tmdbid></tvshow>',
            '<!doctype tvshow><tvshow><tmdbid>42</tmdbid></tvshow>',
            '<!ENTITY leak SYSTEM "file:///etc/passwd">'
            "<tvshow><tmdbid>42</tmdbid></tvshow>",
        ):
            with self.subTest(body=body[:40]):
                self.assertIsNone(
                    read_nfo_identity(RecordingReader(_nfo(body)), "/x/tvshow.nfo"),
                )

    def test_malformed_xml_and_non_bytes_payloads_fail_closed(self) -> None:
        for payload in (
            _nfo("<tvshow><tmdbid>42</tmdbid>"),
            _nfo("not xml at all"),
            b"",
            "<tvshow><tmdbid>42</tmdbid></tvshow>",
            None,
            {"tmdbid": 42},
        ):
            with self.subTest(payload=repr(payload)[:40]):
                self.assertIsNone(
                    read_nfo_identity(RecordingReader(payload), "/x/tvshow.nfo"),
                )

    def test_payload_over_the_byte_ceiling_is_rejected(self) -> None:
        oversized = b"<tvshow><tmdbid>42</tmdbid></tvshow>" + b" " * (1024 * 1024)
        self.assertIsNone(
            read_nfo_identity(RecordingReader(oversized), "/x/tvshow.nfo"),
        )

    def test_reader_failures_and_missing_reader_surface_fail_closed(self) -> None:
        self.assertIsNone(read_nfo_identity(None, "/x/tvshow.nfo"))
        self.assertIsNone(read_nfo_identity(object(), "/x/tvshow.nfo"))
        self.assertIsNone(
            read_nfo_identity(
                RecordingReader(TVSHOW, error=OSError("远端不可读")),
                "/x/tvshow.nfo",
            ),
        )

    def test_control_characters_reject_the_whole_nfo(self) -> None:
        """A raw control byte or an invalid character reference is fail-closed.

        Neither form is well-formed XML, so the document never reaches the
        field-level compaction guard; the reader returns ``None`` for the whole
        sidecar rather than an identity with one blanked field.
        """
        for body in (
            "<tvshow><title>bad\x07title</title><tmdbid>77</tmdbid></tvshow>",
            "<tvshow><title>bad&#7;title</title><tmdbid>77</tmdbid></tvshow>",
        ):
            with self.subTest(body=body[:32]):
                self.assertIsNone(
                    read_nfo_identity(RecordingReader(_nfo(body)), "/x/tvshow.nfo"),
                )

    def test_empty_and_whitespace_only_fields_are_dropped(self) -> None:
        payload = _nfo(
            "<tvshow><title>   </title><originaltitle>Good</originaltitle>"
            "<tmdbid>77</tmdbid></tvshow>"
        )
        result = read_nfo_identity(RecordingReader(payload), "/x/tvshow.nfo")
        assert result is not None
        self.assertIsNone(result["title"])
        self.assertEqual(result["original_title"], "Good")

    def test_name_falls_back_for_title_and_year_comes_from_release_date(self) -> None:
        payload = _nfo(
            "<movie><name>Fallback</name><releasedate>1998-04-01</releasedate>"
            "<tmdbid>5</tmdbid></movie>"
        )
        result = read_nfo_identity(RecordingReader(payload), "/x/movie.nfo")
        assert result is not None
        self.assertEqual((result["title"], result["year"]), ("Fallback", "1998"))

    def test_implausible_year_text_yields_no_year(self) -> None:
        payload = _nfo(
            "<movie><title>C</title><year>不明</year><tmdbid>6</tmdbid></movie>"
        )
        result = read_nfo_identity(RecordingReader(payload), "/x/movie.nfo")
        assert result is not None
        self.assertIsNone(result["year"])

    def test_long_title_is_truncated_to_the_bounded_length(self) -> None:
        payload = _nfo(
            f"<movie><title>{'长' * 400}</title><tmdbid>7</tmdbid></movie>"
        )
        result = read_nfo_identity(RecordingReader(payload), "/x/movie.nfo")
        assert result is not None
        self.assertEqual(len(str(result["title"])), 240)


if __name__ == "__main__":
    unittest.main()
