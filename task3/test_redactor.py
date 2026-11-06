"""Tests for the incremental redactor (no network)."""
from __future__ import annotations

import random
import time

import pytest

from redactor import REPLACEMENT, StreamRedactor, luhn_valid, redact_text

R = REPLACEMENT


def stream(chunks):
    """Feed chunks one by one; return (output, list of emitted pieces, max held)."""
    redactor = StreamRedactor()
    pieces, max_held = [], 0
    for chunk in chunks:
        pieces.append(redactor.feed(chunk))
        max_held = max(max_held, len(redactor.pending))
    pieces.append(redactor.flush())
    return "".join(pieces), pieces, max_held


def random_chunks(text, rng, max_size=8):
    out, i = [], 0
    while i < len(text):
        n = rng.randint(1, max_size)
        out.append(text[i:i + n])
        i += n
    return out


class TestPatterns:
    @pytest.mark.parametrize("text, expected", [
        ("mail jane.doe@example.com now", "mail %s now" % R),
        ("mail JANE+tag@mail.example.co.uk.", "mail %s." % R),
        ("ssn 123-45-6789.", "ssn %s." % R),
        ("card 4111 1111 1111 1111 ok", "card %s ok" % R),
        ("card 4111-1111-1111-1111", "card %s" % R),
        ("card 4111111111111111", "card %s" % R),
        ("amex 3782 822463 10005", "amex %s" % R),
        ("full-width １２３-４５-６７８９", "full-width %s" % R),
        ("a@b.co, 123-45-6789 and 5555555555554444", "%s, %s and %s" % (R, R, R)),
    ])
    def test_redacts(self, text, expected):
        assert redact_text(text) == expected

    @pytest.mark.parametrize("text", [
        "meet at 5pm on 2024-01-15",
        "version 1.2.3, build 4567",
        "phone 555-1234",
        "1234 5678 9012 3456 is an order number",   # fails Luhn
        "1234-56-78901",                            # not SSN shaped
        "9123-45-6789",                             # SSN preceded by a digit
        "user at example dot com",
        "@handle and a@b",
    ])
    def test_leaves_non_pii_alone(self, text):
        assert redact_text(text) == text

    # Typographic separators that LLMs really emit. The first case is verbatim
    # from a Groq gpt-oss-120b reply, which an ASCII-only pattern let through.
    @pytest.mark.parametrize("text", [
        "card 4111 1111 1111 1111",   # narrow no-break space (seen from gpt-oss)
        "card 4111 1111 1111 1111",   # no-break space
        "card 4111 1111 1111 1111",   # thin space
        "card 4111–1111–1111–1111",   # en dash
        "ssn 123‑45‑6789",                 # non-breaking hyphen
        "ssn 123−45−6789",                 # minus sign
        "ssn 123 45 6789",
        "ssn 123 45 6789",
        "mail jane＠example.com",                # full-width at sign
        "mail jane﹫example.com",                # small at sign
    ])
    def test_redacts_unicode_separators(self, text):
        label = text.split(" ", 1)[0]
        assert redact_text(text) == "%s %s" % (label, R)

    def test_unicode_separators_split_anywhere(self):
        text = "x 4111 1111 1111 1111 y 123‑45‑6789 z jane＠example.com."
        expected = "x %s y %s z %s." % (R, R, R)
        for i in range(1, len(text)):
            assert stream([text[:i], text[i:]])[0] == expected, i

    def test_luhn(self):
        assert luhn_valid("4111 1111 1111 1111")
        assert not luhn_valid("4111 1111 1111 1112")
        assert not luhn_valid("1234")

    def test_counts_by_type(self):
        r = StreamRedactor()
        r.feed("a@b.com 123-45-6789 x@y.org 4111111111111111 ")
        r.flush()
        assert r.redactions == {"email": 2, "ssn": 1, "credit_card": 1}


class TestStreaming:
    CORPUS = [
        "Sure! Reach Jane at jane.doe@example.com. SSN 123-45-6789, card 4111 1111 1111 1111. Bye.",
        "Numbers 1234 5678 9012 3456 and 4111-1111-1111-1111 and 9123-45-6789 and 123-45-67890.",
        "Edge a@b.co then a@b.com.au, x.y+z@sub.domain.example.org! 5555555555554444",
        "ref 12345678901234567-45-6789 end; mail@ex 123-4 a@b",
        "Plain prose with no secrets at all, just words and punctuation; nothing to hide.",
    ]

    @pytest.mark.parametrize("text", CORPUS)
    def test_output_is_independent_of_chunking(self, text):
        expected = redact_text(text)
        rng = random.Random(1234)
        for _ in range(500):
            output, _, _ = stream(random_chunks(text, rng))
            assert output == expected
        assert stream(list(text))[0] == expected  # one character at a time

    def test_fuzz_chunking_invariance(self):
        """Random texts built from PII, near-misses and separators, randomly split.

        This caught two real bugs: regex preferring a shorter complete match
        to a longer partial one, and a complete match elsewhere hiding an
        earlier in-progress email.
        """
        frags = ["a@b.co", "a@b.com.au", "x.y+z@sub.domain.example.org", "jane@exa", "@", "123-45-6789",
                 "123-45-67890", "9123-45-6789", "4111 1111 1111 1111", "4111-1111-1111-1112",
                 "5555555555554444", "1234", "-", ".", " ", ", ", "word", "7", "00", "１２３-４５-６７８９", "\n"]
        rng = random.Random(2026)
        for _ in range(3000):
            text = "".join(rng.choice(frags) for _ in range(rng.randint(1, 14)))
            expected = redact_text(text)
            for _ in range(3):
                assert stream(random_chunks(text, rng, max_size=6))[0] == expected, text

    def test_no_partial_pii_is_ever_emitted(self):
        secrets = ["jane.doe@example.com", "123-45-6789", "4111 1111 1111 1111"]
        text = "x " + " y ".join(secrets) + " z"
        redactor, emitted = StreamRedactor(), ""
        for ch in text:
            emitted += redactor.feed(ch)
            for secret in secrets:
                # not even a 3-char prefix of a secret may escape
                assert secret[:3] not in emitted.replace(R, ""), emitted
        emitted += redactor.flush()
        assert emitted == "x %s y %s y %s z" % (R, R, R)

    def test_prose_lags_by_at_most_one_word(self):
        r = StreamRedactor()
        assert r.feed("Hello") == ""            # could be the start of hello@x.com
        assert r.feed(" wor") == "Hello "
        assert r.feed("ld, ") == "world, "
        assert r.pending == ""

    def test_complete_match_that_can_grow_is_held(self):
        r = StreamRedactor()
        assert r.feed("mail a@b.co") == "mail "
        assert r.feed("m.au ") == R + " "        # one redaction, not "[REDACTED]m.au"
        assert r.redactions["email"] == 1

    def test_ssn_followed_by_digit_in_next_chunk_is_not_ssn(self):
        r = StreamRedactor()
        out = r.feed("id 123-45-6789") + r.feed("0 ok") + r.flush()
        assert out == "id 123-45-67890 ok"

    def test_flush_releases_unfinished_non_pii(self):
        r = StreamRedactor()
        assert r.feed("contact jane@exa") == "contact "
        assert r.flush() == "jane@exa"

    def test_held_buffer_is_bounded(self):
        _, _, max_held = stream(["a"] * 5000)     # one endless "word"
        assert max_held <= 64                      # email local part limit
        _, _, max_held = stream(["7"] * 5000)      # endless digits
        assert max_held <= 64

    def test_long_stream_is_linear_time(self):
        text = ("The quick brown fox emails fox@example.com about 123-45-6789. " * 20000)  # ~1.2 MB
        chunks = [text[i:i + 16] for i in range(0, len(text), 16)]
        start = time.perf_counter()
        output, _, max_held = stream(chunks)
        elapsed = time.perf_counter() - start
        assert output.count(R) == 40000 and "fox@example.com" not in output
        assert max_held < 64
        assert elapsed < 10, elapsed  # ~1-2 s on a laptop
