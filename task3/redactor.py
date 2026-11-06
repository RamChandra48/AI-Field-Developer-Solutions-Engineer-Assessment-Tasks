"""Incremental PII redaction over a stream of text chunks.

The difficulty with streaming redaction is that PII can straddle chunk
boundaries: "jane.d" + "oe@exam" + "ple.com". Emitting each chunk as it
arrives leaks the start of the email; buffering the whole response destroys
time-to-first-token.

StreamRedactor holds back the smallest suffix that could still turn into PII,
and nothing more. It uses the third-party ``regex`` module's partial matching
(``fullmatch(text, pos, partial=True)``), which reports whether the text from
``pos`` onwards is a *prefix* of a possible match. So:

    feed("Hello wor")  -> "Hello "       ("wor" could start an email; held)
    feed("ld. Mail ")  -> "world. Mail "
    feed("jane.d")     -> ""             (held)
    feed("oe@x.com ")  -> "[REDACTED] "

Properties:
  * Output is identical however the input is chunked (tests check this over
    thousands of random splits).
  * Held-back text is bounded by the longest possible match (about 700 chars
    for the email pattern, 23 for cards, 11 for SSNs). For ordinary prose it
    is the current word, so the stream lags by roughly one token. Memory per
    stream is O(longest match), not O(response).
  * Each feed scans only the held suffix plus the new chunk, so total work is
    linear in the response length.
"""
from __future__ import annotations

from collections import Counter
from typing import Callable, Dict, Optional

import regex

REPLACEMENT = "[REDACTED]"


def luhn_valid(candidate: str) -> bool:
    """Luhn checksum, used to avoid redacting arbitrary 13-19 digit numbers."""
    digits = [int(c) for c in candidate if c.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False
    total = 0
    for i, digit in enumerate(reversed(digits)):
        if i % 2:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0


# Every quantifier is bounded, so a partial match (and therefore the held-back
# buffer) can never grow without limit. \d is Unicode-aware, so full-width
# digits ("１２３-４５-６７８９") are caught too.
#
# Separators are any Unicode space (\p{Zs}) or dash (\p{Pd}, plus U+2212 minus),
# not just ASCII " " and "-". LLMs really do emit typographic characters: a
# Groq gpt-oss reply wrote a card as "4111 1111 1111 1111"
# (narrow no-break spaces), which an ASCII-only pattern let through. Likewise
# U+2011 non-breaking hyphens in SSNs, and the full-width "＠" (U+FF20) or small
# "﹫" (U+FE6B) in emails.
SEP = r"[\p{Zs}\p{Pd}−]"
AT = r"[@＠﹫]"
PII_PATTERN = regex.compile(
    r"""
      (?P<credit_card> (?<!\d) (?:\d SEP?){12,18} \d (?!\d) )
    | (?P<ssn>         (?<!\d) \d{3} SEP \d{2} SEP \d{4} (?!\d) )
    | (?P<email>       [A-Za-z0-9._%+-]{1,64} AT [A-Za-z0-9-]{1,63} (?:\.[A-Za-z0-9-]{1,63}){0,8}
                       \.[A-Za-z]{2,24} (?![A-Za-z0-9-]) )
    """.replace("SEP", SEP).replace("AT", AT),
    regex.VERBOSE,
)

Validator = Callable[[str], bool]
DEFAULT_VALIDATORS: Dict[str, Validator] = {"credit_card": luhn_valid}


class StreamRedactor:
    """Redacts PII from one text stream. Not thread-safe; use one per stream."""

    def __init__(
        self,
        pattern: "regex.Pattern[str]" = PII_PATTERN,
        replacement: str = REPLACEMENT,
        validators: Optional[Dict[str, Validator]] = None,
        on_redact: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self._pattern = pattern
        self._replacement = replacement
        self._validators = DEFAULT_VALIDATORS if validators is None else validators
        self._on_redact = on_redact  # optional (kind, original_text) hook, for tracing/demos
        self._pending = ""  # held back: may be the start of PII
        self._context = ""  # last emitted char, so lookbehinds work across chunks
        self.redactions: Counter = Counter()

    @property
    def pending(self) -> str:
        """Text received but not yet released."""
        return self._pending

    def feed(self, chunk: str) -> str:
        """Add a chunk; return the text that is now safe to emit."""
        self._pending += chunk
        return self._drain(final=False)

    def flush(self) -> str:
        """End of stream: release everything that is left, redacted."""
        return self._drain(final=True)

    def _open_from(self, text: str, pos: int) -> int:
        """Leftmost index >= pos whose suffix may still be, or grow into, a match.

        ``fullmatch(text, q, partial=True)`` asks: is ``text[q:]`` a prefix of
        some match? Anything before the returned index is settled: every
        regex path from there fails or completes inside the text we already
        have, so more input cannot change it. Most positions fail on their
        first character, so this is cheap.
        """
        fullmatch = self._pattern.fullmatch
        for q in range(pos, len(text)):
            if fullmatch(text, q, partial=True):
                return q
        return len(text)

    def _drain(self, final: bool) -> str:
        text = self._context + self._pending
        end = len(text)
        pos = len(self._context)
        # Everything from `limit` on is held back. (Not search(partial=True):
        # that returns a complete match in preference to an earlier partial one,
        # which would release the start of an email that is still arriving.)
        limit = end if final else self._open_from(text, pos)
        out = []
        while True:
            m = self._pattern.search(text, pos)  # complete matches only
            if m is None or m.start() >= limit:
                break
            kind = m.lastgroup or ""
            validator = self._validators.get(kind)
            if validator is not None and not validator(m.group()):
                # Looks like PII but isn't (e.g. fails Luhn). Release one char
                # and rescan, since another pattern may start inside it.
                out.append(text[pos:m.start() + 1])
                pos = m.start() + 1
                continue
            out.append(text[pos:m.start()])
            out.append(self._replacement)
            self.redactions[kind] += 1
            if self._on_redact is not None:
                self._on_redact(kind, m.group())
            pos = m.end()
            if pos > limit:  # a settled match swallowed the held region
                limit = end if final else self._open_from(text, pos)

        out.append(text[pos:limit])
        self._pending = text[limit:]
        self._context = text[limit - 1:limit] if limit else ""
        return "".join(out)


def redact_text(text: str) -> str:
    """Redact a complete string (non-streaming convenience)."""
    redactor = StreamRedactor()
    return redactor.feed(text) + redactor.flush()
