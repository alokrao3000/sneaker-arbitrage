"""Package init — force UTF-8 console streams before anything logs.

On Windows, stdout/stderr default to cp1252 when redirected (piped, logged to
a file, run as a service). Progress messages use characters outside cp1252
('→', '✓'), and the resulting UnicodeEncodeError inside the logging stack has
killed entire scrape runs (scrape_jobs #38: "'charmap' codec can't encode
character '\\u2192'"). Reconfiguring here covers every entry point — uvicorn,
scripts/, one-off shells — because they all import app.* first.
"""
import sys

for _stream in (sys.stdout, sys.stderr):
    try:
        if _stream is not None and _stream.encoding.lower() not in ("utf-8", "utf8"):
            _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass   # non-reconfigurable stream (pytest capture, embedded) — leave it
