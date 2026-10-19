"""Structured logging: one JSON object per line (or key=value text)."""

from __future__ import annotations

import json
import logging
import sys
import time

_STD = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}
# Never let these reach a log line, whatever a caller passes in ``extra``.
_REDACT = {"local_key", "fake_local_key", "key", "real_local_key"}


def _extras(record: logging.LogRecord) -> dict:
    out = {}
    for k, v in vars(record).items():
        if k in _STD or k.startswith("_"):
            continue
        out[k] = "***" if k in _REDACT else v
    return out


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(record.created))
            + f".{int(record.msecs):03d}",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            **_extras(record),
        }
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        ex = " ".join(f"{k}={v}" for k, v in _extras(record).items())
        return f"{base} {ex}" if ex else base


def setup_logging(level: str = "INFO", fmt: str = "json") -> None:
    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(TextFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # tinytuya DEBUG output includes session keys and raw frames: keep it quiet.
    logging.getLogger("tinytuya").setLevel(logging.WARNING)
