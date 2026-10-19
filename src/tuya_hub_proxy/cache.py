"""Per-child DP cache, persisted to disk so state survives a restart.

Zigbee sensors sleep: the hub often only reports a DP when it changes, and a
fresh ``status()`` may return a partial set (or nothing).  The cache therefore
never drops a value it has seen; it only merges new values on top.

``defaults`` are DPs announced to the client when the hub has never reported
them (e.g. a door sensor that only exposed its battery DP so far).  They are
*not* written to disk, so a real value always wins as soon as one arrives.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class ChildState:
    dps: dict[str, Any] = field(default_factory=dict)
    defaults: dict[str, Any] = field(default_factory=dict)
    updated_at: float = 0.0  # last time any DP arrived from the hub
    last_push: float = 0.0  # last spontaneous push from the hub

    def merged(self) -> dict[str, Any]:
        return {**self.defaults, **self.dps}


class DpsCache:
    def __init__(self, path: str | os.PathLike | None, save_delay: float = 2.0) -> None:
        self._path = Path(path) if path else None
        self._children: dict[str, ChildState] = {}
        self._dirty = False
        self._last_save = 0.0
        self._save_delay = save_delay

    # -- setup -----------------------------------------------------------
    def register(self, child_id: str, defaults: dict[str, Any] | None = None) -> None:
        st = self._children.setdefault(child_id, ChildState())
        st.defaults = {str(k): v for k, v in (defaults or {}).items()}

    def load(self) -> None:
        if not self._path or not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("cache file unreadable, starting empty: %s", exc)
            return
        for cid, entry in raw.get("children", {}).items():
            if cid not in self._children:
                continue  # child removed from config
            st = self._children[cid]
            st.dps = {str(k): v for k, v in entry.get("dps", {}).items()}
            st.updated_at = float(entry.get("updated_at", 0))
            st.last_push = float(entry.get("last_push", 0))

    # -- access ----------------------------------------------------------
    def get(self, child_id: str) -> ChildState:
        return self._children[child_id]

    def dps(self, child_id: str) -> dict[str, Any]:
        return self._children[child_id].merged()

    def update(self, child_id: str, dps: dict[str, Any], push: bool = False) -> dict[str, Any]:
        """Merge ``dps``; return only the DPs whose value actually changed."""
        st = self._children[child_id]
        now = time.time()
        changed = {}
        for k, v in dps.items():
            k = str(k)
            if k not in st.dps or st.dps[k] != v:
                changed[k] = v
            st.dps[k] = v
        st.updated_at = now
        if push:
            st.last_push = now
        if changed:
            self._dirty = True
        return changed

    def children(self) -> dict[str, ChildState]:
        return self._children

    # -- persistence -----------------------------------------------------
    def maybe_save(self, force: bool = False) -> None:
        if not self._path or not self._dirty:
            return
        if not force and time.time() - self._last_save < self._save_delay:
            return
        data = {
            "version": 1,
            "children": {
                cid: {"dps": st.dps, "updated_at": st.updated_at, "last_push": st.last_push}
                for cid, st in self._children.items()
            },
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self._path.parent, prefix=".cache-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(data, fh, indent=1, sort_keys=True)
            os.replace(tmp, self._path)
        except OSError as exc:
            log.warning("could not persist cache: %s", exc)
            try:
                os.unlink(tmp)
            except OSError:
                pass
            return
        self._dirty = False
        self._last_save = time.time()
