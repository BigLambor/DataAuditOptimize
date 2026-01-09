#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simple file-based watermark store.

Used to persist the last successful ClickHouse fetch end_time so that the next run
can query incrementally and avoid time-window gaps.
"""

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class WatermarkState:
    """In-file watermark state."""
    last_end_time: datetime
    updated_at: Optional[datetime] = None


class FileWatermarkStore:
    """
    Persist watermark state to a JSON file with atomic replace.

    JSON format:
      {
        "last_end_time": "2026-01-09T12:34:56+08:00",
        "updated_at": "2026-01-09T12:35:01+08:00"
      }
    """

    def __init__(self, path: str):
        self.path = path

    def load(self) -> Optional[WatermarkState]:
        """Load watermark state. Returns None if missing/unreadable."""
        try:
            if not self.path or not os.path.exists(self.path):
                return None
            with open(self.path, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
            last_end = obj.get("last_end_time")
            if not last_end:
                return None
            last_end_dt = datetime.fromisoformat(last_end)
            updated_at = obj.get("updated_at")
            updated_dt = datetime.fromisoformat(updated_at) if updated_at else None
            return WatermarkState(last_end_time=last_end_dt, updated_at=updated_dt)
        except Exception:
            # Intentionally swallow to avoid breaking runs due to state corruption.
            return None

    def save(self, last_end_time: datetime) -> None:
        """Save watermark state (atomic write)."""
        if not self.path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)

        state: Dict[str, Any] = {
            "last_end_time": last_end_time.isoformat(),
            "updated_at": datetime.now(last_end_time.tzinfo).isoformat() if last_end_time.tzinfo else datetime.now().isoformat(),
        }

        dir_name = os.path.dirname(os.path.abspath(self.path)) or "."
        fd, tmp_path = tempfile.mkstemp(prefix=".watermark_", suffix=".json", dir=dir_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.path)
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    def reset(self) -> None:
        """Delete watermark file if present."""
        try:
            if self.path and os.path.exists(self.path):
                os.remove(self.path)
        except Exception:
            pass


