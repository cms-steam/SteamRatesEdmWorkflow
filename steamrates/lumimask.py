"""Selection of certified lumi sections from a CMS "golden" JSON file.

The JSON maps run numbers to lists of inclusive lumi-section ranges::

    {"402360": [[1, 120], [125, 142]], ...}

:class:`LumiMask` answers "is (run, lumi) inside the JSON?" for whole numpy arrays at once.
(The legacy script re-opened and re-parsed the JSON file for every single event.)
"""

from __future__ import annotations

import hashlib
import json

import numpy as np


class LumiMask:
    def __init__(self, ranges, source=None, sha1=None):
        # ranges: {run: [(first, last), ...]}
        self.ranges = {int(run): [(int(a), int(b)) for a, b in rr] for run, rr in ranges.items()}
        self.source = source
        self.sha1 = sha1
        # flattened, sorted arrays for vectorized lookups
        rows = sorted((run, a, b) for run, rr in self.ranges.items() for a, b in rr)
        self._run = np.array([r[0] for r in rows], dtype=np.int64)
        self._first = np.array([r[1] for r in rows], dtype=np.int64)
        self._last = np.array([r[2] for r in rows], dtype=np.int64)

    @classmethod
    def from_file(cls, path):
        with open(path, "rb") as f:
            raw = f.read()
        return cls(json.loads(raw), source=path, sha1=hashlib.sha1(raw).hexdigest())

    def __call__(self, run, lumi):
        """Boolean array: True where (run[i], lumi[i]) is certified."""
        run = np.asarray(run, dtype=np.int64)
        lumi = np.asarray(lumi, dtype=np.int64)
        if len(self._run) == 0:
            return np.zeros(len(run), dtype=bool)
        # Encode (run, lumi) as one sortable key and find the last range starting at or before it.
        key = run * (1 << 32) + lumi
        start_key = self._run * (1 << 32) + self._first
        i = np.searchsorted(start_key, key, side="right") - 1
        valid = i >= 0
        i = np.clip(i, 0, None)
        return valid & (self._run[i] == run) & (lumi <= self._last[i])

    def to_dict(self):
        return {"source": self.source, "sha1": self.sha1, "runs": sorted(self.ranges)}

    def to_json(self):
        """The ranges in the JSON-file layout ``{"run": [[first, last], ...]}``."""
        return {str(run): [[a, b] for a, b in rr] for run, rr in sorted(self.ranges.items())}

    @classmethod
    def from_json(cls, ranges, source=None):
        """From the JSON-file layout; ``sha1`` is computed from the canonical JSON text."""
        mask = cls(ranges, source=source)
        mask.sha1 = hashlib.sha1(json.dumps(mask.to_json(), sort_keys=True).encode()).hexdigest()
        return mask

    def intersection(self, other):
        """Mask with the lumi sections that are in both ``self`` and ``other``."""
        out = {}
        for run in sorted(set(self.ranges) & set(other.ranges)):
            pieces = sorted((max(a1, a2), min(b1, b2)) for a1, b1 in self.ranges[run]
                            for a2, b2 in other.ranges[run] if max(a1, a2) <= min(b1, b2))
            merged = []
            for a, b in pieces:                      # merge touching/overlapping ranges
                if merged and a <= merged[-1][1] + 1:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], b))
                else:
                    merged.append((a, b))
            if merged:
                out[run] = merged
        return LumiMask.from_json(out, source=f"{self.source} & {other.source}")

    def n_lumis(self):
        return sum(b - a + 1 for rr in self.ranges.values() for a, b in rr)
