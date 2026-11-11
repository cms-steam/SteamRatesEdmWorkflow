"""Old-style per-bit counters, filled directly in the event loop.

This is the "fixed observables" representation of the legacy workflow: for every trigger bit, the
number of events in which it fired, and its pure count. They are filled next to the combination
table (``steamrates.patterns``) and serve two purposes:

  * **cross-check:** the merge step compares them with the same numbers derived from the table, so
    every measurement shows that the table reproduces the plain event loop;
  * **fallback:** for very large samples (MC where nearly every event is a new combination), a
    counting job can skip the table (``count --no-table``); ``merge --from-counters`` then gives
    path rates, pure rates and totals from the counters alone.

What is counted per bit (``w`` = event weight, 1 for data):

    count, sumw, sumw2               events where the bit fired: number, Σw, Σw²
    pure_count, pure_sumw, pure_sumw2
        reference bits:  the bit fired and no *other* reference bit fired
        other bits:      the bit fired and *no* reference bit fired
    totals[label]                    events where at least one bit matching the label's regex fired

The reference bits are those matching ``pure_reference`` (default ``^HLT_``) and none of the
``exclude`` patterns; excluded bits are also left out of the totals. Unlike the table, these
definitions are fixed when the job runs; they are stored in the partial result next to the numbers.
Patterns are matched against the bit names without version suffix (``HLT_IsoMu24_v3`` ->
``HLT_IsoMu24``) unless ``strip_versions`` is off.
"""

from __future__ import annotations

import re

import numpy as np

from .menu import strip_version

FIELDS = ("count", "sumw", "sumw2", "pure_count", "pure_sumw", "pure_sumw2")
DEFAULT_PURE_REFERENCE = "^HLT_"
DEFAULT_TOTALS = {"HLT": "^HLT_", "L1": "^L1_(?!FinalOR|UnprefireableEvent)"}
_BLOCK = 10_000          # events per block for the weighted sums (bounds the index arrays)


def _column_sums(bits, w):
    """Per column of a bool matrix: (number of True, Σw, Σw²) over the rows where it is True."""
    count = bits.sum(axis=0, dtype=np.int64)
    if w is None:
        return count, count.astype(np.float64), count.astype(np.float64)
    sumw = np.zeros(bits.shape[1])
    sumw2 = np.zeros(bits.shape[1])
    for start in range(0, len(bits), _BLOCK):
        rows, cols = np.nonzero(bits[start:start + _BLOCK])
        ww = w[start:start + _BLOCK][rows]
        sumw += np.bincount(cols, weights=ww, minlength=bits.shape[1])
        sumw2 += np.bincount(cols, weights=ww * ww, minlength=bits.shape[1])
    return count, sumw, sumw2


def _sums(mask, w):
    """(number, Σw, Σw²) over the events selected by ``mask``."""
    n = int(mask.sum())
    if w is None:
        return np.array([n, n, n], dtype=np.float64)
    ww = w[mask]
    return np.array([n, ww.sum(), (ww * ww).sum()])


class BitCounters:
    def __init__(self, pure_reference=DEFAULT_PURE_REFERENCE, exclude=(), totals=None, strip_versions=True):
        self.pure_reference = pure_reference
        self.exclude = list(exclude)
        self.totals_regex = dict(DEFAULT_TOTALS if totals is None else totals)
        self.strip_versions = strip_versions
        self._rx_ref = re.compile(pure_reference)
        self._rx_excl = [re.compile(x) for x in self.exclude]
        self._rx_tot = {label: re.compile(rx) for label, rx in self.totals_regex.items()}
        self.names = []                     # all bits seen, in order of appearance
        self._index = {}
        self._values = {f: np.zeros(0) for f in FIELDS}
        self._totals = {label: np.zeros(3) for label in self.totals_regex}
        self._layouts = {}                  # tuple(bit names of a chunk) -> column info

    # ---- bit bookkeeping --------------------------------------------------------------------------
    def _key(self, name):
        return strip_version(name) if self.strip_versions else name

    def is_excluded(self, name):
        key = self._key(name)
        return any(rx.search(key) for rx in self._rx_excl)

    def is_reference(self, name):
        return bool(self._rx_ref.search(self._key(name))) and not self.is_excluded(name)

    def _layout(self, bit_names):
        """Global indices, reference mask and total masks for the columns of a chunk."""
        key = tuple(bit_names)
        if key not in self._layouts:
            for name in bit_names:
                if name not in self._index:
                    self._index[name] = len(self.names)
                    self.names.append(name)
            n = len(self.names)
            for f in FIELDS:
                self._values[f] = np.concatenate([self._values[f], np.zeros(n - len(self._values[f]))])
            idx = np.array([self._index[b] for b in bit_names], dtype=np.int64)
            ref = np.array([self.is_reference(b) for b in bit_names], dtype=bool)
            excluded = np.array([self.is_excluded(b) for b in bit_names], dtype=bool)
            tot = {label: np.array([bool(rx.search(self._key(b))) for b in bit_names], dtype=bool) & ~excluded
                   for label, rx in self._rx_tot.items()}
            self._layouts[key] = (idx, ref, tot)
        return self._layouts[key]

    # ---- filling ----------------------------------------------------------------------------------
    def add(self, bit_names, bits, weights=None):
        """Add a chunk: ``bits`` is a bool matrix (events x bits), ``weights`` per event or None."""
        if len(bits) == 0:
            return
        idx, ref, tot = self._layout(bit_names)
        w = None if weights is None else np.asarray(weights, dtype=np.float64)
        v = self._values

        count, sumw, sumw2 = _column_sums(bits, w)
        v["count"][idx] += count
        v["sumw"][idx] += sumw
        v["sumw2"][idx] += sumw2

        # pure: reference bits in events with exactly one reference bit; other bits in events
        # without any reference bit
        n_ref = bits[:, ref].sum(axis=1)
        for sel_events, sel_bits in ((n_ref == 1, ref), (n_ref == 0, ~ref)):
            if not sel_events.any() or not sel_bits.any():
                continue
            sub = bits[sel_events][:, sel_bits]
            c, s, s2 = _column_sums(sub, None if w is None else w[sel_events])
            cols = idx[sel_bits]
            v["pure_count"][cols] += c
            v["pure_sumw"][cols] += s
            v["pure_sumw2"][cols] += s2

        for label, mask in tot.items():
            if mask.any():
                self._totals[label] += _sums(bits[:, mask].any(axis=1), w)

    # ---- results ----------------------------------------------------------------------------------
    def arrays(self, bit_names=None):
        """Counter arrays in the order of ``bit_names`` (default: order of appearance)."""
        if bit_names is None:
            bit_names = self.names
        missing = [b for b in bit_names if b not in self._index]
        extra = sorted(set(self.names) - set(bit_names))
        if extra:
            raise ValueError(f"counters have bits that are not in the requested list: {extra[:5]}")
        out = {}
        for f in FIELDS:
            arr = np.zeros(len(bit_names), dtype=np.int64 if f.endswith("count") else np.float64)
            for i, b in enumerate(bit_names):
                if b not in missing:
                    arr[i] = self._values[f][self._index[b]]
            out[f] = arr
        return out

    def definition(self):
        """How the counters were defined, plus the totals (stored in the partial's metadata)."""
        return {"pure_reference": self.pure_reference, "exclude": self.exclude,
                "strip_versions": self.strip_versions,
                "totals": {label: {"regex": self.totals_regex[label], "count": int(t[0]), "sumw": float(t[1]),
                                   "sumw2": float(t[2])} for label, t in self._totals.items()}}
