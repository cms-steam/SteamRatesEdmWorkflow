"""Combination tables: each distinct combination of fired trigger bits, stored once with its weight.

Idea
----
Per event, a trigger ntuple holds one bit per trigger (HLT path, L1 seed, ...). Many events share
exactly the same combination of fired bits (in HLTPhysics data only ~2% of events have a new HLT
combination). A :class:`PatternTable` therefore stores every distinct combination ("row") once with

    count  number of events with this combination
    sumw   sum of event weights (= count for data; generator weights for MC)
    sumw2  sum of squared weights (for statistical uncertainties)

Every rate-like quantity is a sum over rows, so it can be computed later from the table alone:
the rate of a path, of a dataset (OR of paths), the pure rate, overlaps between any two paths or
datasets, rates with prescales applied, ... New maps or groupings never require re-reading the
ntuples.

Representations
---------------
In memory, a row is the bit vector packed into bytes (``numpy.packbits``), so identical rows can be
found with ``numpy.unique``. On disk (and for computations) the table is stored like a sparse matrix
in CSR ("compressed sparse row") form: ``indices`` lists the fired bits of all rows one after the
other, and ``offsets[i]:offsets[i+1]`` delimits the bits of row ``i``.

Example with bits [HLT_IsoMu24, HLT_Mu50, L1_SingleMu22] and 3 events:
    event 1: IsoMu24 + SingleMu22,  event 2: same,  event 3: IsoMu24 + Mu50 + SingleMu22
    -> rows: {0, 2} x 2,  {0, 1, 2} x 1
    -> indices = [0, 2, 0, 1, 2], offsets = [0, 2, 5], count = [2, 1]
"""

from __future__ import annotations

import numpy as np

_BLOCK = 50_000        # rows unpacked at once when converting (bounds the memory use)


def _void_view(keys):
    """View each row of a 2-D uint8 array as one opaque item, so numpy.unique works on whole rows."""
    keys = np.ascontiguousarray(keys)
    return keys.view(np.dtype((np.void, keys.shape[1])))[:, 0]


def _aggregate(keys, count, sumw, sumw2):
    """Merge identical rows of ``keys`` and sum their weights."""
    if len(keys) == 0:
        return keys, count, sumw, sumw2
    uniq, inverse = np.unique(_void_view(keys), return_inverse=True)
    inverse = inverse.ravel()
    n = len(uniq)
    return (uniq.view(np.uint8).reshape(n, keys.shape[1]),
            np.bincount(inverse, weights=count, minlength=n).astype(np.int64),
            np.bincount(inverse, weights=sumw, minlength=n),
            np.bincount(inverse, weights=sumw2, minlength=n))


class PatternTable:
    """Distinct combinations of fired bits with event counts and weight sums.

    Attributes
    ----------
    bit_names : list of str          names of the bits (columns)
    keys      : uint8 (rows, bytes)  packed bit vectors, one row per distinct combination
    count     : int64 (rows,)        number of events per row
    sumw      : float64 (rows,)      sum of weights per row
    sumw2     : float64 (rows,)      sum of squared weights per row
    """

    def __init__(self, bit_names, keys, count, sumw, sumw2):
        self.bit_names = list(bit_names)
        self.keys = keys
        self.count = count
        self.sumw = sumw
        self.sumw2 = sumw2

    # ---- construction ------------------------------------------------------------------------

    @classmethod
    def empty(cls, bit_names):
        nbytes = (len(bit_names) + 7) // 8
        return cls(bit_names, np.zeros((0, nbytes), np.uint8), np.zeros(0, np.int64),
                   np.zeros(0), np.zeros(0))

    @classmethod
    def from_bits(cls, bit_names, bits, weights=None):
        """Build a table from a (events x bits) bool matrix and optional per-event weights."""
        bits = np.asarray(bits, dtype=bool)
        if bits.shape[1] != len(bit_names):
            raise ValueError("bits matrix and bit_names have different lengths")
        w = np.ones(len(bits)) if weights is None else np.asarray(weights, dtype=np.float64)
        keys = np.packbits(bits, axis=1)
        return cls(bit_names, *_aggregate(keys, np.ones(len(bits), np.int64), w, w * w))

    @classmethod
    def from_csr(cls, bit_names, offsets, indices, count, sumw, sumw2):
        """Inverse of :meth:`to_csr`."""
        n_rows, n_bits = len(offsets) - 1, len(bit_names)
        keys = np.zeros((n_rows, (n_bits + 7) // 8), np.uint8)
        for start in range(0, n_rows, _BLOCK):
            stop = min(start + _BLOCK, n_rows)
            dense = np.zeros((stop - start, n_bits), dtype=bool)
            lo, hi = offsets[start], offsets[stop]
            rows = np.repeat(np.arange(stop - start), np.diff(offsets[start:stop + 1]))
            dense[rows, indices[lo:hi]] = True
            keys[start:stop] = np.packbits(dense, axis=1)
        return cls(bit_names, keys, np.asarray(count, np.int64), np.asarray(sumw, np.float64),
                   np.asarray(sumw2, np.float64))

    # ---- basic properties --------------------------------------------------------------------

    @property
    def n_rows(self):
        return len(self.keys)

    @property
    def n_bits(self):
        return len(self.bit_names)

    def totals(self):
        return {"n_events": int(self.count.sum()), "sumw": float(self.sumw.sum()),
                "sumw2": float(self.sumw2.sum()), "n_rows": self.n_rows}

    # ---- combining tables --------------------------------------------------------------------

    def __add__(self, other):
        """Sum of two tables with identical bit names."""
        if self.bit_names != other.bit_names:
            raise ValueError("tables have different bits; use PatternTable.merge")
        return PatternTable(self.bit_names, *_aggregate(np.concatenate([self.keys, other.keys]),
                                                        np.concatenate([self.count, other.count]),
                                                        np.concatenate([self.sumw, other.sumw]),
                                                        np.concatenate([self.sumw2, other.sumw2])))

    def remap(self, bit_names):
        """The same table expressed with another list of bits (e.g. a union of two menus).

        Bits missing from ``bit_names`` must never have fired; new bits are False everywhere.
        """
        if list(bit_names) == self.bit_names:
            return self
        pos = {n: i for i, n in enumerate(bit_names)}
        offsets, indices = self.to_csr()
        lookup = np.array([pos.get(n, -1) for n in self.bit_names], dtype=np.int64)
        new_idx = lookup[indices]
        if np.any(new_idx < 0):
            lost = sorted({self.bit_names[i] for i in indices[new_idx < 0]})
            raise ValueError(f"remap would drop fired bits: {lost[:5]}...")
        # rows stay distinct under an injective column mapping, so no re-aggregation is needed
        out = PatternTable.from_csr(bit_names, offsets, new_idx, self.count, self.sumw, self.sumw2)
        return PatternTable(bit_names, *_aggregate(out.keys, out.count, out.sumw, out.sumw2))

    def rename(self, func):
        """Rename bits with ``func`` (e.g. strip version suffixes). Names must stay unique."""
        new = [func(n) for n in self.bit_names]
        if len(set(new)) != len(new):
            raise ValueError("renaming makes bit names ambiguous")
        return PatternTable(new, self.keys, self.count, self.sumw, self.sumw2)

    @staticmethod
    def merge(tables):
        """Sum any number of tables; the result uses the union of all bit names (first-seen order)."""
        tables = list(tables)
        if not tables:
            raise ValueError("nothing to merge")
        names = []
        seen = set()
        for t in tables:
            for n in t.bit_names:
                if n not in seen:
                    seen.add(n)
                    names.append(n)
        parts = [t.remap(names) for t in tables]
        return PatternTable(names, *_aggregate(np.concatenate([p.keys for p in parts]),
                                               np.concatenate([p.count for p in parts]),
                                               np.concatenate([p.sumw for p in parts]),
                                               np.concatenate([p.sumw2 for p in parts])))

    # ---- sparse (CSR) view and simple quantities ---------------------------------------------

    def iter_csr(self, block=_BLOCK):
        """Yield the table in blocks of rows: ``(start, stop, offsets, indices)``.

        ``offsets``/``indices`` are the CSR form of rows ``start:stop`` (offsets start at 0). Used
        where the whole CSR form would be too large (MC tables with ~200 fired bits per row).
        """
        for start in range(0, self.n_rows, block):
            dense = np.unpackbits(self.keys[start:start + block], axis=1, count=self.n_bits).astype(bool)
            _, cols = np.nonzero(dense)          # row-major: the bits of each row in increasing order
            offsets = np.zeros(len(dense) + 1, np.int64)
            np.cumsum(dense.sum(axis=1), out=offsets[1:])
            yield start, start + len(dense), offsets, cols.astype(np.int32)

    def to_csr(self):
        """Return ``(offsets, indices)``: the fired bits of row ``i`` are ``indices[offsets[i]:offsets[i+1]]``."""
        idx_parts, per_row = [], []
        for _, _, offsets, indices in self.iter_csr():
            idx_parts.append(indices)
            per_row.append(np.diff(offsets))
        indices = np.concatenate(idx_parts) if idx_parts else np.zeros(0, np.int32)
        per_row = np.concatenate(per_row) if per_row else np.zeros(0, np.int64)
        offsets = np.zeros(self.n_rows + 1, np.int64)
        np.cumsum(per_row, out=offsets[1:])
        return offsets, indices

    def _weights(self, what):
        return {"count": self.count, "sumw": self.sumw, "sumw2": self.sumw2}[what]

    def bit_totals(self, what="count"):
        """Per bit: number of events (or sum of weights) in which the bit fired."""
        offsets, indices = self.to_csr()
        row_of = np.repeat(np.arange(self.n_rows), np.diff(offsets))
        return np.bincount(indices, weights=self._weights(what)[row_of], minlength=self.n_bits)

    def exclusive_totals(self, reference, what="count", non_reference="zero"):
        """Per bit: events where the bit fired and *no other* bit of ``reference`` fired.

        ``reference`` is a bool array over the bits (e.g. all ``HLT_*`` paths). This is the "pure"
        count of the legacy workflow when ``reference`` selects the paths that define purity.
        Bits outside ``reference`` get 0 (``non_reference="zero"``, legacy), or the events where
        they fired and no reference bit fired (``non_reference="exclusive"``, as in
        :mod:`steamrates.counters`).
        """
        reference = np.asarray(reference, dtype=bool)
        offsets, indices = self.to_csr()
        row_of = np.repeat(np.arange(self.n_rows), np.diff(offsets))
        in_ref = reference[indices]
        n_ref_per_row = np.bincount(row_of, weights=in_ref, minlength=self.n_rows)
        sel = in_ref & (n_ref_per_row[row_of] == 1)
        if non_reference == "exclusive":
            sel |= ~in_ref & (n_ref_per_row[row_of] == 0)
        elif non_reference != "zero":
            raise ValueError(f"non_reference must be 'zero' or 'exclusive', not {non_reference!r}")
        return np.bincount(indices[sel], weights=self._weights(what)[row_of[sel]], minlength=self.n_bits)

    def any_totals(self, mask, what="count"):
        """Events (or Σw) in which at least one bit of ``mask`` fired (e.g. the total HLT rate)."""
        mask = np.asarray(mask, dtype=bool)
        offsets, indices = self.to_csr()
        row_of = np.repeat(np.arange(self.n_rows), np.diff(offsets))
        hit = np.bincount(row_of, weights=mask[indices], minlength=self.n_rows) > 0
        return float(self._weights(what)[hit].sum())


class PatternAccumulator:
    """Collects events chunk by chunk; copes with files whose bit lists differ (different menus)."""

    def __init__(self):
        self._tables = {}          # tuple(bit names) -> PatternTable

    def add(self, bit_names, bits, weights=None):
        key = tuple(bit_names)
        new = PatternTable.from_bits(bit_names, bits, weights)
        self._tables[key] = self._tables[key] + new if key in self._tables else new

    def result(self, bit_names_if_empty=()):
        if not self._tables:
            return PatternTable.empty(list(bit_names_if_empty))
        if len(self._tables) == 1:
            return next(iter(self._tables.values()))
        return PatternTable.merge(self._tables.values())
