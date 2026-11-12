"""Rates from combination tables: per-path, dataset and stream sums, pure sums, totals and overlaps.

Every rate-like quantity is a sum over the rows of the combination table (steamrates.patterns), so
the tables of a measurement can be added one at a time, and each table in blocks of rows: memory is
bounded by one block plus the accumulators, however large the sample (a future web server will have
little memory). The accumulators hold three numbers per entry, the "W columns"::

    count   number of events          (to judge the statistics)
    sumw    sum of event weights      (rate = sumw x scale, steamrates.normalize)
    sumw2   sum of squared weights    (rate uncertainty = sqrt(sumw2) x scale)

Linear algebra
--------------
A block of rows is a sparse 0/1 matrix ``X`` (rows x bits) plus the W columns (rows x 3).

    path p fired in row r         X[r, p]
    combined dataset c fired      (X @ M_cds)[r, c] > 0      M_cds: bits x combined datasets (member paths)
    combined stream c fired       (X @ M_cst)[r, c] > 0      M_cst: bits x combined streams
    split dataset d fired         P[r, d] = (X @ M_ds)[r, d] > 0 (its paths), AND its Dataset_ bit
                                  if the ntuples have it (round robin, dataset prescale)
    split stream s fired          (split datasets @ M_st)[r, s] > 0
    sums per item                 A.T @ W                    A = X or one of the decision matrices
    pure sums, reference group g  A.T @ (W * [n_g == 1])     n_g[r] = members of g fired in row r
                                  (only for items whose reference group is g; bits, combined items)
    shared with standard+parking  A.T @ (W * [any standard/parking path fired])
    total over a set of paths     sum of W over rows where at least one of them fired
    overlap matrix A x B          A.T @ diag(w) @ B          (one per W column; bits, combined items)

Scaled by the normalization these give rates, uncertainties, pure rates, shared rates and overlap
rates. The maps (which bit is in which dataset/stream/reference group) come from steamrates.maps.
Split datasets and streams get only their own rates (steamrates.results adds estimates for those
without a Dataset_ bit in the ntuples).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import numpy as np
import scipy.sparse as sp

from .maps import CATEGORIES, HLT_REFERENCES, PHYSICS_CATEGORIES, l1_reference

W_FIELDS = ("count", "sumw", "sumw2")
LEVELS = ("bits", "datasets", "streams")              # with pure and shared sums (combined datasets/streams)
SPLIT_LEVELS = ("datasets_split", "streams_split")    # own rates only
DEFAULT_BLOCK = 20_000          # rows per block (bounds the size of the sparse matrices)


@dataclass
class Total:
    """Events in which, for every mask in ``all_of``, at least one bit of that mask fired.

    ``all_of = []`` selects every event (the input of the HLT). One mask = an OR over its bits;
    two masks = events that fired both sets (e.g. scouting AND standard+parking).
    """
    label: str
    description: str
    all_of: list


@dataclass
class Overlap:
    """Overlap matrix between items of two levels (bits/datasets/streams), restricted to subsets."""
    name: str
    description: str
    rows: tuple                 # (level, bool mask over that level's items)
    cols: tuple
    symmetric: bool = False


def default_totals(maps, extra_regex_totals=None):
    """The totals of a measurement: input, per category, physics, all datasets, L1 per flavour,
    the Status_OnCPU/OnGPU check, the scouting overlap and the configured regex totals."""
    out = [Total("input", "all events (HLT input)", [])]
    if maps.datasets:
        for cat in ("standard", "parking", "scouting", "other"):
            out.append(Total(cat, f"any path in a {cat} dataset", [maps.category_mask([cat])]))
        out.append(Total("standard_parking", "any path in a standard or parking dataset",
                         [maps.category_mask(["standard", "parking"])]))
        out.append(Total("physics", "any path in a standard, parking or scouting dataset",
                         [maps.category_mask(PHYSICS_CATEGORIES)]))
        out.append(Total("all_datasets", "any path in any dataset (events written to a stream)",
                         [maps.category_mask(CATEGORIES[:-1])]))
        out.append(Total("scouting_and_standard_parking", "events in a scouting AND a standard or parking dataset",
                         [maps.category_mask(["scouting"]), maps.category_mask(["standard", "parking"])]))
    else:
        out.append(Total("standard", "any path treated as standard physics (no datasets: merge.pure_reference)",
                         [maps.category_mask(["standard"])]))
        scouting = maps.category_mask(["scouting"])
        if scouting.any():
            out.append(Total("scouting", "any path treated as scouting (no datasets: merge.scouting_paths)", [scouting]))
            out.append(Total("physics", "any standard or scouting path", [maps.category_mask(PHYSICS_CATEGORIES)]))
            out.append(Total("scouting_and_standard_parking", "events with a scouting AND a standard path",
                             [scouting, maps.category_mask(["standard", "parking"])]))
    for flavour in maps.l1_flavours():
        label = l1_reference(flavour)
        out.append(Total(label, "any L1 seed" + (f" ({flavour})" if flavour else ""), [maps.l1_seed_mask(flavour)]))
    status = maps.mask(lambda i: maps.bits[i] in ("Status_OnCPU", "Status_OnGPU"))
    if status.any():
        out.append(Total("status_oncpu_or_ongpu", "Status_OnCPU or Status_OnGPU (should equal the input)", [status]))
    for label, regex in (extra_regex_totals or {}).items():
        rx = re.compile(regex)
        mask = maps.mask(lambda i, rx=rx: bool(rx.search(maps.bits[i])) and not maps.excluded[i])
        if mask.any():               # (e.g. no L1 total for EDM ntuples, which have no L1 bits)
            out.append(Total(f"regex_{label}", f"any bit matching {regex!r} (not excluded)", [mask]))
    return out


def default_overlaps(maps, settings):
    """The overlap matrices switched on in ``merge.overlaps`` (datasets and streams: the combined ones)."""
    nds, nst = len(maps.combined_datasets), len(maps.combined_streams)
    hlt = maps.mask(lambda i: maps.kind[i] == "hlt")
    l1 = maps.l1_seed_mask()
    all_ds, all_st = np.ones(nds, bool), np.ones(nst, bool)
    candidates = [
        ("datasets", Overlap("datasets", "dataset x dataset", ("datasets", all_ds), ("datasets", all_ds), True), nds),
        ("streams", Overlap("streams", "stream x stream", ("streams", all_st), ("streams", all_st), True), nst),
        ("paths_datasets", Overlap("paths_datasets", "HLT path x dataset", ("bits", hlt), ("datasets", all_ds)),
         nds and hlt.any()),
        ("l1_datasets", Overlap("l1_datasets", "L1 seed x dataset", ("bits", l1), ("datasets", all_ds)), nds and l1.any()),
        ("l1_streams", Overlap("l1_streams", "L1 seed x stream", ("bits", l1), ("streams", all_st)), nst and l1.any()),
        ("paths", Overlap("paths", "HLT path x HLT path", ("bits", hlt), ("bits", hlt), True), hlt.any()),
    ]
    return [ov for key, ov, possible in candidates if settings.get(key) and possible]


class RateAccumulator:
    """Adds up combination tables into per-item sums, pure sums, totals and overlaps."""

    def __init__(self, maps, totals=(), overlaps=(), block=DEFAULT_BLOCK):
        self.maps = maps
        self.block = block
        self.index = {b: i for i, b in enumerate(maps.bits)}
        self.totals = list(totals)
        self.overlaps = list(overlaps)
        nb = maps.n_bits
        m_ds = maps.dataset_members()                              # bits x split datasets
        self.has_groups = m_ds.shape[1] > 0
        csr = lambda a: sp.csr_matrix(np.asarray(a, dtype=np.float64))   # noqa: E731
        i64 = lambda a: a.astype(np.int64)                                # noqa: E731
        m_st, c_ds, c_st = maps.stream_members(), maps.combination("datasets"), maps.combination("streams")
        self.M_ds = csr(m_ds)
        self.M_st = csr(m_st)                                      # split datasets x split streams
        self.M_cds = csr((i64(m_ds) @ i64(c_ds)) > 0)              # bits x combined datasets
        self.M_cst = csr((i64(m_ds) @ i64(m_st) @ i64(c_st)) > 0)  # bits x combined streams
        dp = maps.dataset_path_bits()                              # Dataset_ bit of each split dataset
        has = dp >= 0
        self.M_dp = sp.csr_matrix((np.ones(int(has.sum())), (dp[has], np.flatnonzero(has))), shape=(nb, len(dp)))
        self.no_dp = sp.diags((~has).astype(np.float64)).tocsr()
        self.n_items = {"bits": nb, "datasets": c_ds.shape[1], "streams": c_st.shape[1],
                        "datasets_split": m_ds.shape[1], "streams_split": m_st.shape[1]}

        # pure-rate reference groups: member masks per level, and the group of each item
        self.references = maps.references()
        ds_names, st_names = list(maps.combined_datasets), list(maps.combined_streams)
        self.item_group = {
            "bits": np.array([maps.bit_reference(i) for i in range(nb)], dtype=object),
            "datasets": np.array([maps.group_reference(maps.dataset_category[d]) for d in ds_names], dtype=object),
            "streams": np.array([maps.group_reference(maps.stream_category[s]) for s in st_names], dtype=object),
        }
        self.sp_mask = maps.category_mask(HLT_REFERENCES["standard_parking"])

        self.sums = {lvl: {k: np.zeros((self.n_items[lvl], 3)) for k in ("fired", "pure", "shared_sp")}
                     for lvl in LEVELS}
        self.sums.update({lvl: {"fired": np.zeros((self.n_items[lvl], 3))} for lvl in SPLIT_LEVELS})
        self.total_sums = {t.label: np.zeros(3) for t in self.totals}
        self.overlap_sums = {ov.name: [None, None, None] for ov in self.overlaps}
        self.n_rows = 0
        self.n_tables = 0

    # ---- input -------------------------------------------------------------------------------------
    def add_table(self, table):
        """Add a :class:`~steamrates.patterns.PatternTable` whose bit names are all known."""
        try:
            lookup = np.array([self.index[n] for n in table.bit_names], dtype=np.int64)
        except KeyError as e:
            raise ValueError(f"bit {e.args[0]!r} is not in the merged bit list") from None
        for start, stop, offsets, indices in table.iter_csr(self.block):
            x = sp.csr_matrix((np.ones(len(indices)), lookup[indices], offsets),
                              shape=(stop - start, self.maps.n_bits))
            w = np.column_stack([table.count[start:stop], table.sumw[start:stop],
                                 table.sumw2[start:stop]]).astype(np.float64)
            self._add_block(x, w)
        self.n_tables += 1

    def _add_block(self, x, w):
        self.n_rows += x.shape[0]
        mats = {"bits": x}
        if self.has_groups:
            mats["datasets"] = ((x @ self.M_cds) > 0).astype(np.float64).tocsr()
            mats["streams"] = ((x @ self.M_cst) > 0).astype(np.float64).tocsr()
            paths_or = ((x @ self.M_ds) > 0).astype(np.float64).tocsr()
            split_ds = (paths_or.multiply(x @ self.M_dp) + paths_or @ self.no_dp).tocsr()
            split_st = ((split_ds @ self.M_st) > 0).astype(np.float64).tocsr()
            self.sums["datasets_split"]["fired"] += split_ds.T.tocsr() @ w
            self.sums["streams_split"]["fired"] += split_st.T.tocsr() @ w

        sp_fired = (x @ self.sp_mask.astype(np.float64)) > 0
        for lvl, a in mats.items():
            at = a.T.tocsr()
            s = self.sums[lvl]
            s["fired"] += at @ w
            s["shared_sp"] += at @ (w * sp_fired[:, None])
            groups = self.item_group[lvl]
            for g, masks in self.references.items():
                ref = masks[LEVELS.index(lvl)]
                items = groups == g
                if not ref.any() or not items.any():
                    continue
                n_ref = a @ ref.astype(np.float64)
                s["pure"][items] += (at @ (w * (n_ref == 1)[:, None]))[items]

        for t in self.totals:
            sel = np.ones(x.shape[0], dtype=bool)
            for m in t.all_of:
                sel &= (x @ m.astype(np.float64)) > 0
            self.total_sums[t.label] += w[sel].sum(axis=0)

        for ov in self.overlaps:
            (lvl_a, mask_a), (lvl_b, mask_b) = ov.rows, ov.cols
            if lvl_a not in mats or lvl_b not in mats:
                continue
            a = mats[lvl_a][:, np.flatnonzero(mask_a)]
            b = mats[lvl_b][:, np.flatnonzero(mask_b)]
            acc = self.overlap_sums[ov.name]
            for k in range(3):
                c = (a.multiply(w[:, k][:, None]).tocsr().T @ b).tocsr()
                acc[k] = c if acc[k] is None else acc[k] + c

    # ---- output ------------------------------------------------------------------------------------
    def overlap_matrix(self, name):
        """``(count, sumw, sumw2)`` sparse matrices of one overlap (zero matrices if nothing was added)."""
        ov = next(o for o in self.overlaps if o.name == name)
        shape = (int(ov.rows[1].sum()), int(ov.cols[1].sum()))
        return [m if m is not None else sp.csr_matrix(shape) for m in self.overlap_sums[name]]
