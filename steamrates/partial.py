"""Partial results: the output of one counting job, stored as one ROOT file.

File layout (written with uproot, zstd-compressed):

    metadata   TObjString with a JSON document (see below)
    patterns   TTree, one entry per distinct combination of fired bits
                 fired  : list of bit indices (int16, or int32 for > 32767 bits)
                 count  : int64    number of events
                 sumw   : float64  sum of event weights
                 sumw2  : float64  sum of squared weights
    lumis      TTree, one entry per (run, lumi section) seen
                 run, luminosityBlock : uint32
                 n_events : int64   selected events in this lumi section
                 in_file  : bool    lumi section listed in the input files' LuminosityBlocks tree

The JSON metadata holds everything needed to interpret and merge the file:
``schema``, ``steamrates_version``, ``created``, ``host``, ``command``, ``sample`` (label of the
sample; lumi sections are counted per sample), ``format``, ``bits`` (names of the bit indices used in
``fired``), ``menu`` (HLT menu of the first input file), ``lumi_mask``, ``weight_branch``,
``inputs`` (per input file: path, events read/selected, menu name), ``totals``, ``mc_sums``
(``genEventSumw`` etc. from the Runs trees) and ``duplicate_events``.

Partial results of a measurement are added up with :func:`merge_partials`.
"""

from __future__ import annotations

import json
import os

import awkward as ak
import numpy as np
import uproot

from .patterns import PatternTable

SCHEMA = "steamrates.partial/1"


class PartialResult:
    def __init__(self, metadata, table, lumis):
        self.metadata = metadata
        self.table = table
        self.lumis = lumis              # dict of numpy arrays: run, luminosityBlock, n_events, in_file

    # ---- writing -------------------------------------------------------------------------------

    def write(self, path):
        """Write to ``path`` atomically (via a temporary file next to it, then rename)."""
        offsets, indices = self.table.to_csr()
        idx_type = np.int16 if self.table.n_bits <= np.iinfo(np.int16).max else np.int32
        fired = ak.unflatten(indices.astype(idx_type), np.diff(offsets))
        meta = dict(self.metadata, bits=self.table.bit_names, schema=SCHEMA)
        tmp = f"{path}.tmp-{os.getpid()}"
        with uproot.recreate(tmp, compression=uproot.ZSTD(5)) as f:
            f["metadata"] = json.dumps(meta)
            f.mktree("patterns", {"fired": f"var * {np.dtype(idx_type).name}",
                                  "count": np.int64, "sumw": np.float64, "sumw2": np.float64})
            if self.table.n_rows:
                f["patterns"].extend({"fired": fired, "count": self.table.count, "sumw": self.table.sumw,
                                      "sumw2": self.table.sumw2})
            f.mktree("lumis", {"run": np.uint32, "luminosityBlock": np.uint32, "n_events": np.int64,
                               "in_file": np.bool_})
            if len(self.lumis["run"]):
                f["lumis"].extend({k: self.lumis[k] for k in ("run", "luminosityBlock", "n_events", "in_file")})
        os.replace(tmp, path)

    # ---- reading -------------------------------------------------------------------------------

    @classmethod
    def read(cls, path):
        with uproot.open(path) as f:
            meta = json.loads(str(f["metadata"]))
            if meta.get("schema") != SCHEMA:
                raise ValueError(f"{path}: unknown schema {meta.get('schema')!r}")
            t = f["patterns"]
            if t.num_entries:
                fired = t["fired"].array(library="ak")
                offsets = np.asarray(fired.layout.offsets.data, dtype=np.int64)
                indices = np.asarray(fired.layout.content.data, dtype=np.int64)
                offsets = offsets - offsets[0]
                cols = t.arrays(["count", "sumw", "sumw2"], library="np")
                table = PatternTable.from_csr(meta["bits"], offsets, indices, cols["count"], cols["sumw"],
                                              cols["sumw2"])
            else:
                table = PatternTable.empty(meta["bits"])
            lumis = f["lumis"].arrays(["run", "luminosityBlock", "n_events", "in_file"], library="np")
        return cls(meta, table, dict(lumis))


def merge_partials(partials, rename=None):
    """Add up several partial results.

    Returns ``(table, lumi_sets, info)``:
      * ``table``: the summed :class:`PatternTable` (union of all bits; ``rename``, e.g.
        :func:`steamrates.menu.strip_version`, is applied to the bit names first),
      * ``lumi_sets``: ``{sample: {"with_events": set of (run, lumi), "in_file": set of (run, lumi)}}``
        - every lumi section counted once per sample, however many files/jobs it was spread over,
      * ``info``: summed totals and the list of inputs (to spot a file that was counted twice).
    """
    tables, lumi_sets, inputs = [], {}, []
    totals = {"n_events_read": 0, "n_events_selected": 0}
    mc_sums = {}
    for p in partials:
        t = p.table.rename(rename) if rename else p.table
        tables.append(t)
        sample = p.metadata.get("sample", "default")
        s = lumi_sets.setdefault(sample, {"with_events": set(), "in_file": set()})
        run, lumi = p.lumis["run"].tolist(), p.lumis["luminosityBlock"].tolist()
        s["with_events"].update((r, l) for r, l, n in zip(run, lumi, p.lumis["n_events"].tolist()) if n > 0)
        s["in_file"].update((r, l) for r, l, x in zip(run, lumi, p.lumis["in_file"].tolist()) if x)
        for k in totals:
            totals[k] += p.metadata["totals"][k]
        for k, v in p.metadata.get("mc_sums", {}).items():
            mc_sums[k] = mc_sums.get(k, 0.0) + v
        inputs.extend(i["path"] for i in p.metadata["inputs"])
    seen, dup = set(), set()
    for i in inputs:
        (dup if i in seen else seen).add(i)
    info = dict(totals, mc_sums=mc_sums, n_inputs=len(inputs), duplicate_inputs=sorted(dup))
    return PatternTable.merge(tables), lumi_sets, info
