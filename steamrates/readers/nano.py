"""Reader for NanoAOD-style flat ntuples (HLTPhysics NANOAOD, NANOAODSIM, future STEAM ntuples).

In NanoAOD every trigger decision is a flat ``bool`` branch named after the path without its version
suffix (``HLT_IsoMu24``, ``L1_SingleMu22``, ``DST_PFScouting_...``). Which branches exist is decided
by NanoAOD's ``TriggerOutputBranches``: names starting with ``HLT``, ``L1`` or ``Flag`` or containing
``Scouting``. Paths missing from the menu of a given run are still written, filled with False.

Per event we read: ``run``, ``luminosityBlock``, ``event``, the selected trigger bits and, for MC,
optionally a weight branch (e.g. ``genWeight``). Per file we also read:
  * the ``LuminosityBlocks`` tree -> all (run, lumi) pairs in the file (also lumi sections without
    events), needed to count lumi sections for the data normalization,
  * the ``Runs`` tree -> ``genEventSumw``/``genEventSumw2``/``genEventCount`` (MC normalization),
  * the HLT menu from the ``ParameterSets`` tree (see :mod:`steamrates.menu`), if present
    (first file only: it is metadata here, the bit names are the branch names).
"""

from __future__ import annotations

import logging
import re

import numpy as np
import uproot

from ..menu import ParameterSetRegistry
from .base import EventChunk, FileInfo, Reader

log = logging.getLogger(__name__)

# Default selection of trigger-bit branches: HLT paths (incl. HLTriggerFirst/FinalPath), L1 seeds
# and other L1 bits, scouting/DST, dataset and AlCa/MC paths. Event filters (Flag_*) are excluded:
# they are not triggers and would only make the combination table larger.
DEFAULT_BITS_INCLUDE = r"^(HLT|L1|DST_|Dataset_|AlCa_|MC_|Status_)|Scouting"
DEFAULT_BITS_EXCLUDE = r"^Flag_"

_PROCESS_IN_TITLE = re.compile(r"\(process:\s*(\w+)\)")


class NanoReader(Reader):
    """Reads trigger bits from a NanoAOD ``Events`` tree."""

    format = "nano"

    def __init__(self, bits_include=DEFAULT_BITS_INCLUDE, bits_exclude=DEFAULT_BITS_EXCLUDE,
                 weight_branch=None, process=None, chunk_size=200_000):
        super().__init__(chunk_size)
        self.bits_include = re.compile(bits_include) if bits_include else None
        self.bits_exclude = re.compile(bits_exclude) if bits_exclude else None
        self.weight_branch = weight_branch
        self.process = process
        self._menu_tried = False

    def select_bits(self, tree):
        """Names of the scalar ``bool`` branches that are trigger bits (in tree order)."""
        names = []
        for name in tree.keys(recursive=False):
            if tree[name].typename != "bool":          # scalar bools only (not per-object flags)
                continue
            if self.bits_include and not self.bits_include.search(name):
                continue
            if self.bits_exclude and self.bits_exclude.search(name):
                continue
            names.append(name)
        return names

    def open(self, path):
        self._file = uproot.open(path)
        self._events = self._file["Events"]
        self._names = self.select_bits(self._events)
        if not self._names:
            raise RuntimeError(f"{path}: no trigger-bit branches selected")
        if self.weight_branch and self.weight_branch not in self._events:
            raise RuntimeError(f"{path}: weight branch {self.weight_branch!r} not found")

        info = FileInfo(path=path, format=self.format, n_events=self._events.num_entries)
        if "LuminosityBlocks" in self._file:
            lb = self._file["LuminosityBlocks"].arrays(["run", "luminosityBlock"], library="np")
            info.lumi_blocks = np.stack([lb["run"], lb["luminosityBlock"]], axis=1).astype(np.uint32)
        if "Runs" in self._file:
            runs = self._file["Runs"]
            for key in ("genEventSumw", "genEventSumw2", "genEventCount"):
                if key in runs:
                    info.mc_sums[key] = float(np.sum(runs[key].array(library="np")))

        # The HLT menu is optional metadata here (the bit names are the branch names). Reading the
        # ParameterSets tree costs ~0.5 s, so it is done for the first file only.
        if not self._menu_tried and "ParameterSets" in self._file:
            self._menu_tried = True
            # The HLT process is written in the branch titles, e.g. "Trigger/flag bit (process: HLT)".
            process = self.process
            if process is None:
                hlt = [n for n in self._names if n.startswith("HLT")]
                m = _PROCESS_IN_TITLE.search(self._events[hlt[0]].title) if hlt else None
                process = m.group(1) if m else None
            try:
                info.menu = ParameterSetRegistry.from_file(self._file).menu(process=process)
            except Exception as err:
                log.warning("%s: could not read the HLT menu (%s)", path, err)
        return info

    def chunks(self, max_events=None):
        branches = ["run", "luminosityBlock", "event"] + self._names
        if self.weight_branch:
            branches.append(self.weight_branch)
        for start, stop in self._ranges(self._events.num_entries, max_events):
            a = self._events.arrays(branches, entry_start=start, entry_stop=stop, library="np")
            bits = np.empty((stop - start, len(self._names)), dtype=bool)
            for j, name in enumerate(self._names):
                bits[:, j] = a[name]
            yield EventChunk(run=a["run"].astype(np.uint32), lumi=a["luminosityBlock"].astype(np.uint32),
                             event=a["event"].astype(np.uint64), bits=bits, bit_names=self._names,
                             weights=a[self.weight_branch].astype(np.float64) if self.weight_branch else None)
