"""Reader for NanoAOD-style flat ntuples (HLTPhysics NANOAOD, NANOAODSIM, future STEAM ntuples).

In NanoAOD every trigger decision is a flat ``bool`` branch named after the path without its version
suffix (``HLT_IsoMu24``, ``L1_SingleMu22``, ``DST_PFScouting_...``). Which branches exist is decided
by NanoAOD's ``TriggerOutputBranches``: names starting with ``HLT``, ``L1`` or ``Flag`` or containing
``Scouting``. Paths missing from the menu of a given run are still written, filled with False.

Several processes in one file
-----------------------------
NanoAOD writes the paths of **every** ``edm::TriggerResults`` in the event, i.e. of every process that
ran paths. Each branch title names its process: ``Trigger/flag bit (process: HLT2)``. If two processes
have a path of the same name, the branch written second gets the suffix ``_p<process>``
(``HLT_IsoMu24_FromL1TkMuon_pHLT``). Which process keeps the plain name is not defined by the process
order, so only the title is reliable. Examples:

  * Run-3 PromptReco NANOAOD: HLT paths from process ``HLT``, L1 seeds and ``Flag_*`` from ``RECO``.
  * Re-HLT MC (e.g. the first STEAM Phase-2 ntuples): HLT paths of the original sample production
    (``HLT``) *and* of the re-run menu (``HLT2``).

Mixing two HLT runs in one combination table would be wrong, so this reader takes the bits of one
process per kind of bit:

  * HLT-like bits (everything that does not start with ``L1``): process ``process``,
  * L1 bits (names starting with ``L1``): process ``l1_process``.

If a value is not given and the file has bits of exactly one process of that kind, that process is
used; if it has several, the reader stops and asks for a choice. The ``_p<process>`` suffix is
removed, so the bit names in the partial result are the path names.

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
from collections import defaultdict

import numpy as np

from ..inputs import open_root
from ..menu import ParameterSetRegistry
from .base import EventChunk, FileInfo, Reader

log = logging.getLogger(__name__)

# Default selection of trigger-bit branches: HLT paths (incl. HLTriggerFirst/FinalPath), L1 seeds
# and other L1 bits, scouting/DST, dataset and AlCa/MC paths. The patterns are applied to the path
# name (after removing a ``_p<process>`` suffix). Excluded by default:
#   * event filters (``Flag_*``): not triggers, they would only make the combination table larger;
#   * cmsDriver step paths (``L1Reco_step``, ``L1simulation_step``, ``L1TrackTrigger_step``, ...):
#     they start with "L1" and are therefore written by NanoAOD, but they are always true.
DEFAULT_BITS_INCLUDE = r"^(HLT|L1|DST_|Dataset_|AlCa_|MC_|Status_)|Scouting"
DEFAULT_BITS_EXCLUDE = r"^Flag_|_step$"

_PROCESS_IN_TITLE = re.compile(r"\(process:\s*(\w+)\)")


def bit_kind(name):
    """'l1' for L1 bits (names starting with ``L1``), 'hlt' for all other trigger bits."""
    return "l1" if name.startswith("L1") else "hlt"


class NanoReader(Reader):
    """Reads trigger bits from a NanoAOD ``Events`` tree."""

    format = "nano"

    def __init__(self, bits_include=DEFAULT_BITS_INCLUDE, bits_exclude=DEFAULT_BITS_EXCLUDE,
                 weight_branch=None, process=None, l1_process=None, chunk_size=50_000):
        super().__init__(chunk_size)
        self.bits_include = re.compile(bits_include) if bits_include else None
        self.bits_exclude = re.compile(bits_exclude) if bits_exclude else None
        self.weight_branch = weight_branch
        self.process = process
        self.l1_process = l1_process
        self.selected_processes = {}   # kind -> process actually used (for the job metadata)
        self._menu_tried = False

    def select_bits(self, tree):
        """Choose the trigger-bit branches of ``tree``.

        Returns ``(branches, names, processes)``: the branch names to read, the path names to use
        for them (``_p<process>`` suffix removed; same order) and ``{kind: process}``.
        """
        # 1. All scalar bool branches (not per-object flags) that are trigger bits, with the process
        #    from the branch title and the path name without the "_p<process>" collision suffix.
        candidates = []                                  # (branch, name, kind, process)
        for branch in tree.keys(recursive=False):
            if tree[branch].typename != "bool":
                continue
            m = _PROCESS_IN_TITLE.search(tree[branch].title)
            process = m.group(1) if m else None
            name = branch
            if process and branch.endswith("_p" + process):
                name = branch[:-len(process) - 2]
            if self.bits_include and not self.bits_include.search(name):
                continue
            if self.bits_exclude and self.bits_exclude.search(name):
                continue
            candidates.append((branch, name, bit_kind(name), process))

        # 2. One process per kind of bit (see the module docstring).
        by_kind = defaultdict(lambda: defaultdict(int))  # kind -> process -> number of bits
        for _, _, kind, process in candidates:
            by_kind[kind][process] += 1
        wanted = {"hlt": (self.process, "--process"), "l1": (self.l1_process, "--l1-process")}
        chosen = {}
        for kind, counts in by_kind.items():
            requested, option = wanted[kind]
            found = ", ".join(f"{p} ({n} bits)" for p, n in sorted(counts.items(), key=str))
            if requested is not None:
                if requested not in counts:
                    raise RuntimeError(f"no {kind.upper()} bits of process {requested!r}; found: {found}")
                chosen[kind] = requested
            elif len(counts) == 1:
                chosen[kind] = next(iter(counts))
            else:
                raise RuntimeError(f"{kind.upper()} bits of several processes: {found}. "
                                   f"Choose one with {option}.")
        # A process requested for a kind that has no bits at all is most likely a typo.
        for kind, (requested, option) in wanted.items():
            if requested is not None and kind not in by_kind:
                raise RuntimeError(f"{option} {requested}: the file has no {kind.upper()} bits")

        selected = [(b, n) for b, n, kind, process in candidates if chosen[kind] == process]
        names = [n for _, n in selected]
        if len(set(names)) != len(names):
            dups = sorted({n for n in names if names.count(n) > 1})
            raise RuntimeError(f"bit names appear twice after removing the process suffix: {dups[:5]}")
        return [b for b, _ in selected], names, chosen

    def open(self, path):
        self._file = open_root(path)
        self._events = self._file["Events"]
        self._branches, self._names, chosen = self.select_bits(self._events)
        if not self._names:
            raise RuntimeError(f"{path}: no trigger-bit branches selected")
        if self.selected_processes and chosen != self.selected_processes:
            raise RuntimeError(f"{path}: trigger bits from processes {chosen}, but the first file had "
                               f"{self.selected_processes}")
        self.selected_processes = chosen
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
        # ParameterSets tree costs ~0.5 s, so it is done for the first file only. It is read for the
        # process whose HLT bits were selected.
        if not self._menu_tried and "ParameterSets" in self._file:
            self._menu_tried = True
            try:
                info.menu = ParameterSetRegistry.from_file(self._file).menu(process=chosen.get("hlt"))
            except Exception as err:
                log.warning("%s: could not read the HLT menu (%s)", path, err)
        return info

    def chunks(self, max_events=None):
        branches = ["run", "luminosityBlock", "event"] + self._branches
        if self.weight_branch:
            branches.append(self.weight_branch)
        for start, stop in self._ranges(self._events.num_entries, max_events):
            a = self._events.arrays(branches, entry_start=start, entry_stop=stop, library="np")
            bits = np.empty((stop - start, len(self._branches)), dtype=bool)
            for j, branch in enumerate(self._branches):
                bits[:, j] = a[branch]
            yield EventChunk(run=a["run"].astype(np.uint32), lumi=a["luminosityBlock"].astype(np.uint32),
                             event=a["event"].astype(np.uint64), bits=bits, bit_names=self._names,
                             weights=a[self.weight_branch].astype(np.float64) if self.weight_branch else None)
