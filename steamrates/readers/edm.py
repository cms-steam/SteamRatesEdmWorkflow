"""Reader for EDM files that contain ``edm::TriggerResults`` (the current STEAM step-1 ntuples).

The step-1 ntuples (``hlt_N.root``) are EDM files that keep only ``edm::TriggerResults`` of the
re-run HLT process (usually ``MYHLT``). Reading them with FWLite costs ~1 ms per event in Python.
This reader decodes the raw bytes with numpy instead (~16x faster) and needs only uproot.

Three things are decoded (byte layouts below; all integers are big-endian):

1. ``edm::TriggerResults`` (branch ``edmTriggerResults_TriggerResults__<PROCESS>.``)
   One entry per event::

       [0:6]    edm::TriggerResults header         (class version 11)
       [6:12]   base class edm::HLTGlobalStatus    (class version 10)
       [12:18]  its member vector<edm::HLTPathStatus> paths_; the version word has the
                "memberwise" flag 0x4000 set -> 0x400A
       [18:20]  class version of edm::HLTPathStatus (10)
       [20:24]  N = number of paths (unsigned int)
       [24:24+2N]  one 16-bit status word per path:
                   state = word & 3  (0 ready, 1 pass, 2 fail, 3 exception)
                   index = word >> 2 (index of the last module that ran in the path)
       then     edm::DoNotRecordParents base (6 bytes), the psetid (edm::Hash<1>: 6 + 6 header
                bytes, a length byte = 16, 16 hash bytes) and names_ (vector<string>, empty).

   The psetid is the hash of the parameter set holding the ordered path names
   (``@trigger_paths``); the names themselves come from the file's ``ParameterSets`` tree
   (see :mod:`steamrates.menu`).

2. ``edm::EventAuxiliary`` (branch ``EventAuxiliary``), one entry per event::

       [0:29]   processHistoryID (edm::Hash<2>: headers + 16 bytes)
       [29:35]  edm::EventID header (class version 12)
       [35:39]  run   [39:43] luminosity block   [43:51] event number
       ...      processGUID, time, flags
       [-12:-8] bunch crossing   [-8:-4] orbit number   [-4:] store number

3. ``edm::LuminosityBlockAuxiliary`` (tree ``LuminosityBlocks``), one entry per lumi section::

       [0:29]   processHistoryID   [29:35] edm::LuminosityBlockID header (version 10)
       [35:39]  run   [39:43] luminosity block   then begin/end time

The decoder checks every header and the class versions stored in the file and raises
:class:`~steamrates.rootbytes.LayoutError` if anything is unexpected, so a future layout
change cannot go unnoticed. It was cross-checked against FWLite (accept bits, last-module index,
path names, run/lumi/event) on GRunV51/V59/V62 and two HI menus (2026-11-09).
"""

from __future__ import annotations

import re

import numpy as np
import uproot

from ..menu import ParameterSetRegistry
from .base import EventChunk, FileInfo, Reader
from ..rootbytes import LayoutError, check_class_versions, check_header, read_raw_entries, u2, u4, u8

# Class versions the byte layouts above were written for.
EXPECTED_CLASS_VERSIONS = {
    "edm::TriggerResults": 11,
    "edm::HLTGlobalStatus": 10,
    "edm::HLTPathStatus": 10,
    "edm::EventAuxiliary": 11,
    "edm::EventID": 12,
}

_TR_BRANCH = re.compile(r"^edmTriggerResults_TriggerResults__(\w+)\.$")


def find_trigger_results(events_tree, process=None):
    """Name of the TriggerResults branch to use and its process name."""
    found = {}
    for key in events_tree.keys(recursive=False):
        m = _TR_BRANCH.match(key)
        if m:
            found[m.group(1)] = key
    if process is not None:
        if process not in found:
            raise LayoutError(f"no TriggerResults for process {process!r}; available: {sorted(found)}")
        return found[process], process
    if len(found) != 1:
        raise LayoutError(f"found TriggerResults of processes {sorted(found)}; choose one with --process")
    process, key = next(iter(found.items()))
    return key, process


def decode_trigger_results(data, starts, sizes):
    """Decode raw TriggerResults entries.

    Returns ``(status, psetid)``: ``status`` is a (events x paths) uint16 matrix of status words,
    ``psetid`` the hex hash of the trigger-names parameter set.
    """
    check_header(data, starts, 11, "TriggerResults")
    check_header(data, starts + 6, 10, "TriggerResults: HLTGlobalStatus base")
    check_header(data, starts + 12, 0x4000 | 10, "TriggerResults: memberwise vector<HLTPathStatus>")
    if not np.all(u2(data, starts + 18) == 10):
        raise LayoutError("TriggerResults: unexpected HLTPathStatus class version")
    n_paths = np.unique(u4(data, starts + 20))
    if len(n_paths) != 1:
        raise LayoutError(f"TriggerResults: entries with different numbers of paths {n_paths.tolist()}")
    n = int(n_paths[0])
    # The trailer after the status words has a fixed size when names_ is empty (always the case
    # when the names are stored via the psetid). Anything else is a layout we do not handle.
    size = 24 + 2 * n + 45
    if not np.all(sizes == size):
        raise LayoutError("TriggerResults: unexpected entry size (non-empty names_?)")
    # All entries have the same size and follow each other, so the bytes form an
    # (events x size) matrix; the status words are columns 24 .. 24+2N, read as big-endian uint16.
    rows = data[starts[0]:starts[0] + len(starts) * size].reshape(len(starts), size)
    status = np.ascontiguousarray(rows[:, 24:24 + 2 * n]).view(">u2").astype(np.uint16)
    # psetid: after the status words come DoNotRecordParents (6 bytes) and two 6-byte headers of
    # the edm::Hash<1> and its std::string; then the length byte (16) and the 16 hash bytes.
    h = 24 + 2 * n + 18
    if not np.all(rows[:, h] == 16):
        raise LayoutError("TriggerResults: unexpected psetid encoding")
    hash_bytes = np.unique(rows[:, h + 1:h + 17], axis=0)
    if len(hash_bytes) > 1:
        raise LayoutError(f"TriggerResults: {len(hash_bytes)} different trigger-name parameter sets in one block")
    return status, (bytes(hash_bytes[0]).hex() if len(hash_bytes) else None)


def decode_event_aux(data, starts, sizes):
    """Decode raw EventAuxiliary entries -> ``(run, lumi, event, bunch_crossing)`` arrays."""
    if not np.all(data[starts + 12] == 16):
        raise LayoutError("EventAuxiliary: unexpected processHistoryID encoding")
    check_header(data, starts + 29, 12, "EventAuxiliary: EventID")
    run = u4(data, starts + 35).astype(np.uint32)
    lumi = u4(data, starts + 39).astype(np.uint32)
    event = u8(data, starts + 43)
    bx = u4(data, starts + sizes - 12).astype(np.uint32)
    return run, lumi, event, bx


def decode_lumi_aux(data, starts):
    """Decode raw LuminosityBlockAuxiliary entries -> (n, 2) array of (run, lumi)."""
    if len(starts) == 0:
        return np.zeros((0, 2), dtype=np.uint32)
    if not np.all(data[starts + 12] == 16):
        raise LayoutError("LuminosityBlockAuxiliary: unexpected processHistoryID encoding")
    check_header(data, starts + 29, 10, "LuminosityBlockAuxiliary: LuminosityBlockID")
    return np.stack([u4(data, starts + 35), u4(data, starts + 39)], axis=1).astype(np.uint32)


class EdmReader(Reader):
    """Reads ``edm::TriggerResults`` + event IDs from EDM files (e.g. STEAM ``hlt_N.root``)."""

    format = "edm"

    def __init__(self, process=None, chunk_size=200_000):
        super().__init__(chunk_size)
        self.process = process
        self._file = None
        # Menus already read, keyed by the psetid stored in TriggerResults. The psetid is a hash of
        # the ordered path-name list, so equal psetids mean identical names: the (13 MB, ~0.6 s)
        # ParameterSets tree is then read only for the first file of each menu.
        self._menu_cache = {}

    def open(self, path):
        self._file = uproot.open(path)
        check_class_versions(self._file, EXPECTED_CLASS_VERSIONS)
        self._events = self._file["Events"]
        self._tr_key, process = find_trigger_results(self._events, self.process)
        self._tr_branch = self._events[self._tr_key + "/" + self._tr_key + "obj"]
        self._aux_branch = self._events["EventAuxiliary"]
        self._names = None
        self._psetid = None
        n = self._events.num_entries

        # Menu: identified through the psetid of the first event (the names must not change).
        menu = None
        if n > 0:
            _, psetid = decode_trigger_results(*read_raw_entries(self._tr_branch, 0, 1))
            if psetid not in self._menu_cache:
                registry = ParameterSetRegistry.from_file(self._file)
                self._menu_cache[psetid] = registry.menu(trigger_paths_id=psetid)
            menu = self._menu_cache[psetid]
            self._psetid, self._names = psetid, list(menu.trigger_paths)
        lb = self._file["LuminosityBlocks"]["LuminosityBlockAuxiliary"]
        data, starts, _ = read_raw_entries(lb)
        return FileInfo(path=path, format=self.format, n_events=n, menu=menu, menu_id=self._psetid,
                        lumi_blocks=decode_lumi_aux(data, starts))

    def chunks(self, max_events=None):
        for start, stop in self._ranges(self._events.num_entries, max_events):
            status, psetid = decode_trigger_results(*read_raw_entries(self._tr_branch, start, stop))
            if psetid != self._psetid:
                raise LayoutError(f"trigger names change inside the file ({self._psetid} -> {psetid})")
            if status.shape[1] != len(self._names):
                raise LayoutError("number of status words differs from the number of path names")
            run, lumi, event, _ = decode_event_aux(*read_raw_entries(self._aux_branch, start, stop))
            accept = (status & 3) == 1
            yield EventChunk(run=run, lumi=lumi, event=event, bits=accept, bit_names=self._names)
