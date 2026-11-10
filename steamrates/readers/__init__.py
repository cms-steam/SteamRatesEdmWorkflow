"""Ntuple readers. Use :func:`make_reader` to get the right one for a file format."""

from __future__ import annotations

import uproot

from .base import EventChunk, FileInfo, Reader
from .edm import EdmReader
from .nano import DEFAULT_BITS_EXCLUDE, DEFAULT_BITS_INCLUDE, NanoReader

FORMATS = ("edm", "nano")


def detect_format(path):
    """'edm' if the file has ``edm::TriggerResults``, 'nano' if it has flat ``HLT_*`` branches."""
    with uproot.open(path) as f:
        events = f["Events"]
        keys = events.keys(recursive=False)
        if any(k.startswith("edmTriggerResults_TriggerResults__") for k in keys):
            return "edm"
        if any(k.startswith(("HLT", "L1_", "DST_")) for k in keys):
            return "nano"
    raise ValueError(f"{path}: cannot determine the ntuple format")


def make_reader(fmt, process=None, l1_process=None, weight_branch=None, bits_include=DEFAULT_BITS_INCLUDE,
                bits_exclude=DEFAULT_BITS_EXCLUDE, chunk_size=200_000):
    if fmt == "edm":
        if weight_branch:
            raise ValueError("EDM TriggerResults ntuples have no weight branch")
        if l1_process:
            raise ValueError("EDM TriggerResults ntuples have no L1 bits")
        return EdmReader(process=process, chunk_size=chunk_size)
    if fmt == "nano":
        return NanoReader(bits_include=bits_include, bits_exclude=bits_exclude, weight_branch=weight_branch,
                          process=process, l1_process=l1_process, chunk_size=chunk_size)
    raise ValueError(f"unknown format {fmt!r}; choose from {FORMATS}")


__all__ = ["EventChunk", "FileInfo", "Reader", "EdmReader", "NanoReader", "detect_format",
           "make_reader", "FORMATS"]
