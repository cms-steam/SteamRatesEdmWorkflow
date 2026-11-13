"""Ntuple readers. Use :func:`make_reader` to get the right one for a file format."""

from __future__ import annotations

import numpy as np

from ..inputs import open_root
from ..rootbytes import read_raw_entries
from .base import EventChunk, FileInfo, Reader
from .edm import EdmReader, decode_lumi_aux
from .nano import DEFAULT_BITS_EXCLUDE, DEFAULT_BITS_INCLUDE, NanoReader

FORMATS = ("edm", "nano")


def detect_format_of(f, path="?"):
    """Format of an open file: 'edm' if it has ``edm::TriggerResults``, 'nano' if it has flat
    ``HLT_*``/``L1_*`` branches."""
    keys = f["Events"].keys(recursive=False)
    if any(k.startswith("edmTriggerResults_TriggerResults__") for k in keys):
        return "edm"
    if any(k.startswith(("HLT", "L1_", "DST_")) for k in keys):
        return "nano"
    raise ValueError(f"{path}: cannot determine the ntuple format")


def detect_format(path):
    with open_root(path) as f:
        return detect_format_of(f, path)


def file_lumi_blocks(f, fmt):
    """(n, 2) uint32 array of the (run, lumi) pairs in the file's ``LuminosityBlocks`` tree.

    Reads only that small tree, not the events. Empty if the file has no such tree.
    """
    if "LuminosityBlocks" not in f:
        return np.zeros((0, 2), np.uint32)
    lb = f["LuminosityBlocks"]
    if fmt == "edm":
        data, starts, _ = read_raw_entries(lb["LuminosityBlockAuxiliary"])
        return decode_lumi_aux(data, starts)
    a = lb.arrays(["run", "luminosityBlock"], library="np")
    return np.stack([a["run"], a["luminosityBlock"]], axis=1).astype(np.uint32)


def make_reader(fmt, process=None, l1_process=None, weight_branch=None, bits_include=DEFAULT_BITS_INCLUDE,
                bits_exclude=DEFAULT_BITS_EXCLUDE, chunk_size=50_000):
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
           "detect_format_of", "file_lumi_blocks", "make_reader", "FORMATS"]
