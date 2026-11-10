"""Counting step: ntuple files -> one partial result (combination table + lumi sections + metadata).

Everything the job needs is given on the command line, and the only file written is ``--output``
(atomically, via a temporary file next to it). Several counting jobs, also of different
measurements, can therefore run at the same time in the same directory.

Examples (from the repository root, inside a CMSSW environment)::

    # current STEAM step-1 ntuples (EDM files with TriggerResults of the re-run HLT)
    python3 -m steamrates.count --lumi-json Json/json_402360.txt --sample EphemeralHLTPhysics \\
        --output partial_0.root /eos/.../402360_GRunV59/hlt_0.root /eos/.../hlt_1.root

    # NanoAOD, file names read from a text file (one per line), MC weights from genWeight
    python3 -m steamrates.count --format nano --weight-branch genWeight --sample VBFHToMuMu \\
        --input-list files.txt --output vbf_0.root

Steps per input file: open it (``steamrates.readers``), read the events in chunks, keep the events
inside the lumi JSON (if given), and add their trigger bits to the combination table
(``steamrates.patterns``). Lumi sections are recorded per sample, both those with selected events and
those listed in the file's LuminosityBlocks tree (inside the JSON), so the merge step can count each
lumi section exactly once.
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import re
import socket
import sys
import time

import numpy as np

from . import __version__
from .lumimask import LumiMask
from .partial import PartialResult
from .patterns import PatternAccumulator
from .readers import DEFAULT_BITS_EXCLUDE, DEFAULT_BITS_INCLUDE, FORMATS, detect_format, make_reader

log = logging.getLogger("steamrates.count")


# --------------------------------------------------------------------------------------------------
# Input files
# --------------------------------------------------------------------------------------------------

_LFN = re.compile(r"(/store/.*)$")


def canonical_name(path):
    """Key used to detect the same file given twice (e.g. once via xrootd, once via /eos)."""
    m = _LFN.search(path)
    return m.group(1) if m else os.path.realpath(path)


def collect_inputs(paths, list_files):
    """Input files from the command line and from list files; refuses duplicates."""
    inputs = list(paths)
    for lf in list_files:
        with open(lf) as f:
            inputs.extend(line.strip() for line in f if line.strip() and not line.startswith("#"))
    if not inputs:
        raise SystemExit("no input files given")
    seen, dups = {}, []
    for p in inputs:
        key = canonical_name(p)
        if key in seen:
            dups.append((seen[key], p))
        seen.setdefault(key, p)
    if dups:
        lines = "\n".join(f"  {a}\n  {b}" for a, b in dups[:10])
        raise SystemExit(f"{len(dups)} input file(s) given more than once, e.g.:\n{lines}")
    return inputs


# --------------------------------------------------------------------------------------------------
# Lumi sections and event IDs
# --------------------------------------------------------------------------------------------------

def _lumi_key(run, lumi):
    return (np.asarray(run, np.uint64) << np.uint64(32)) | np.asarray(lumi, np.uint64)


class LumiBook:
    """Counts selected events per (run, lumi) and remembers lumi sections listed in the files."""

    def __init__(self):
        self._event_keys = []
        self._file_keys = []

    def add_events(self, run, lumi):
        self._event_keys.append(_lumi_key(run, lumi))

    def add_file_lumis(self, run_lumi):
        self._file_keys.append(_lumi_key(run_lumi[:, 0], run_lumi[:, 1]))

    def arrays(self):
        ev = np.concatenate(self._event_keys) if self._event_keys else np.zeros(0, np.uint64)
        fl = np.concatenate(self._file_keys) if self._file_keys else np.zeros(0, np.uint64)
        ev_keys, ev_counts = np.unique(ev, return_counts=True)
        keys = np.union1d(ev_keys, np.unique(fl))
        n_events = np.zeros(len(keys), np.int64)
        n_events[np.searchsorted(keys, ev_keys)] = ev_counts
        return {"run": (keys >> np.uint64(32)).astype(np.uint32),
                "luminosityBlock": (keys & np.uint64(0xFFFFFFFF)).astype(np.uint32),
                "n_events": n_events, "in_file": np.isin(keys, fl)}


def count_duplicate_events(id_blocks):
    """Number of events whose (run, lumi, event) appeared before."""
    if not id_blocks:
        return 0
    ids = np.concatenate(id_blocks)
    return int(len(ids) - len(np.unique(ids)))


# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(prog="python3 -m steamrates.count", description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("inputs", nargs="*", help="input ntuple files (local paths or root:// URLs)")
    p.add_argument("--input-list", action="append", default=[], metavar="FILE",
                   help="text file with one input file per line (can be repeated)")
    p.add_argument("-o", "--output", required=True, help="output partial-result ROOT file")
    p.add_argument("--format", choices=("auto",) + FORMATS, default="auto",
                   help="ntuple format (default: detect from the first file)")
    p.add_argument("--process", help="process of the HLT results (EDM: e.g. MYHLT; NanoAOD: process of the "
                                     "HLT-path bits, e.g. HLT2 in re-HLT ntuples; default: the only one present)")
    p.add_argument("--l1-process", help="NanoAOD: process of the L1 bits (default: the only one present)")
    p.add_argument("--sample", default="default",
                   help="label of the sample (lumi sections are counted per sample at merge time)")
    p.add_argument("--lumi-json", help="certified-lumi JSON; events outside it are skipped (data only)")
    p.add_argument("--weight-branch", help="per-event weight branch, e.g. genWeight (NanoAOD MC)")
    p.add_argument("--bits-include", default=DEFAULT_BITS_INCLUDE,
                   help="NanoAOD: regex selecting trigger-bit branches (default: %(default)s)")
    p.add_argument("--bits-exclude", default=DEFAULT_BITS_EXCLUDE,
                   help="NanoAOD: regex of branches to skip (default: %(default)s)")
    p.add_argument("--chunk-size", type=int, default=200_000, help="events read at once (default: %(default)s)")
    p.add_argument("--max-events", type=int, help="stop after this many events (for tests)")
    p.add_argument("--allow-duplicate-events", action="store_true",
                   help="do not fail if the same (run, lumi, event) appears more than once")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s: %(message)s")
    t_start = time.time()
    inputs = collect_inputs(args.inputs, args.input_list)
    fmt = detect_format(inputs[0]) if args.format == "auto" else args.format
    mask = LumiMask.from_file(args.lumi_json) if args.lumi_json else None
    reader = make_reader(fmt, process=args.process, l1_process=args.l1_process, weight_branch=args.weight_branch,
                         bits_include=args.bits_include, bits_exclude=args.bits_exclude,
                         chunk_size=args.chunk_size)

    acc = PatternAccumulator()
    lumis = LumiBook()
    event_ids = []
    file_records = []
    menu = None
    mc_sums = {}
    n_read = n_selected = 0
    remaining = args.max_events

    for path in inputs:
        if remaining is not None and remaining <= 0:
            break
        info = reader.open(path)
        if menu is None and info.menu is not None:
            menu = info.menu
        elif info.menu is not None and menu.name != info.menu.name:
            log.warning("%s: menu %s differs from the first file's menu %s", path, info.menu.name, menu.name)
        if info.lumi_blocks is not None and len(info.lumi_blocks):
            lb = info.lumi_blocks
            lumis.add_file_lumis(lb if mask is None else lb[mask(lb[:, 0], lb[:, 1])])
        for k, v in info.mc_sums.items():
            mc_sums[k] = mc_sums.get(k, 0.0) + v

        f_read = f_sel = 0
        for chunk in reader.chunks(max_events=remaining):
            f_read += len(chunk)
            if mask is not None:
                chunk = chunk.select(mask(chunk.run, chunk.lumi))
            f_sel += len(chunk)
            if len(chunk) == 0:
                continue
            acc.add(chunk.bit_names, chunk.bits, chunk.weights)
            lumis.add_events(chunk.run, chunk.lumi)
            ids = np.empty(len(chunk), dtype=[("run", "u4"), ("lumi", "u4"), ("event", "u8")])
            ids["run"], ids["lumi"], ids["event"] = chunk.run, chunk.lumi, chunk.event
            event_ids.append(ids)
        n_read += f_read
        n_selected += f_sel
        if remaining is not None:
            remaining -= f_read
        # "menu": name of the menu read from this file (NanoAOD: first file only);
        # "menu_id": EDM only, hash of the path-name list (identical for files of the same menu)
        file_records.append({"path": path, "n_events": info.n_events, "n_read": f_read, "n_selected": f_sel,
                             "menu": info.menu.name if info.menu else None, "menu_id": info.menu_id})
        log.info("%s: %d events read, %d selected", path, f_read, f_sel)

    n_dup = count_duplicate_events(event_ids)
    if n_dup:
        msg = f"{n_dup} events appear more than once (same run, lumi, event)"
        if not args.allow_duplicate_events:
            raise SystemExit(msg + "; check the input files or use --allow-duplicate-events")
        log.warning(msg)

    table = acc.result(bit_names_if_empty=menu.trigger_paths if menu else ())
    totals = dict(table.totals(), n_events_read=n_read, n_events_selected=n_selected)
    metadata = {
        "steamrates_version": __version__,
        "created": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "host": socket.gethostname(),
        "command": " ".join([os.path.basename(sys.executable), "-m", "steamrates.count"] + sys.argv[1:]),
        "sample": args.sample,
        "format": fmt,
        "process": menu.process if menu else args.process,
        # NanoAOD: process the HLT-path bits and the L1 bits were taken from (see readers/nano.py)
        "bit_processes": getattr(reader, "selected_processes", None),
        "menu": menu.to_dict() if menu else None,
        "lumi_mask": mask.to_dict() if mask else None,
        "weight_branch": args.weight_branch,
        "inputs": file_records,
        "totals": totals,
        "mc_sums": mc_sums,
        "duplicate_events": n_dup,
        "runtime_s": round(time.time() - t_start, 2),
    }
    PartialResult(metadata, table, lumis.arrays()).write(args.output)
    log.info("wrote %s: %d events selected of %d read, %d distinct combinations, %.1f s",
             args.output, n_selected, n_read, table.n_rows, time.time() - t_start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
