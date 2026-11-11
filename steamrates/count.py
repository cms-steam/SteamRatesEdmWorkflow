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
import json
import logging
import os
import socket
import sys
import time

import numpy as np

from . import __version__
from .counters import DEFAULT_PURE_REFERENCE, DEFAULT_TOTALS, BitCounters
from .inputs import collect_inputs
from .lumimask import LumiMask
from .partial import PartialResult
from .patterns import PatternAccumulator
from .readers import DEFAULT_BITS_EXCLUDE, DEFAULT_BITS_INCLUDE, FORMATS, detect_format, make_reader

log = logging.getLogger("steamrates.count")


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
    p.add_argument("--chunk-size", type=int, default=50_000,
                   help="events read at once; bounds the memory use (default: %(default)s)")
    p.add_argument("--max-events", type=int, help="stop after this many events (for tests)")
    p.add_argument("--allow-duplicate-events", action="store_true",
                   help="do not fail if the same (run, lumi, event) appears more than once")
    g = p.add_argument_group("old-style counters (filled next to the combination table; see steamrates.counters)")
    g.add_argument("--pure-reference", default=DEFAULT_PURE_REFERENCE,
                   help="regex of the bits that define pure counts (default: %(default)s)")
    g.add_argument("--exclude", action="append", default=[], metavar="REGEX",
                   help="bits left out of the pure reference and the totals (can be repeated)")
    g.add_argument("--total", action="append", default=[], metavar="LABEL=REGEX",
                   help="OR total over the bits matching REGEX (can be repeated; default: "
                        + ", ".join(f"{k}={v}" for k, v in DEFAULT_TOTALS.items()) + ")")
    g.add_argument("--no-strip-versions", action="store_true",
                   help="match the patterns against the versioned names (HLT_X_v3) instead of HLT_X")
    g.add_argument("--no-table", action="store_true",
                   help="do not build the combination table; only the counters (for very large samples)")
    p.add_argument("--job-info", help=argparse.SUPPRESS)   # JSON stored as metadata["job"] (steamrates.jobrun)
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

    total_regexes = DEFAULT_TOTALS
    if args.total:
        try:
            total_regexes = dict(t.split("=", 1) for t in args.total)
        except ValueError:
            raise SystemExit("--total must look like LABEL=REGEX") from None
    counters = BitCounters(args.pure_reference, args.exclude, total_regexes,
                           strip_versions=not args.no_strip_versions)
    sum_w = sum_w2 = 0.0           # over all selected events (the table has these too)
    acc = None if args.no_table else PatternAccumulator()
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
            if acc is not None:
                acc.add(chunk.bit_names, chunk.bits, chunk.weights)
            counters.add(chunk.bit_names, chunk.bits, chunk.weights)
            w = np.ones(len(chunk)) if chunk.weights is None else chunk.weights
            sum_w += float(w.sum())
            sum_w2 += float((w * w).sum())
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

    if acc is not None:
        table = acc.result(bit_names_if_empty=menu.trigger_paths if menu else counters.names)
        bit_names = table.bit_names
        totals = dict(table.totals(), n_events_read=n_read, n_events_selected=n_selected)
    else:
        table = None
        bit_names = counters.names or (list(menu.trigger_paths) if menu else [])
        totals = {"n_events": n_selected, "sumw": sum_w, "sumw2": sum_w2, "n_rows": None,
                  "n_events_read": n_read, "n_events_selected": n_selected}
    metadata = {
        "steamrates_version": __version__,
        "created": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "host": socket.gethostname(),
        "command": " ".join([os.path.basename(sys.executable), "-m", "steamrates.count"]
                            + (sys.argv[1:] if argv is None else list(argv))),
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
        "counters": counters.definition(),
        "job": json.loads(args.job_info) if args.job_info else None,
        "bits": list(bit_names),
        "runtime_s": round(time.time() - t_start, 2),
    }
    PartialResult(metadata, table, lumis.arrays(), counters.arrays(bit_names)).write(args.output)
    log.info("wrote %s: %d events selected of %d read, %s, %.1f s", args.output, n_selected, n_read,
             f"{table.n_rows} distinct combinations" if table is not None else "counters only (no table)",
             time.time() - t_start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
