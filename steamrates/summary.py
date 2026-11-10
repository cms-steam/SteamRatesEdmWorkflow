"""Inspect and add up partial results (no normalization yet; this prints raw counts).

Examples::

    python3 -m steamrates.summary partial_*.root                       # totals + top 25 bits
    python3 -m steamrates.summary partial_*.root --match '^HLT_Iso' --top 0
    python3 -m steamrates.summary partial_*.root --csv counts.csv --pure-reference '^HLT_'

``--pure-reference REGEX`` adds the "pure" (exclusive) count per bit: events where the bit fired and
no other bit matching REGEX fired. ``--strip-versions`` merges e.g. ``HLT_IsoMu24_v33`` and
``HLT_IsoMu24_v34`` from different menus into ``HLT_IsoMu24``.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys

import numpy as np

from .menu import strip_version
from .partial import PartialResult, merge_partials


def build_parser():
    p = argparse.ArgumentParser(prog="python3 -m steamrates.summary", description=__doc__.split("\n\n")[0])
    p.add_argument("partials", nargs="+", help="partial-result ROOT files")
    p.add_argument("--top", type=int, default=25, help="print the N bits with the most events (0: all)")
    p.add_argument("--match", help="only print bits matching this regex")
    p.add_argument("--pure-reference", help="regex of the bits that define pure counts, e.g. '^HLT_'")
    p.add_argument("--strip-versions", action="store_true", help="merge bits that differ only by _vN")
    p.add_argument("--weighted", action="store_true", help="print sums of weights instead of counts")
    p.add_argument("--csv", help="write all bit counts to this CSV file")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    partials = [PartialResult.read(p) for p in args.partials]
    table, lumi_sets, info = merge_partials(partials, rename=strip_version if args.strip_versions else None)
    what = "sumw" if args.weighted else "count"

    print(f"partials: {len(partials)}   input files: {info['n_inputs']}"
          + (f"   DUPLICATE INPUTS: {len(info['duplicate_inputs'])}" if info["duplicate_inputs"] else ""))
    print(f"events read: {info['n_events_read']}   selected: {info['n_events_selected']}   "
          f"sum of weights: {table.sumw.sum():.6g}   distinct combinations: {table.n_rows}   bits: {table.n_bits}")
    for k, v in info["mc_sums"].items():
        print(f"{k}: {v:.6g}")
    for sample, s in sorted(lumi_sets.items()):
        print(f"sample {sample!r}: {len(s['with_events'])} lumi sections with selected events, "
              f"{len(s['in_file'])} listed in the input files' LuminosityBlocks trees "
              f"(both after the lumi-JSON selection, if any)")

    totals = table.bit_totals(what)
    pure = None
    if args.pure_reference:
        rx = re.compile(args.pure_reference)
        pure = table.exclusive_totals(np.array([bool(rx.search(n)) for n in table.bit_names]), what)
    order = np.argsort(-totals, kind="stable")
    rx_match = re.compile(args.match) if args.match else None
    shown = [i for i in order if rx_match is None or rx_match.search(table.bit_names[i])]
    if args.top:
        shown = shown[:args.top]
    width = max([len(table.bit_names[i]) for i in shown] + [10])
    print(f"\n{'bit':{width}s} {what:>12s}" + (f" {'pure':>12s}" if pure is not None else ""))
    for i in shown:
        line = f"{table.bit_names[i]:{width}s} {totals[i]:12.6g}"
        print(line + (f" {pure[i]:12.6g}" if pure is not None else ""))

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["bit", what] + (["pure"] if pure is not None else []))
            for i in order:
                w.writerow([table.bit_names[i], totals[i]] + ([pure[i]] if pure is not None else []))
    return 0


if __name__ == "__main__":
    sys.exit(main())
