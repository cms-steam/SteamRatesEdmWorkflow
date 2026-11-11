"""Sample catalog: the central RAW inputs and the ntuples made from them (maintained by STEAM).

A rate measurement starts from a RAW input that STEAM provides centrally (e.g. EphemeralHLTPhysics
of one run, or a Phase-2 MinBias sample). Everything needed to normalize counts to rates belongs to
that RAW input: prescale, number of datasets, luminosity and lumi JSON (data), or cross section /
number of bunches (MC). Ntuples are made from a RAW input with a given CMSSW release and HLT (+ L1)
menu. A catalog ntuple entry therefore names its parent RAW entry and inherits the normalization
from it, instead of repeating it, and documents how it was produced.

Layout: a directory (default: ``sample_catalog/`` in the repository) with one YAML file per era or
campaign. IDs are unique across all files. Each file has two optional sections::

    raw:
      <id>:
        type: data | mc
        description: free text
        datasets: [/EphemeralHLTPhysics0/Run2026D-v1/RAW, ...]   # documentation (DAS later)
        inputs: [<glob, file or list file>, ...]                # where the RAW files are
        lumi_json: json/json_403663.txt     # data: selected lumi sections (relative to this file)
        runs: [403663]                      # data, optional: default = the runs of lumi_json
        normalization: ...                  # see below
        production: {...}                   # free-form hints for producing ntuples (later)
        notes: free text
    ntuples:
      <id>:
        raw: <id of the parent RAW entry>
        format: edm | nano
        inputs: [<glob, file or list file>, ...]
        process: MYHLT          # process of the HLT results (EDM) / HLT bits (NanoAOD)
        l1_process: null        # NanoAOD: process of the L1 bits
        weight_branch: null     # NanoAOD MC: e.g. genWeight
        cmssw: CMSSW_16_0_6_patch1
        hlt_menu: /dev/CMSSW_16_0_0/GRun/V59
        l1_menu: null
        global_tag: null
        produced_by: who/how
        date: 2026-11-09
        complete: true          # every event of each lumi section of the RAW input was processed
        lumi_json: null         # optional: lumi sections that are complete (intersected with the RAW JSON)
        description: free text
        notes: free text

Normalization (lumi in units of 1e34 cm^-2 s^-1)::

    data:            {mode: data, prescale: 1760, n_datasets: 8, lumi: 2.2}
        prescale   = prescale of each of the n_datasets datasets (the legacy ``hlt_ps``); the
                     datasets together record 1 in prescale/n_datasets events
        lumi       = average instantaneous luminosity of the selected lumi sections
    MC, σ x L:       {mode: cross_section, xsec_pb: 984.5, filter_eff: 1.0, target_lumi: 7.5}
    MC, per crossing:{mode: bunch_crossing, n_bunches: 2760}   (MinBias/NuGun at fixed pileup)

Values nobody has filled in yet can be written as ``TODO``; such an entry can be listed, but a
measurement that uses it stops with a clear message.

Why ``complete`` matters (data): the normalization divides by the number of lumi sections found in
the ntuples. If some RAW files of a lumi section were not processed (e.g. 1 of the 8 Ephemeral
datasets), the lumi section is still found in the other datasets' files, and the rates come out
too low. ``complete: true`` states that this cannot happen; otherwise give a ``lumi_json`` with the
complete lumi sections only.

Command line::

    python3 -m steamrates catalog list [--catalog DIR]
    python3 -m steamrates catalog show ID [--catalog DIR]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from dataclasses import dataclass, field

import yaml

from .schema import (TODO, ConfigError, check_id, check_keys, find_todos, str_list, value)

NORMALIZATION_KEYS = {
    "data": ("mode", "prescale", "n_datasets", "lumi"),
    "cross_section": ("mode", "xsec_pb", "filter_eff", "target_lumi"),
    "bunch_crossing": ("mode", "n_bunches"),
}
MODES_FOR_TYPE = {"data": ("data",), "mc": ("cross_section", "bunch_crossing")}
RAW_KEYS = ("type", "description", "datasets", "inputs", "lumi_json", "runs", "normalization", "production",
            "notes")
NTUPLE_KEYS = ("raw", "format", "inputs", "process", "l1_process", "weight_branch", "cmssw", "hlt_menu",
               "l1_menu", "global_tag", "produced_by", "date", "complete", "lumi_json", "description", "notes")
NTUPLE_FORMATS = ("edm", "nano")


def default_dir():
    """``sample_catalog/`` next to the ``steamrates`` package (the repository root)."""
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sample_catalog")


def _abs(path, base_dir):
    if path is None or path == TODO:
        return path
    return path if os.path.isabs(path) else os.path.normpath(os.path.join(base_dir, path))


# --------------------------------------------------------------------------------------------------
# Entries
# --------------------------------------------------------------------------------------------------

def parse_normalization(d, sample_type, where):
    check_keys(d, sum(NORMALIZATION_KEYS.values(), ()), where, required=("mode",))
    mode = value(d, "mode", (str,), where)
    if mode not in MODES_FOR_TYPE[sample_type]:
        raise ConfigError(f"{where}.mode: {mode!r} is not possible for type {sample_type!r}; "
                          f"choose from {', '.join(MODES_FOR_TYPE[sample_type])}")
    check_keys(d, NORMALIZATION_KEYS[mode], where)
    positive = dict(check=lambda x: x > 0, check_msg="must be > 0")
    if mode == "data":
        return {"mode": mode,
                "prescale": value(d, "prescale", (float,), where, allow_todo=True, **positive),
                "n_datasets": value(d, "n_datasets", (int,), where, default=1, **positive),
                "lumi": value(d, "lumi", (float,), where, allow_todo=True, **positive)}
    if mode == "cross_section":
        return {"mode": mode,
                "xsec_pb": value(d, "xsec_pb", (float,), where, allow_todo=True, **positive),
                "filter_eff": value(d, "filter_eff", (float,), where, default=1.0, allow_todo=True,
                                    check=lambda x: 0 < x <= 1, check_msg="must be in (0, 1]"),
                "target_lumi": value(d, "target_lumi", (float,), where, allow_todo=True, **positive)}
    return {"mode": mode, "n_bunches": value(d, "n_bunches", (int,), where, allow_todo=True, **positive)}


@dataclass
class RawEntry:
    id: str
    type: str
    normalization: dict
    description: str = ""
    datasets: list = field(default_factory=list)
    inputs: list = field(default_factory=list)
    lumi_json: str | None = None        # absolute path
    runs: list | None = None
    production: dict = field(default_factory=dict)
    notes: str = ""
    base_dir: str = "."                 # relative input paths are relative to this
    source: str | None = None           # YAML file (None: inline in a measurement config)

    @classmethod
    def parse(cls, entry_id, d, where, base_dir, source=None):
        check_keys(d, RAW_KEYS, where, required=("type", "normalization"))
        sample_type = value(d, "type", (str,), where, check=lambda t: t in MODES_FOR_TYPE,
                            check_msg="must be 'data' or 'mc'")
        lumi_json = _abs(value(d, "lumi_json", (str, type(None)), where, default=None), base_dir)
        if sample_type == "mc" and lumi_json:
            raise ConfigError(f"{where}.lumi_json: MC samples have no lumi JSON")
        runs = value(d, "runs", (list, type(None)), where, default=None)
        if runs is not None and not all(isinstance(r, int) for r in runs):
            raise ConfigError(f"{where}.runs: expected a list of run numbers")
        return cls(id=entry_id, type=sample_type,
                   normalization=parse_normalization(value(d, "normalization", (dict,), where), sample_type,
                                                     f"{where}.normalization"),
                   description=value(d, "description", (str,), where, default=""),
                   datasets=str_list(d, "datasets", where, default=[]),
                   inputs=str_list(d, "inputs", where, default=[]),
                   lumi_json=lumi_json, runs=runs,
                   production=value(d, "production", (dict, type(None)), where, default=None) or {},
                   notes=value(d, "notes", (str,), where, default=""),
                   base_dir=base_dir, source=source)

    def known_runs(self):
        """Runs this RAW input contains (``runs``, else the runs of ``lumi_json``), or ``None``."""
        if self.runs is not None:
            return sorted(self.runs)
        if self.lumi_json and self.lumi_json != TODO and os.path.exists(self.lumi_json):
            with open(self.lumi_json) as f:
                return sorted(int(r) for r in json.load(f))
        return None

    def todos(self):
        return find_todos(self.to_dict(), f"raw {self.id}")

    def to_dict(self):
        d = {"id": self.id, "type": self.type, "description": self.description, "datasets": self.datasets,
             "inputs": self.inputs, "lumi_json": self.lumi_json, "runs": self.runs,
             "normalization": self.normalization, "production": self.production, "notes": self.notes,
             "source": self.source}
        return {k: v for k, v in d.items() if v not in (None, "", [], {})}


@dataclass
class NtupleEntry:
    id: str
    raw: object                         # id of the RAW entry (str), or an inline RawEntry
    format: str | None
    inputs: list
    process: str | None = None
    l1_process: str | None = None
    weight_branch: str | None = None
    cmssw: str | None = None
    hlt_menu: str | None = None
    l1_menu: str | None = None
    global_tag: str | None = None
    produced_by: str | None = None
    date: str | None = None
    complete: bool | None = None
    lumi_json: str | None = None        # absolute path
    description: str = ""
    notes: str = ""
    base_dir: str = "."
    source: str | None = None

    @classmethod
    def parse(cls, entry_id, d, where, base_dir, source=None, inline_raw_ok=False, require=("complete",)):
        check_keys(d, NTUPLE_KEYS, where, required=("raw", "inputs") + tuple(require))
        raw = d["raw"]
        if isinstance(raw, dict) and inline_raw_ok:
            raw = RawEntry.parse(f"{entry_id}.raw", raw, f"{where}.raw", base_dir, source=None)
        else:
            raw = check_id(value(d, "raw", (str,), where), f"{where}.raw")
        opt_str = (str, type(None))
        entry = cls(id=entry_id, raw=raw,
                    format=value(d, "format", opt_str, where, default=None,
                                 check=lambda f: f in NTUPLE_FORMATS, check_msg="must be 'edm' or 'nano'"),
                    inputs=str_list(d, "inputs", where),
                    complete=value(d, "complete", (bool, type(None)), where, default=None),
                    lumi_json=_abs(value(d, "lumi_json", opt_str, where, default=None), base_dir),
                    description=value(d, "description", (str,), where, default=""),
                    notes=value(d, "notes", (str,), where, default=""),
                    base_dir=base_dir, source=source)
        for key in ("process", "l1_process", "weight_branch", "cmssw", "hlt_menu", "l1_menu", "global_tag",
                    "produced_by", "date"):
            setattr(entry, key, value(d, key, opt_str, where, default=None))
        if not entry.inputs:
            raise ConfigError(f"{where}.inputs: no inputs given")
        return entry

    @property
    def raw_id(self):
        return self.raw.id if isinstance(self.raw, RawEntry) else self.raw

    def to_dict(self):
        d = {k: getattr(self, k) for k in NTUPLE_KEYS if k != "raw"}
        d = {"id": self.id, "raw": self.raw_id, **d, "source": self.source}
        return {k: v for k, v in d.items() if v not in (None, "", [], {})}


# --------------------------------------------------------------------------------------------------
# The catalog
# --------------------------------------------------------------------------------------------------

class Catalog:
    def __init__(self, directory, raw, ntuples, files):
        self.directory = directory
        self.raw = raw            # id -> RawEntry
        self.ntuples = ntuples    # id -> NtupleEntry
        self.files = files        # YAML files read

    @classmethod
    def load(cls, directory=None):
        directory = os.path.abspath(directory or default_dir())
        if not os.path.isdir(directory):
            raise ConfigError(f"sample catalog: {directory} is not a directory")
        files = sorted(glob.glob(os.path.join(directory, "*.yaml")) + glob.glob(os.path.join(directory, "*.yml")))
        raw, ntuples, origin = {}, {}, {}
        for path in files:
            name = os.path.relpath(path, directory)
            with open(path) as f:
                try:
                    doc = yaml.safe_load(f) or {}
                except yaml.YAMLError as e:
                    raise ConfigError(f"{name}: invalid YAML ({e})") from None
            check_keys(doc, ("raw", "ntuples"), name)
            for kind, store, parser in (("raw", raw, RawEntry.parse), ("ntuples", ntuples, NtupleEntry.parse)):
                section = doc.get(kind) or {}
                if not isinstance(section, dict):
                    raise ConfigError(f"{name}: {kind}: expected a mapping of ID -> entry")
                for entry_id, d in section.items():
                    where = f"{name}: {kind}.{entry_id}"
                    check_id(entry_id, where)
                    if entry_id in origin:
                        raise ConfigError(f"{where}: ID already used in {origin[entry_id]}")
                    origin[entry_id] = name
                    store[entry_id] = parser(entry_id, d, where, os.path.dirname(path), source=path)
        for nt in ntuples.values():
            if nt.raw not in raw:
                raise ConfigError(f"{os.path.relpath(nt.source, directory)}: ntuples.{nt.id}.raw: "
                                  f"unknown RAW entry {nt.raw!r}")
        return cls(directory, raw, ntuples, files)

    def get_raw(self, entry_id):
        if entry_id not in self.raw:
            hint = " (that is an ntuples entry)" if entry_id in self.ntuples else ""
            raise ConfigError(f"sample catalog {self.directory}: no RAW entry {entry_id!r}{hint}")
        return self.raw[entry_id]

    def get_ntuples(self, entry_id):
        if entry_id not in self.ntuples:
            hint = " (that is a RAW entry)" if entry_id in self.raw else ""
            raise ConfigError(f"sample catalog {self.directory}: no ntuples entry {entry_id!r}{hint}")
        return self.ntuples[entry_id]


# --------------------------------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------------------------------

def _norm_summary(n):
    g = lambda x: f"{x:g}" if isinstance(x, (int, float)) else str(x)   # noqa: E731  (TODO stays TODO)
    if n["mode"] == "data":
        return f"PS {g(n['prescale'])} / {n['n_datasets']} dataset(s), lumi {g(n['lumi'])}"
    if n["mode"] == "cross_section":
        return f"σ {g(n['xsec_pb'])} pb x eff {g(n['filter_eff'])}, target lumi {g(n['target_lumi'])}"
    return f"{g(n['n_bunches'])} bunches"


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates catalog", description="Browse the sample catalog.")
    p.add_argument("action", choices=("list", "show"))
    p.add_argument("id", nargs="?", help="entry to show")
    p.add_argument("--catalog", help="catalog directory (default: %(default)s)", default=default_dir())
    args = p.parse_args(argv)
    try:
        cat = Catalog.load(args.catalog)
    except ConfigError as e:
        raise SystemExit(f"error: {e}") from None

    if args.action == "list":
        print(f"Sample catalog {cat.directory} ({len(cat.files)} files)\n")
        print(f"RAW inputs ({len(cat.raw)}):")
        for e in cat.raw.values():
            todo = "  [has TODO]" if e.todos() else ""
            print(f"  {e.id:45s} {e.type:4s}  {_norm_summary(e.normalization)}{todo}")
        print(f"\nntuples ({len(cat.ntuples)}):")
        for e in cat.ntuples.values():
            print(f"  {e.id:45s} {e.format or '?':4s}  {e.hlt_menu or '(menu not named)'}  <- {e.raw_id}")
        return 0

    if not args.id:
        raise SystemExit("error: 'show' needs an ID")
    if args.id in cat.ntuples:
        nt = cat.ntuples[args.id]
        out = {"ntuples": {nt.id: nt.to_dict()}, "raw": {nt.raw_id: cat.raw[nt.raw_id].to_dict()}}
    elif args.id in cat.raw:
        out = {"raw": {args.id: cat.raw[args.id].to_dict()},
               "ntuples made from it": sorted(n.id for n in cat.ntuples.values() if n.raw_id == args.id)}
    else:
        raise SystemExit(f"error: no entry {args.id!r} in {cat.directory}")
    yaml.safe_dump(out, sys.stdout, sort_keys=False, allow_unicode=True, width=120)
    return 0


if __name__ == "__main__":
    sys.exit(main())
