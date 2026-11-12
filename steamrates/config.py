"""Measurement config: the YAML file a user writes for one rate measurement.

A measurement runs on **one** input. The input is either a set of ntuples from the sample catalog,
or the user's own ntuples together with the RAW input they were made from (which provides the
normalization)::

    name: rates_403663_HLTPhysics          # letters, digits, _ . + -
    catalog: null                           # sample catalog directory (default: sample_catalog/)
    input:
      ntuples: Run2026D_403663_HLTPhysics_PromptNano       # A) central ntuples (catalog ID)
      # ntuples:                                            # B) own ntuples
      #   raw: Run2026D_403663_EphemeralHLTPhysics          #    catalog RAW entry (or an inline
      #   inputs: [/eos/user/.../*.root, files.txt]         #    RAW entry: a mapping with the same
      #   format: edm                                       #    keys as in the catalog)
      #   process: MYHLT
      #   hlt_menu: /dev/CMSSW_16_0_0/GRun/V62
      #   complete: true
      # produce: {...}                                      # C) later: produce the ntuples from RAW
    target_lumi: 2.0    # optional (1e34 cm^-2 s^-1); data and MC cross_section only
    counting: {...}     # see COUNTING below; process/format/... default to the ntuples entry
    jobs: {...}
    batch: {...}
    merge: {...}

All keys are checked: an unknown key (typo) is an error. Relative paths are relative to the config
file. :func:`load_measurement` validates the config, resolves the input against the catalog and
returns a :class:`Measurement` with every setting filled in.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
from dataclasses import dataclass, field

import yaml

from .catalog import Catalog, NtupleEntry, RawEntry, default_dir
from .lumimask import LumiMask
from .readers import DEFAULT_BITS_EXCLUDE, DEFAULT_BITS_INCLUDE, FORMATS
from .schema import ConfigError, check_id, check_keys, check_regex, find_todos, value

TOP_KEYS = ("name", "catalog", "input", "target_lumi", "counting", "jobs", "batch", "merge")
PRODUCE_KEYS = ("raw", "cmssw_base", "hlt_menu", "global_tag", "l1", "output", "keep_ntuples")
FLAVOURS = ("espresso", "microcentury", "longlunch", "workday", "tomorrow", "testmatch", "nextweek")

_OPT_STR = (str, type(None))
_POSITIVE = dict(check=lambda x: x > 0, check_msg="must be > 0")

# Each section: key -> (allowed types, default, extra checks for schema.value)
COUNTING = {
    "format": (_OPT_STR, None, dict(check=lambda f: f in FORMATS, check_msg=f"must be one of {FORMATS}")),
    "process": (_OPT_STR, None, {}),
    "l1_process": (_OPT_STR, None, {}),
    "weight_branch": (_OPT_STR, None, {}),
    "bits_include": ((str,), DEFAULT_BITS_INCLUDE, {}),
    "bits_exclude": ((str,), DEFAULT_BITS_EXCLUDE, {}),
    "chunk_size": ((int,), 50_000, _POSITIVE),
    "allow_duplicate_events": ((bool,), False, {}),
    "table": ((bool,), True, {}),        # false: counters only (count --no-table), for huge samples
}
JOBS = {
    "events_per_job": ((int,), 1_000_000, _POSITIVE),
    "files_per_job": ((int, type(None)), None, _POSITIVE),
    "check_inputs": ((bool,), True, {}),
    "check_threads": ((int,), 8, _POSITIVE),
}
BATCH = {
    "flavour": ((str,), "espresso", dict(check=lambda f: f in FLAVOURS, check_msg=f"must be one of {FLAVOURS}")),
    "request_memory_mb": ((int,), 2000, _POSITIVE),
    "proxy": (((str, bool)), "auto", dict(check=lambda p: p in ("auto", True, False),
                                          check_msg="must be auto, true or false")),
    "cmssw_base": (_OPT_STR, None, {}),
}
MERGE = {
    "strip_versions": ((bool,), True, {}),
    "pure_reference": ((str,), "^HLT_", {}),
    "default_exclude": ((bool,), True, {}),   # also exclude the paths of DEFAULT_EXCLUDE
    "exclude": ((list,), [], {}),
    "scouting_paths": ((str,), "^DST_.*Scouting", {}),   # scouting paths of menus without streams (Phase-2)
    "totals": ((dict,), {"HLT": "^HLT_", "L1": "^L1_(?!FinalOR|UnprefireableEvent)"}, {}),
    "datasets": ((str, type(None)), "menu", {}),
    "streams": ((str, type(None)), "menu", {}),
    "overlaps": ((dict,), None, {}),     # default: OVERLAPS
}
# Overlap matrices computed by the merge (steamrates.rates.default_overlaps).
# !!! "paths" (HLT x HLT) is the largest and slowest one. Data: cheap (403663 baseline, 13.5M events:
# !!! +1.5 s, +44 MB), because events with the same fired paths share one row of the table. MC: every
# !!! event is its own row, so it costs ~ events x (fired paths per event)^2: about +13 s and +150 MB
# !!! per 100k events with 100 fired paths each (700-path menu). Switch it off for large MC samples.
OVERLAPS = {"datasets": True, "streams": True, "paths_datasets": True, "l1_datasets": True, "l1_streams": True,
            "paths": True}

# Paths left out of every combined quantity (totals, dataset/stream rates, pure-rate references) in
# every measurement, unless ``merge.default_exclude: false``; ``merge.exclude`` adds more for a
# measurement. They keep their own rate. Only paths with a reason valid for every sample belong here:
# HLT_Physics and HLT_EphemeralPhysics take (prescaled) unbiased events. They select the HLTPhysics /
# EphemeralHLTPhysics samples, so in prompt ntuples (online decisions) they fire in every event, and
# MC menus run them without prescale. (The legacy Rates/triggerstoexclude.py list only patched the
# legacy pure-rate count and is not needed here.) The merge lists every excluded bit in
# results/excluded_paths.json.
DEFAULT_EXCLUDE = [
    "^HLT_Physics$",
    "^HLT_EphemeralPhysics$",
]


def exclude_patterns(merge):
    """The exclude patterns in effect: DEFAULT_EXCLUDE (unless ``default_exclude`` is false) + ``exclude``."""
    patterns = (DEFAULT_EXCLUDE if merge.get("default_exclude", True) else []) + list(merge.get("exclude") or [])
    return list(dict.fromkeys(patterns))


def _section(doc, name, spec, where):
    d = doc.get(name) or {}
    check_keys(d, spec, f"{where}{name}")
    return {key: value(d, key, types, f"{where}{name}", default=copy.deepcopy(default), **checks)
            for key, (types, default, checks) in spec.items()}


def _abs(path, base_dir):
    return path if path is None or os.path.isabs(path) else os.path.normpath(os.path.join(base_dir, path))


@dataclass
class Measurement:
    """A validated measurement config with everything resolved."""

    name: str
    config_path: str
    config_text: str
    ntuples: NtupleEntry
    raw: RawEntry
    ntuples_in_catalog: bool
    raw_in_catalog: bool
    catalog_dir: str | None
    target_lumi: float | None
    lumi_mask: LumiMask | None
    counting: dict
    jobs: dict
    batch: dict
    merge: dict
    warnings: list = field(default_factory=list)

    @property
    def is_data(self):
        return self.raw.type == "data"

    def normalization(self):
        """The RAW entry's normalization plus the target luminosity of this measurement."""
        return dict(self.raw.normalization, target_lumi=self.target_lumi)

    def settings(self):
        """Everything that defines the measurement (its sha1 detects changed settings)."""
        return {
            "name": self.name,
            "input": {"ntuples": self.ntuples.to_dict(), "raw": self.raw.to_dict(),
                      "ntuples_in_catalog": self.ntuples_in_catalog, "raw_in_catalog": self.raw_in_catalog},
            "normalization": self.normalization(),
            "lumi_mask": self.lumi_mask.to_json() if self.lumi_mask else None,
            "counting": self.counting, "jobs": self.jobs, "batch": self.batch, "merge": self.merge,
        }

    def settings_sha1(self):
        return hashlib.sha1(json.dumps(self.settings(), sort_keys=True).encode()).hexdigest()


def load_measurement(path, catalog_dir=None):
    """Read, validate and resolve a measurement config. Raises :class:`ConfigError`."""
    path = os.path.abspath(path)
    where = os.path.basename(path) + ": "
    base_dir = os.path.dirname(path)
    with open(path) as f:
        text = f.read()
    try:
        doc = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"{where}invalid YAML ({e})") from None
    check_keys(doc, TOP_KEYS, where.rstrip(": "), required=("name", "input"))
    name = check_id(doc["name"], f"{where}name")
    warnings = []

    # ---- catalog (loaded only if the input refers to it) ----------------------------------------
    cat_dir = catalog_dir or _abs(value(doc, "catalog", _OPT_STR, where.rstrip(": "), default=None), base_dir) \
        or default_dir()
    catalog = None

    def get_catalog():
        nonlocal catalog
        if catalog is None:
            catalog = Catalog.load(cat_dir)
        return catalog

    # ---- input ------------------------------------------------------------------------------------
    inp = doc["input"]
    check_keys(inp, ("ntuples", "produce"), f"{where}input")
    if ("ntuples" in inp) == ("produce" in inp):
        raise ConfigError(f"{where}input: give exactly one of 'ntuples' and 'produce'")
    if "produce" in inp:
        check_keys(inp["produce"], PRODUCE_KEYS, f"{where}input.produce", required=("raw",))
        raise ConfigError(f"{where}input.produce: producing ntuples from RAW is not implemented yet; "
                          "use input.ntuples")
    spec = inp["ntuples"]
    if isinstance(spec, str):
        ntuples = get_catalog().get_ntuples(spec)
        raw = catalog.raw[ntuples.raw_id]
        ntuples_in_catalog = raw_in_catalog = True
    else:
        ntuples = NtupleEntry.parse(f"{name}.ntuples", spec, f"{where}input.ntuples", base_dir, source=path,
                                    inline_raw_ok=True, require=())
        ntuples_in_catalog = False
        if isinstance(ntuples.raw, RawEntry):
            raw, raw_in_catalog = ntuples.raw, False
            warnings.append(f"the RAW input is defined in the config, not taken from the sample catalog "
                            f"({cat_dir}); its normalization is not reviewed by STEAM")
        else:
            raw, raw_in_catalog = get_catalog().get_raw(ntuples.raw), True

    todos = raw.todos() + find_todos(ntuples.to_dict(), f"ntuples {ntuples.id}")
    if todos:
        raise ConfigError(f"{where}the input has values that are not filled in yet (TODO): {', '.join(todos)}")

    # ---- settings sections ------------------------------------------------------------------------
    counting = _section(doc, "counting", COUNTING, where)
    jobs = _section(doc, "jobs", JOBS, where)
    batch = _section(doc, "batch", BATCH, where)
    batch["cmssw_base"] = _abs(batch["cmssw_base"], base_dir)
    merge = _section(doc, "merge", MERGE, where)
    _check_merge(merge, base_dir, f"{where}merge")
    for key in ("bits_include", "bits_exclude"):
        check_regex(counting[key], f"{where}counting.{key}")

    # The ntuples entry provides defaults for the counting options; a different explicit value
    # for the format is a contradiction, for the processes it is a deliberate choice.
    if counting["format"] and ntuples.format and counting["format"] != ntuples.format:
        raise ConfigError(f"{where}counting.format: {counting['format']!r}, but the ntuples are "
                          f"{ntuples.format!r}")
    for key in ("format", "process", "l1_process", "weight_branch"):
        if counting[key] is None:
            counting[key] = getattr(ntuples, key)
    if counting["format"] == "edm" and (counting["l1_process"] or counting["weight_branch"]):
        raise ConfigError(f"{where}counting: EDM TriggerResults ntuples have no L1 bits and no weight branch")
    if jobs["files_per_job"] is None and not jobs["check_inputs"]:
        raise ConfigError(f"{where}jobs: without check_inputs the event counts are unknown; "
                          "set files_per_job")

    # ---- normalization ----------------------------------------------------------------------------
    norm = raw.normalization
    target_lumi = value(doc, "target_lumi", (float, type(None)), where.rstrip(": "), default=None, **_POSITIVE)
    if norm["mode"] == "bunch_crossing":
        if target_lumi is not None:
            raise ConfigError(f"{where}target_lumi: not possible for a bunch_crossing sample "
                              "(the pileup, and thus the luminosity, is fixed by the sample)")
    elif target_lumi is None:
        target_lumi = norm["lumi"] if norm["mode"] == "data" else norm["target_lumi"]

    # ---- lumi mask (data): RAW selection, intersected with the complete lumi sections ------------
    lumi_mask = None
    if raw.type == "mc":
        if ntuples.lumi_json:
            raise ConfigError(f"{where}input.ntuples.lumi_json: MC samples have no lumi JSON")
    else:
        for source in (raw.lumi_json, ntuples.lumi_json):
            if not source:
                continue
            if not os.path.exists(source):
                raise ConfigError(f"{where}lumi JSON {source} does not exist")
            try:
                mask = LumiMask.from_file(source)
            except (ValueError, TypeError) as e:
                raise ConfigError(f"{where}lumi JSON {source}: {e}") from None
            lumi_mask = mask if lumi_mask is None else lumi_mask.intersection(mask)
        if lumi_mask is None:
            warnings.append("no lumi JSON: every lumi section in the ntuples is used")
        elif not lumi_mask.ranges:
            raise ConfigError(f"{where}the lumi JSONs ({raw.lumi_json}, {ntuples.lumi_json}) have no "
                              "lumi section in common")
        if ntuples.complete is not True and not ntuples.lumi_json:
            warnings.append("the ntuples are not marked complete and have no lumi_json of complete lumi "
                            "sections: if RAW files of a counted lumi section were not processed, the "
                            "rates come out too low")

    return Measurement(name=name, config_path=path, config_text=text, ntuples=ntuples, raw=raw,
                       ntuples_in_catalog=ntuples_in_catalog, raw_in_catalog=raw_in_catalog,
                       catalog_dir=cat_dir if (ntuples_in_catalog or raw_in_catalog) else None,
                       target_lumi=target_lumi, lumi_mask=lumi_mask, counting=counting, jobs=jobs,
                       batch=batch, merge=merge, warnings=warnings)


def _check_merge(merge, base_dir, where):
    check_regex(merge["pure_reference"], f"{where}.pure_reference")
    check_regex(merge["scouting_paths"], f"{where}.scouting_paths")
    for i, pattern in enumerate(merge["exclude"]):
        if not isinstance(pattern, str):
            raise ConfigError(f"{where}.exclude[{i}]: expected a regular expression, got {pattern!r}")
        check_regex(pattern, f"{where}.exclude[{i}]")
    for label, pattern in merge["totals"].items():
        if not isinstance(pattern, str):
            raise ConfigError(f"{where}.totals.{label}: expected a regular expression, got {pattern!r}")
        check_regex(pattern, f"{where}.totals.{label}")
    overlaps = merge["overlaps"] or {}
    check_keys(overlaps, tuple(OVERLAPS), f"{where}.overlaps")
    merge["overlaps"] = {k: value(overlaps, k, (bool,), f"{where}.overlaps", default=d) for k, d in OVERLAPS.items()}
    for key in ("datasets", "streams"):
        v = merge[key]
        if v not in (None, "menu"):
            merge[key] = _abs(v, base_dir)
            if not os.path.exists(merge[key]):
                raise ConfigError(f"{where}.{key}: {merge[key]} does not exist (use 'menu', null or a YAML file)")


__all__ = ["Measurement", "load_measurement", "ConfigError", "DEFAULT_EXCLUDE", "exclude_patterns"]
