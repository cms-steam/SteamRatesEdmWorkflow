"""Trigger maps: what each bit is, which datasets and streams it feeds, and the categories and pure-rate
references that follow from the stream names.

The maps come from the HLT menu embedded in the ntuples (``merge.datasets/streams: menu``), from
YAML files (``{dataset: [paths]}`` and ``{stream: [datasets]}``), or are absent (Phase-2 python menus
have no datasets or streams). In the legacy workflow they came from ConfDB via ``STEAM_maps``.

Kinds of bits
-------------
    hlt            trigger paths: HLT_*, DST_*, AlCa_*, MC_*, Status_*, HLTriggerFirstPath, ...
    dataset_path   Dataset_<name>: the decision of a dataset (OR of its paths x dataset prescale)
    l1             L1 bits (names starting with "L1"). L1 *seeds* are all of them except
                   L1_FinalOR* and L1_UnprefireableEvent*. Phase-2 ntuples store each seed twice,
                   L1_<algo>_final and L1_<algo>_beforePrescale: the suffix is the seed's *flavour*,
                   and pure rates and totals are computed per flavour. Run-3 NanoAOD L1 bits have no
                   suffix (they are the final decisions).

Split and combined datasets and streams
---------------------------------------
A stream has a maximum bandwidth. Above it, the stream and its datasets are *split*: the menu has N
datasets with the same paths (EGamma0 ... EGamma5), and the prescale module of each Dataset_ path
assigns the events round robin (event 1 -> EGamma0, event 2 -> EGamma1, ...), so every selected event
is written to exactly one of them. A split stream holds one or more of these datasets (stream
ParkingAnomalyDetection0 = datasets ParkingAnomalyDetection0 and 1); the menu gives the assignment.
Analyses use the split datasets together.

    split datasets    as in the menu; their rates are what each one writes (bandwidth)
    combined dataset  the split datasets with the same paths and the same name apart from a trailing
                      number (EGamma = EGamma0-5); an unsplit dataset is its own combined dataset
    combined stream   likewise: streams with the same name apart from a trailing number whose
                      datasets belong to the same combined datasets (PhysicsEGamma = PhysicsEGamma0-5)

Combined datasets and streams fire when one of their (non-excluded) paths fires: for split ones that
is the sum of the split ones, as the round robin gives each event to exactly one. Pure rates and
overlaps are computed for the combined ones. Datasets with the same paths but without a trailing
number (TestDataRaw/TestDataScouting) are not split: each gets every event.

Categories (from the stream names)
----------------------------------
    stream    Physics* -> standard, Parking* -> parking, Scouting* -> scouting,
              everything else -> other (calibration, monitoring, express, ...)
    dataset   the category of its stream
    path      the categories of all datasets it is in; its main category is the first of
              standard, parking, scouting, other, none (none = in no dataset)
A path is a *physics* path if one of its categories is standard, parking or scouting. Many paths
are in a standard dataset and also in a monitoring one (Express, HLTMonitor, OnlineMonitor): they
count for both categories in the totals, and as standard in the pure rates.

If the maps have datasets but no streams, all datasets are taken as standard. Without datasets
(Phase-2), paths matching ``merge.scouting_paths`` (default ``^DST_.*Scouting``) are scouting, paths
matching ``merge.pure_reference`` (default ``^HLT_``) standard, all others none (with a warning).

Pure rates
----------
The pure rate of an item is the rate of events where it fired and no *other* item of its reference
group fired:
    standard and parking paths   w.r.t. all standard and parking paths
    scouting paths               w.r.t. all scouting paths
    datasets, streams            the same, with the combined datasets/streams instead of paths
    L1 seeds                     w.r.t. the other L1 seeds of the same flavour
    everything else              0 (not null: the CSV files are opened in Excel)

Excluded paths (``merge.exclude`` and the default list ``config.DEFAULT_EXCLUDE``: HLT_Physics,
HLT_EphemeralPhysics) are left out of the totals and of every pure-rate reference; their own pure
rate is 0. They keep their own rate and stay in the dataset and stream rates (what is written) and in
the overlaps. The pure rates of datasets and streams are computed without them.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field

import numpy as np
import yaml

from .config import exclude_patterns
from .menu import strip_version

CATEGORIES = ("standard", "parking", "scouting", "other", "none")     # also the priority order
PHYSICS_CATEGORIES = ("standard", "parking", "scouting")
# pure-rate reference groups of the HLT side: name -> categories whose paths/datasets/streams are in it
HLT_REFERENCES = {"standard_parking": ("standard", "parking"), "scouting": ("scouting",)}

L1_NON_SEED = re.compile(r"^L1_(FinalOR|UnprefireableEvent)")
L1_FLAVOUR = re.compile(r"_(final|beforePrescale|beforeBxMaskAndPrescale)$")
SPLIT_NAME = re.compile(r"^(.*\D)(\d+)$")          # base name + trailing number


class MapsError(ValueError):
    pass


def stream_category(name):
    if name.startswith("Physics"):
        return "standard"
    if name.startswith("Parking"):
        return "parking"
    if name.startswith("Scouting"):
        return "scouting"
    return "other"


def bit_kind(name):
    if name.startswith("L1"):
        return "l1"
    if name.startswith("Dataset_"):
        return "dataset_path"
    return "hlt"


def is_l1_seed(name):
    return name.startswith("L1_") and not L1_NON_SEED.search(name)


def l1_flavour(name):
    m = L1_FLAVOUR.search(name)
    return m.group(1) if m else ""


def l1_reference(flavour):
    """Name of the pure-rate reference group (and total) of the L1 seeds of one flavour."""
    return f"l1_{flavour}" if flavour else "l1"


def main_category(categories):
    return next(c for c in CATEGORIES if c in categories)


def combine_split(names, content):
    """Group split items: ``{combined name: [split names]}`` for all ``names`` (sorted by combined name).

    Items are split copies of one combined item if they have the same name apart from a trailing
    number, at least two of them exist, and ``content(name)`` (paths of a dataset, combined datasets
    of a stream) is the same for all. If the base name is itself an item, nothing is combined.
    Returns ``(groups, warnings)``.
    """
    by_base = {}
    for n in names:
        m = SPLIT_NAME.match(n)
        if m:
            by_base.setdefault(m.group(1), []).append(n)
    member_of, warnings = {}, []
    for base, members in by_base.items():
        if len(members) < 2 or len({content(n) for n in members}) != 1:
            continue
        if base in names:
            warnings.append(f"{members} look like split copies, but {base!r} exists too: not combined")
            continue
        for n in members:
            member_of[n] = base
    groups = {}
    for n in names:
        groups.setdefault(member_of.get(n, n), []).append(n)
    for members in groups.values():
        if len(members) > 1:                       # split copies: in the order of their numbers
            members.sort(key=lambda n: int(SPLIT_NAME.match(n).group(2)))
    return dict(sorted(groups.items())), warnings


def _load_map(path, what):
    with open(path) as f:
        doc = yaml.safe_load(f) or {}
    if not isinstance(doc, dict) or not all(isinstance(v, list) and all(isinstance(x, str) for x in v)
                                            for v in doc.values()):
        raise MapsError(f"{path}: expected a mapping {what} -> list of names")
    return {str(k): list(v) for k, v in doc.items()}


@dataclass
class Maps:
    bits: list                               # bit names (as in the merged table)
    kind: list                               # per bit: hlt, dataset_path, l1
    excluded: np.ndarray                     # per bit: matched by an exclude pattern
    excluded_by: dict                        # excluded bit -> the patterns that matched it
    exclude_patterns: list                   # the patterns in effect (default list + merge.exclude)
    datasets: dict                           # split dataset (as in the menu) -> member paths (also those without a bit)
    streams: dict                            # split stream -> split datasets
    stream_category: dict                    # split and combined streams
    dataset_category: dict                   # split and combined datasets
    dataset_streams: dict                    # split dataset -> split streams
    combined_datasets: dict                  # combined dataset -> split datasets
    combined_streams: dict                   # combined stream -> split streams
    dataset_combined: dict                   # split dataset -> combined dataset
    stream_combined: dict                    # split stream -> combined stream
    path_datasets: dict                      # bit name -> split datasets it is in
    path_categories: list                    # per bit: categories (sorted by priority); [] for non-hlt bits
    dataset_prescales: dict                  # split dataset -> prescale of its Dataset_ path in the menu
                                             # (default column; 1 if not in the table); {} without a table
    prescale_column: str | None              # the menu's default prescale column
    source: dict                             # where datasets/streams came from
    menu_paths: set | None                   # trigger paths of the menu (names as bits), None if no menu
    warnings: list = field(default_factory=list)

    # ---- per-bit properties ----------------------------------------------------------------------
    @property
    def n_bits(self):
        return len(self.bits)

    def mask(self, predicate):
        return np.array([bool(predicate(i)) for i in range(self.n_bits)], dtype=bool)

    def main_category(self, i):
        """standard/parking/scouting/other/none for paths; l1 or dataset_path for the other bits."""
        if self.kind[i] != "hlt":
            return self.kind[i]
        return main_category(self.path_categories[i])

    def l1_seed_mask(self, flavour=None):
        return self.mask(lambda i: self.kind[i] == "l1" and is_l1_seed(self.bits[i]) and not self.excluded[i]
                         and (flavour is None or l1_flavour(self.bits[i]) == flavour))

    def l1_flavours(self):
        return sorted({l1_flavour(b) for b, k in zip(self.bits, self.kind) if k == "l1" and is_l1_seed(b)})

    def overlap_l1_flavour(self):
        """The one L1 flavour in the L1 x L1 and HLT x L1 overlaps: "final" if the seeds have flavours
        (Phase-2 GT bits), else the seeds without suffix (Run-3 NanoAOD: final decisions); another
        flavour only if neither exists. None if there are no L1 seeds."""
        flavours = self.l1_flavours()
        if not flavours:
            return None
        for preferred in ("final", ""):
            if preferred in flavours:
                return preferred
        return flavours[0]

    def category_mask(self, categories):
        """Non-excluded paths with at least one of ``categories`` (used for totals)."""
        cats = set(categories)
        return self.mask(lambda i: self.kind[i] == "hlt" and not self.excluded[i]
                         and cats & set(self.path_categories[i]))

    # ---- combined datasets and streams -------------------------------------------------------------
    def combined_dataset_streams(self, c):
        """Combined streams of a combined dataset."""
        return sorted({self.stream_combined[s] for d in self.combined_datasets[c] for s in self.dataset_streams[d]})

    def combined_stream_datasets(self, c):
        """Combined datasets of a combined stream."""
        return sorted({self.dataset_combined[d] for s in self.combined_streams[c] for d in self.streams[s]
                       if d in self.dataset_combined})

    def path_combined_datasets(self, bit):
        return sorted({self.dataset_combined[d] for d in self.path_datasets.get(bit, [])})

    # ---- pure-rate reference groups --------------------------------------------------------------
    def references(self):
        """``{group: (bit mask, combined dataset mask, combined stream mask)}``: members of each reference."""
        ds, st = list(self.combined_datasets), list(self.combined_streams)
        out = {}
        for group, cats in HLT_REFERENCES.items():
            out[group] = (self.category_mask(cats),
                          np.array([self.dataset_category[d] in cats for d in ds], dtype=bool),
                          np.array([self.stream_category[s] in cats for s in st], dtype=bool))
        for flavour in self.l1_flavours():
            out[l1_reference(flavour)] = (self.l1_seed_mask(flavour), np.zeros(len(ds), bool), np.zeros(len(st), bool))
        return out

    def bit_reference(self, i):
        """The reference group of bit ``i`` for its pure rate, or None (pure rate 0)."""
        if self.excluded[i]:
            return None
        if self.kind[i] == "l1":
            return l1_reference(l1_flavour(self.bits[i])) if is_l1_seed(self.bits[i]) else None
        if self.kind[i] != "hlt":
            return None
        cat = self.main_category(i)
        return next((g for g, cats in HLT_REFERENCES.items() if cat in cats), None)

    def group_reference(self, category):
        """Reference group of a dataset or stream of the given category, or None."""
        return next((g for g, cats in HLT_REFERENCES.items() if category in cats), None)

    # ---- membership matrices -----------------------------------------------------------------------
    def dataset_members(self, with_excluded=True):
        """Bool matrix (bits x split datasets): bit is a path of the dataset (``with_excluded=False``:
        only the non-excluded paths, as used for the pure-rate references)."""
        index = {b: i for i, b in enumerate(self.bits)}
        m = np.zeros((self.n_bits, len(self.datasets)), dtype=bool)
        for j, paths in enumerate(self.datasets.values()):
            for p in paths:
                i = index.get(p)
                if i is not None and (with_excluded or not self.excluded[i]):
                    m[i, j] = True
        return m

    def dataset_path_bits(self):
        """Per split dataset: the bit index of its Dataset_ path, or -1 if it is not used.

        Not used: absent from the ntuples, or the dataset is split and the menu has no prescale table
        (MC menus): the round robin is done by the PrescaleService, without it the Dataset_ path of
        every split copy accepts all events.
        """
        index = {b: i for i, b in enumerate(self.bits)}
        return np.array([-1 if (not self.dataset_prescales and len(self.combined_datasets[self.dataset_combined[d]]) > 1)
                         else index.get(f"Dataset_{d}", -1) for d in self.datasets], dtype=np.int64)

    def stream_members(self):
        """Bool matrix (split datasets x split streams)."""
        ds_index = {d: j for j, d in enumerate(self.datasets)}
        m = np.zeros((len(self.datasets), len(self.streams)), dtype=bool)
        for k, dss in enumerate(self.streams.values()):
            for d in dss:
                if d in ds_index:
                    m[ds_index[d], k] = True
        return m

    def combination(self, level):
        """Bool matrix (split x combined) for ``datasets`` or ``streams``."""
        split, combined = ((self.datasets, self.combined_datasets) if level == "datasets"
                           else (self.streams, self.combined_streams))
        index = {n: j for j, n in enumerate(split)}
        m = np.zeros((len(split), len(combined)), dtype=bool)
        for k, members in enumerate(combined.values()):
            for n in members:
                m[index[n], k] = True
        return m

    def missing_paths(self, dataset):
        """Paths of a dataset that have no bit in the ntuples (e.g. AlCa_/MC_ paths not stored in NanoAOD)."""
        present = set(self.bits)
        return [p for p in self.datasets[dataset] if p not in present]

    def paths_without_dataset(self):
        """Indices of HLT paths in no dataset (Dataset_ paths and L1 bits are not paths in this sense)."""
        return [i for i in range(self.n_bits) if self.kind[i] == "hlt" and not self.path_datasets.get(self.bits[i])]

    def bits_not_in_menu(self):
        if self.menu_paths is None:
            return []
        return [i for i in range(self.n_bits) if self.kind[i] != "l1" and self.bits[i] not in self.menu_paths]


def menu_prescales(menu, rename):
    """``({path: prescale in the default column}, column)`` or ``({}, None)`` without a prescale table."""
    if not menu or not menu.get("prescales"):
        return {}, None
    cols = menu.get("prescale_columns") or []
    col = menu.get("prescale_default_column")
    if col not in cols:
        return {}, None
    k = cols.index(col)
    return {rename(p): ps[k] for p, ps in menu["prescales"].items() if k < len(ps)}, col


def build_maps(bits, menu, settings):
    """Build :class:`Maps` for the merged bit list.

    ``menu``: the menu dict of the partials (``MenuInfo.to_dict()``) or None; ``settings``: the
    ``merge`` section of the measurement config.
    """
    rename = strip_version if settings["strip_versions"] else (lambda n: n)
    warnings = []
    patterns = exclude_patterns(settings)
    excl = [(x, re.compile(x)) for x in patterns]
    excluded_by = {}
    for b in bits:
        hit = [x for x, rx in excl if rx.search(b)]
        if hit:
            excluded_by[b] = hit
    excluded = np.array([b in excluded_by for b in bits], dtype=bool)
    kind = [bit_kind(b) for b in bits]

    def source_map(key, what):
        spec = settings[key]
        if spec is None:
            return {}, None
        if spec == "menu":
            if menu is None:
                return {}, "menu (no menu in the ntuples)"
            return {k: list(v) for k, v in (menu.get(key) or {}).items()}, "menu"
        if not os.path.exists(spec):
            raise MapsError(f"merge.{key}: {spec} does not exist")
        return _load_map(spec, what), spec

    datasets, ds_source = source_map("datasets", "dataset")
    streams, st_source = source_map("streams", "stream")
    datasets = {d: list(dict.fromkeys(rename(p) for p in paths)) for d, paths in sorted(datasets.items())}
    streams = {s: [d for d in dss] for s, dss in sorted(streams.items())}

    unknown = sorted({d for dss in streams.values() for d in dss} - set(datasets))
    if unknown:
        warnings.append(f"streams refer to {len(unknown)} unknown dataset(s): {unknown[:5]}")
    stream_cat = {s: stream_category(s) for s in streams}
    dataset_streams = {d: [s for s, dss in streams.items() if d in dss] for d in datasets}
    if datasets and not streams:
        warnings.append("datasets are defined but no streams: all datasets are treated as standard physics")
        dataset_cat = {d: "standard" for d in datasets}
    else:
        dataset_cat = {d: (main_category([stream_cat[s] for s in ss]) if ss else "none")
                       for d, ss in dataset_streams.items()}
        no_stream = [d for d, ss in dataset_streams.items() if not ss]
        if no_stream:
            warnings.append(f"{len(no_stream)} dataset(s) are in no stream (category none): {no_stream[:5]}")

    # split -> combined datasets and streams
    combined_ds, w = combine_split(list(datasets), lambda d: frozenset(datasets[d]))
    warnings.extend(w)
    dataset_combined = {d: c for c, members in combined_ds.items() for d in members}
    combined_st, w = combine_split(list(streams), lambda s: frozenset(dataset_combined.get(d, d) for d in streams[s]))
    warnings.extend(w)
    stream_combined = {s: c for c, members in combined_st.items() for s in members}
    for c, members in combined_ds.items():
        if c not in dataset_cat:
            dataset_cat[c] = main_category([dataset_cat[d] for d in members])
    for c, members in combined_st.items():
        if c not in stream_cat:
            stream_cat[c] = main_category([stream_cat[s] for s in members])

    menu_ps, ps_column = menu_prescales(menu, rename)
    dataset_ps = {d: menu_ps.get(f"Dataset_{d}", 1) for d in datasets} if ps_column else {}

    path_datasets = {}
    for d, paths in datasets.items():
        for p in paths:
            path_datasets.setdefault(p, []).append(d)
    if datasets:
        path_categories = [sorted({dataset_cat[d] for d in path_datasets.get(b, [])} or {"none"},
                                  key=CATEGORIES.index) if k == "hlt" else []
                           for b, k in zip(bits, kind)]
    else:
        rx_sc, rx_std = re.compile(settings.get("scouting_paths") or "(?!)"), re.compile(settings["pure_reference"])
        path_categories = [(["scouting"] if rx_sc.search(b) else ["standard"] if rx_std.search(b) else ["none"])
                           if k == "hlt" else [] for b, k in zip(bits, kind)]
        if any(k == "hlt" for k in kind):
            warnings.append(f"no datasets/streams ({ds_source or 'none configured'}): paths matching "
                            f"{settings.get('scouting_paths')!r} are treated as scouting, those matching "
                            f"{settings['pure_reference']!r} as standard physics, all others as paths without dataset")

    menu_paths = {rename(p) for p in menu["trigger_paths"]} if menu and menu.get("trigger_paths") else None
    return Maps(bits=list(bits), kind=kind, excluded=excluded, excluded_by=excluded_by, exclude_patterns=patterns,
                datasets=datasets, streams=streams, stream_category=stream_cat, dataset_category=dataset_cat,
                dataset_streams=dataset_streams, combined_datasets=combined_ds, combined_streams=combined_st,
                dataset_combined=dataset_combined, stream_combined=stream_combined,
                path_datasets=path_datasets, path_categories=path_categories, dataset_prescales=dataset_ps,
                prescale_column=ps_column, source={"datasets": ds_source, "streams": st_source},
                menu_paths=menu_paths, warnings=warnings)
