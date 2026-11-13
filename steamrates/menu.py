"""Read the HLT menu that CMSSW embeds in every EDM and NanoAOD file.

Every file written by CMSSW contains a ``ParameterSets`` tree with the complete configuration of all
processes that produced it (including the HLT menu that was run). Each entry is a pair
``(hash, text)``: a 16-byte hash (the "ParameterSetID") and the parameter set in CMSSW's text
encoding. From it we extract what the rate measurement needs:

  * the ordered list of trigger paths (needed to name the bits of ``edm::TriggerResults``),
  * the menu name (``HLTConfigVersion.tableName``),
  * dataset -> paths and stream -> datasets,
  * the HLT prescale table (``PrescaleService``), if present (data menus only; MC menus have none).

The text encoding (CMSSW ``ParameterSet::toString``) looks like this::

    <@module_label=+Z(hltPreIsoMu24\x00);offset=+U(0);datasets=+Q(1a2b...);paths=+z({A\x00,B\x00})>

Each entry is ``name=`` + ``+`` (tracked) or ``-`` (untracked) + a one-letter type + the value.
Types used here: ``Z`` string, ``z`` vector of strings, ``Q`` nested parameter set referenced by its
hash, ``q`` vector of nested parameter sets (hashes), ``U``/``I``/``u``/``i`` (vectors of) integers.
Strings end with a NUL byte, which we strip.

Only uproot is needed (no PyROOT): see :mod:`steamrates.rootbytes` for the byte layout.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import numpy as np

from .rootbytes import LayoutError, check_header, read_raw_entries, read_string


# --------------------------------------------------------------------------------------------------
# Low level: ParameterSets tree -> {hash: text}
# --------------------------------------------------------------------------------------------------

def read_parameter_sets(file):
    """Return ``{hex hash: parameter-set text}`` for all entries of the file's ``ParameterSets`` tree.

    Layout of one entry (``std::pair<edm::Hash<1>, edm::ParameterSetBlob>``):
        [0:6]   edm::Hash<1> header (class version 11)
        [6:12]  header of its std::string member (version 10)
        [12]    string length (always 16)
        [13:29] the 16 hash bytes
        [29:35] edm::ParameterSetBlob header (class version 10)
        [35:41] header of its std::string member (version 10)
        [41:]   the parameter-set text as a ROOT std::string (length byte(s) + characters)
    """
    branch = file["ParameterSets"]["IdToParameterSetsBlobs"]
    data, starts, _ = read_raw_entries(branch)
    check_header(data, starts, 11, "ParameterSets: edm::Hash<1>")
    check_header(data, starts + 29, 10, "ParameterSets: edm::ParameterSetBlob")
    if not np.all(data[starts + 12] == 16):
        raise LayoutError("ParameterSets: unexpected hash length")
    blobs = {}
    for s in starts.tolist():
        key = bytes(data[s + 13:s + 29]).hex()
        text, _ = read_string(data, s + 41)
        blobs[key] = text
    return blobs


# --------------------------------------------------------------------------------------------------
# Parsing the text encoding
# --------------------------------------------------------------------------------------------------

_OPEN = {"(": ")", "{": "}", "<": ">"}
_CLOSE = set(_OPEN.values())
_ENTRY_END = re.compile(r"[;>]")
_PROCESS_NAME = re.compile(r"(?:^<|;)@process_name=\+Z\(([^)]*)\)")


def parse_pset(text):
    """Split one encoded parameter set into ``{name: (type letter, raw value)}``.

    Only the top level is split; nested values stay raw text and are decoded on demand with the
    ``as_*`` helpers below. Splitting respects brackets, so ';' inside a nested value is harmless.
    """
    if not (text.startswith("<") and text.endswith(">")):
        raise LayoutError("parameter set text does not start with '<' and end with '>'")
    entries = {}
    body = text[1:-1]
    depth, start = 0, 0
    for i, ch in enumerate(body + ";"):
        if ch in _OPEN:
            depth += 1
        elif ch in (")", "}", ">"):
            depth -= 1
        elif ch == ";" and depth == 0:
            item = body[start:i]
            start = i + 1
            if not item:
                continue
            name, _, rest = item.partition("=")
            # rest = '+' or '-' (tracked/untracked), then the type letter, then the value
            entries[name] = (rest[1:2], rest[2:])
    return entries


def _strip_brackets(raw):
    raw = raw.strip()
    while raw[:1] in ("(", "{") and raw[-1:] in (")", "}"):
        raw = raw[1:-1]
    return raw


def as_string(raw):
    """Decode a string value ``(text\\x00)``."""
    return _strip_brackets(raw).replace("\x00", "")


def as_strings(raw):
    """Decode a vector-of-strings value ``({a\\x00,b\\x00})``."""
    inner = _strip_brackets(raw)
    return [s.replace("\x00", "") for s in inner.split(",")] if inner else []


def as_ints(raw):
    """Decode a (vector of) integer value(s)."""
    inner = _strip_brackets(raw)
    return [int(x) for x in inner.split(",")] if inner else []


def as_ref(raw):
    """Decode a nested-parameter-set reference ``(hash)`` -> hex hash."""
    return _strip_brackets(raw)


def as_refs(raw):
    """Decode a vector of nested-parameter-set references ``{h1,h2}`` -> list of hex hashes."""
    inner = _strip_brackets(raw)
    return inner.split(",") if inner else []


# --------------------------------------------------------------------------------------------------
# High level: the menu
# --------------------------------------------------------------------------------------------------

@dataclass
class MenuInfo:
    """What we know about the HLT menu of one process."""

    process: str
    name: str | None = None                       # e.g. /cdaq/physics/Run2026/2e34/v1.3.0/HLT/V3
    trigger_paths: list = field(default_factory=list)
    datasets: dict = field(default_factory=dict)  # dataset -> [paths]
    streams: dict = field(default_factory=dict)   # stream -> [datasets]
    prescale_columns: list = field(default_factory=list)
    prescale_default_column: str | None = None
    prescales: dict = field(default_factory=dict)  # path -> [prescale per column]
    trigger_paths_id: str | None = None           # hash of the PSet holding @trigger_paths

    def to_dict(self):
        return {"process": self.process, "name": self.name, "trigger_paths": self.trigger_paths,
                "datasets": self.datasets, "streams": self.streams,
                "prescale_columns": self.prescale_columns,
                "prescale_default_column": self.prescale_default_column, "prescales": self.prescales}


class ParameterSetRegistry:
    """All parameter sets of one file, with helpers to find processes and resolve references."""

    def __init__(self, blobs):
        self.blobs = blobs
        self._parsed = {}

    @classmethod
    def from_file(cls, file):
        return cls(read_parameter_sets(file))

    def get(self, key):
        """Parsed parameter set for a hex hash (cached). Use for small parameter sets only."""
        if key not in self._parsed:
            self._parsed[key] = parse_pset(self.blobs[key])
        return self._parsed[key]

    def entry(self, key, name):
        """``(type letter, raw value)`` of one top-level entry of a parameter set, or ``None``.

        Unlike :meth:`get` this does not parse the whole parameter set. That matters for the
        top-level process parameter sets, which hold a complete HLT menu (several MB of text): the
        entry is found with a regex anchored at an entry boundary (start of the set or ';'), and
        only its own value is scanned for the matching closing bracket.
        """
        text = self.blobs[key]
        m = re.search(r"(?:^<|;)" + re.escape(name) + r"=[+-](\w)", text)
        if not m:
            return None
        start = m.end()
        if start < len(text) and text[start] in _OPEN:
            depth = 0
            for pos in range(start, len(text)):
                ch = text[pos]
                if ch in _OPEN:
                    depth += 1
                elif ch in _CLOSE:
                    depth -= 1
                    if depth == 0:
                        return m.group(1), text[start:pos + 1]
            raise LayoutError(f"unbalanced brackets in entry {name!r}")
        end = _ENTRY_END.search(text, start)
        return m.group(1), text[start:end.start() if end else len(text)]

    def processes(self):
        """``{process name: hash}`` for all top-level process parameter sets in the file."""
        out = {}
        for key, text in self.blobs.items():
            if "@trigger_paths=" in text:          # the process PSet and the @trigger_paths PSet
                m = _PROCESS_NAME.search(text)     # only the process PSet has @process_name
                if m:
                    out[m.group(1).replace("\x00", "")] = key
        return out

    def trigger_names(self, trigger_paths_id):
        """Ordered path names of the ``@trigger_paths`` PSet with the given hash.

        ``edm::TriggerResults`` stores this hash instead of the names; bit ``i`` belongs to name ``i``.
        """
        if trigger_paths_id not in self.blobs:
            raise LayoutError(f"trigger-paths parameter set {trigger_paths_id} not found in file")
        return as_strings(self.get(trigger_paths_id)["@trigger_paths"][1])

    def menu(self, process=None, trigger_paths_id=None):
        """Extract :class:`MenuInfo` for one process.

        The process is chosen, in this order, by the ``@trigger_paths`` hash stored in
        ``edm::TriggerResults`` (most reliable), by name, or as the only process that has an
        ``HLTConfigVersion`` (i.e. that ran an HLT menu).
        """
        procs = self.processes()
        key = None
        if trigger_paths_id is not None:
            for name, k in procs.items():
                if as_ref(self.entry(k, "@trigger_paths")[1]) == trigger_paths_id:
                    process, key = name, k
        if key is None and process is not None:
            key = procs.get(process)
        if key is None:
            hlt = [n for n, k in procs.items() if self.entry(k, "HLTConfigVersion") is not None]
            if len(hlt) != 1:
                raise LayoutError(f"cannot choose the HLT process among {sorted(procs)}; specify it")
            process, key = hlt[0], procs[hlt[0]]
        top = {name: self.entry(key, name) for name in
               ("@trigger_paths", "HLTConfigVersion", "datasets", "streams", "PrescaleService")}
        info = MenuInfo(process=process)
        info.trigger_paths_id = as_ref(top["@trigger_paths"][1])
        info.trigger_paths = self.trigger_names(info.trigger_paths_id)
        if top["HLTConfigVersion"]:
            hcv = self.get(as_ref(top["HLTConfigVersion"][1]))
            info.name = as_string(hcv["tableName"][1]) if "tableName" in hcv else None
        for attr in ("datasets", "streams"):
            if top[attr] and top[attr][0] == "Q":
                sub = self.get(as_ref(top[attr][1]))
                setattr(info, attr, {k: as_strings(v[1]) for k, v in sub.items() if v[0] == "z"})
        if top["PrescaleService"]:
            ps = self.get(as_ref(top["PrescaleService"][1]))
            info.prescale_columns = as_strings(ps["lvl1Labels"][1]) if "lvl1Labels" in ps else []
            if "lvl1DefaultLabel" in ps:
                info.prescale_default_column = as_string(ps["lvl1DefaultLabel"][1])
            for row_key in as_refs(ps.get("prescaleTable", ("q", ""))[1]):
                row = self.get(row_key)
                info.prescales[as_string(row["pathName"][1])] = as_ints(row["prescales"][1])
        return info


_VERSION_RE = re.compile(r"_v\d+$")


def strip_version(name):
    """``HLT_IsoMu24_v33`` -> ``HLT_IsoMu24``. Only a trailing ``_v<digits>`` is removed.

    (The legacy code used ``rstrip("0123456789")``, which also mangled names such as
    ``Dataset_AlCaP0``.)
    """
    return _VERSION_RE.sub("", name)
