"""Input files of a measurement: expand globs and list files, drop duplicates, check the files.

An input specification (in the sample catalog, a measurement config or on the command line) is a
string, one of:

  * a ROOT file:      ``/eos/cms/store/.../f.root`` or ``root://eoscms.cern.ch//eos/cms/store/.../f.root``
  * a glob pattern:   ``/eos/cms/store/.../00000/*.root`` (local and EOS-fuse paths only; xrootd
                      has no globbing)
  * a list file:      any path that does not end in ``.root``; one input per line, ``#`` starts a
                      comment. Its entries are again inputs (but not list files).
  * an LFN:           ``/store/...``: becomes ``/eos/cms/store/...`` if that exists on this machine,
                      else ``<redirector>/store/...`` (default: the CMS global redirector).

Relative paths are relative to the file they appear in (YAML file or list file).

The same file can be reachable under different names (``/eos/cms/store/x.root`` and
``root://eoscms.cern.ch//eos/cms/store/x.root``); :func:`canonical_name` maps all of them to one key,
so duplicates are found reliably.
"""

from __future__ import annotations

import glob
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import uproot
import uproot.source.xrootd

log = logging.getLogger(__name__)

GLOBAL_REDIRECTOR = "root://cms-xrd-global.cern.ch/"
_LFN = re.compile(r"(/store/.*)$")
_GLOB_CHARS = re.compile(r"[*?\[]")


class InputError(Exception):
    """An input specification that cannot be resolved (no match, missing file, ...)."""


# --------------------------------------------------------------------------------------------------
# Names
# --------------------------------------------------------------------------------------------------

def canonical_name(path):
    """Key used to detect the same file given twice (e.g. once via xrootd, once via /eos)."""
    m = _LFN.search(path)
    return m.group(1) if m else os.path.realpath(path)


def is_remote(path):
    return path.startswith("root://")


def resolve_lfn(path, redirector=GLOBAL_REDIRECTOR):
    """``/store/...`` -> a path that can be opened here (EOS fuse if present, else xrootd)."""
    if not path.startswith("/store/"):
        return path
    local = "/eos/cms" + path
    if os.path.exists(local):
        return local
    return redirector.rstrip("/") + "/" + path


def open_root(path, **options):
    """``uproot.open`` that also works for ``root://`` URLs in CMSSW's Python.

    uproot opens xrootd URLs through fsspec-xrootd by default, which is not installed in CMSSW;
    its own pyxrootd-based source (``XRootDSource``) works and is used instead.
    """
    if is_remote(path):
        options.setdefault("handler", uproot.source.xrootd.XRootDSource)
    return uproot.open(path, **options)


# --------------------------------------------------------------------------------------------------
# Expanding specifications
# --------------------------------------------------------------------------------------------------

def _read_list_file(path):
    with open(path) as f:
        lines = [line.split("#", 1)[0].strip() for line in f]
    return [line for line in lines if line]


def _expand_one(spec, base_dir, redirector, allow_list=True):
    spec = spec.strip()
    if not spec:
        raise InputError("empty input specification")
    if spec.startswith("/store/"):
        spec = resolve_lfn(spec, redirector)
    if is_remote(spec):
        if _GLOB_CHARS.search(spec):
            raise InputError(f"{spec}: wildcards are not supported for root:// URLs; use a list file")
        return [spec]
    path = spec if os.path.isabs(spec) else os.path.normpath(os.path.join(base_dir, spec))
    if _GLOB_CHARS.search(path):
        matches = sorted(glob.glob(path))
        if not matches:
            raise InputError(f"{spec}: the pattern matches no file")
        return matches
    if not os.path.exists(path):
        raise InputError(f"{spec}: no such file ({path})")
    if path.endswith(".root"):
        return [path]
    if not allow_list:
        raise InputError(f"{spec}: list files cannot contain list files (expected .root files)")
    out = []
    list_dir = os.path.dirname(path)
    for entry in _read_list_file(path):
        out.extend(_expand_one(entry, list_dir, redirector, allow_list=False))
    if not out:
        raise InputError(f"{spec}: the list file is empty")
    return out


@dataclass
class InputList:
    """Expanded inputs, in the given order, without duplicates."""

    files: list
    duplicates: list = field(default_factory=list)   # [(kept, dropped), ...]


def expand_inputs(specs, base_dir=".", redirector=GLOBAL_REDIRECTOR):
    """Expand input specifications into a list of files; drop duplicates (kept: first occurrence).

    Duplicates are dropped with a warning (the caller reports how many). Use
    :func:`collect_inputs` where duplicates must be an error.
    """
    if isinstance(specs, str):
        specs = [specs]
    files, dups, seen = [], [], {}
    for spec in specs:
        for path in _expand_one(spec, base_dir, redirector):
            key = canonical_name(path)
            if key in seen:
                dups.append((seen[key], path))
                continue
            seen[key] = path
            files.append(path)
    if not files:
        raise InputError("no input files")
    return InputList(files, dups)


def collect_inputs(paths, list_files):
    """Inputs of a counting job (command line + list files); duplicates are an error here.

    A counting job gets its files from ``prepare``, which has already removed duplicates, so a
    duplicate at this point means something went wrong and must not be counted twice.
    """
    inputs = list(paths)
    for lf in list_files:
        inputs.extend(_read_list_file(lf))
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
# Checking files
# --------------------------------------------------------------------------------------------------

@dataclass
class FileCheck:
    """What ``prepare`` learns about one input file by opening it."""

    path: str
    n_events: int | None = None
    format: str | None = None
    runs: list | None = None          # run numbers listed in the LuminosityBlocks tree
    n_lumis: int | None = None        # number of lumi sections listed there
    error: str | None = None

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


def check_file(path):
    """Open one file and read its event count, format and lumi sections (cheap: no event data)."""
    from .readers import detect_format_of, file_lumi_blocks
    try:
        with open_root(path) as f:
            fmt = detect_format_of(f, path)
            lumis = file_lumi_blocks(f, fmt)
            return FileCheck(path, n_events=int(f["Events"].num_entries), format=fmt,
                             runs=sorted({int(r) for r in lumis[:, 0]}), n_lumis=len(lumis))
    except Exception as e:  # noqa: BLE001  (any failure to read the file is reported, not raised)
        return FileCheck(path, error=f"{type(e).__name__}: {e}")


def check_files(paths, threads=8, progress_every=200):
    """:func:`check_file` for many files in a thread pool (the work is mostly waiting for I/O)."""
    t0 = time.time()
    results = []
    with ThreadPoolExecutor(max(1, threads)) as pool:
        for i, res in enumerate(pool.map(check_file, paths), 1):
            results.append(res)
            if progress_every and i % progress_every == 0:
                log.info("checked %d/%d input files (%.0f s)", i, len(paths), time.time() - t0)
    log.info("checked %d input files in %.1f s", len(paths), time.time() - t0)
    return results
