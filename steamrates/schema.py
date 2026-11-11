"""Small helpers to validate YAML documents (sample catalog, measurement configs).

Every mapping is checked against the keys it may contain, so a typo is an error instead of a
silently ignored setting. Error messages name the location, e.g.
``meas.yaml: counting.chunk_sise: unknown key (did you mean 'chunk_size'?)``.

The catalog may contain the placeholder string ``TODO`` for values nobody has filled in yet. It is
accepted while loading, and :func:`find_todos` reports it when an entry is actually used.
"""

from __future__ import annotations

import datetime
import difflib
import re

TODO = "TODO"
REQUIRED = object()          # marker: the key has no default
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.+-]*$")


class ConfigError(Exception):
    """An invalid catalog or measurement config."""


def check_keys(d, allowed, where, required=()):
    """``d`` must be a mapping with keys from ``allowed`` and all ``required`` keys."""
    if not isinstance(d, dict):
        raise ConfigError(f"{where}: expected a mapping, got {type(d).__name__}")
    for key in d:
        if key not in allowed:
            hint = difflib.get_close_matches(str(key), list(allowed), n=1)
            raise ConfigError(f"{where}.{key}: unknown key" + (f" (did you mean {hint[0]!r}?)" if hint else "")
                              + f"; allowed: {', '.join(sorted(allowed))}")
    for key in required:
        if key not in d:
            raise ConfigError(f"{where}: missing key {key!r}")


def _type_name(types):
    names = {bool: "true/false", int: "an integer", float: "a number", str: "a string", list: "a list",
             dict: "a mapping", type(None): "null"}
    return " or ".join(names.get(t, t.__name__) for t in types)


def value(d, key, types, where, default=REQUIRED, allow_todo=False, check=None, check_msg=""):
    """Get ``d[key]`` (or ``default``) and check its type (and optionally ``check(value)``).

    ``types`` is a tuple of Python types; ``float`` also accepts integers, and ``str`` also accepts
    dates (YAML turns ``2026-11-10`` into a date). With ``allow_todo``, the string ``TODO`` is
    accepted as a placeholder.
    """
    where = f"{where}.{key}"
    if key not in d:
        if default is REQUIRED:
            raise ConfigError(f"{where}: missing")
        return default
    v = d[key]
    if allow_todo and v == TODO:
        return v
    if isinstance(v, (datetime.date, datetime.datetime)) and str in types:
        v = v.isoformat()
    ok = isinstance(v, types) and not (isinstance(v, bool) and bool not in types)
    if not ok and float in types and isinstance(v, int) and not isinstance(v, bool):
        v, ok = float(v), True
    if not ok:
        raise ConfigError(f"{where}: expected {_type_name(types)}, got {v!r}")
    if check is not None and v is not None and not check(v):
        raise ConfigError(f"{where}: {check_msg or 'invalid value'} (got {v!r})")
    return v


def str_list(d, key, where, default=REQUIRED):
    """A list of strings; a single string is accepted as a list of one."""
    v = value(d, key, (list, str, type(None)), where, default)
    if v is None:
        return []
    if isinstance(v, str):
        return [v]
    for i, item in enumerate(v):
        if not isinstance(item, str):
            raise ConfigError(f"{where}.{key}[{i}]: expected a string, got {item!r}")
    return list(v)


def check_id(name, where):
    if not isinstance(name, str) or not _ID.match(name):
        raise ConfigError(f"{where}: {name!r} is not a valid name (letters, digits and _ . + -)")
    return name


def check_regex(pattern, where):
    try:
        re.compile(pattern)
    except re.error as e:
        raise ConfigError(f"{where}: invalid regular expression {pattern!r} ({e})") from None
    return pattern


def find_todos(obj, where):
    """Locations of ``TODO`` placeholders in a nested structure."""
    if obj == TODO:
        return [where]
    if isinstance(obj, dict):
        return [t for k, v in obj.items() for t in find_todos(v, f"{where}.{k}")]
    if isinstance(obj, list):
        return [t for i, v in enumerate(obj) for t in find_todos(v, f"{where}[{i}]")]
    return []
