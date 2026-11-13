"""plots: tables and figures of a merged measurement (PNG + PDF), for talks and quick looks.

    python3 -m steamrates plots WORKDIR [-o DIR] [--top 10] [--path HLT_IsoMu24 ...] [--format png pdf]

Reads only ``WORKDIR/results/`` (the JSON files written by ``merge``) and writes into
``WORKDIR/plots/`` (or ``-o``):

    rate_budget              output totals (standard, parking, scouting, ...) and the HLT input rate
    top_hlt_paths            table: the HLT paths with the highest rates
    top_l1_seeds             table: the L1 seeds with the highest rates
    top_datasets             table: datasets (split copies combined), with pure rates
    top_streams              table: streams (split copies combined)
    datasets_pure_shared     bars: pure and shared part of the standard + parking datasets
    scouting_shared          bars: scouting paths, part also stored by standard + parking paths
    overlap_streams          matrix: stream x stream overlap (fraction of the row's events)
    overlap_datasets         matrix: dataset x dataset overlap
    partners_<path>          bars: the paths with the largest overlap with <path>
    matrix_<path>            matrix: <path> and those paths
    index.html               all figures on one page with short captions

Items that fire in (nearly) every event of the sample are left out of tables and matrices and named
in a footnote: in HLTPhysics data the sample's selection path (HLT_Physics) fires in every event,
so every dataset that contains it shows the full input rate, which says nothing about its rate.

The functions are meant to be reused (notebooks, a future web page): ``Results`` loads a results
directory, ``Overlap`` one overlap matrix, and each ``fig_*`` function returns a matplotlib figure.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap

import numpy as np

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt                       # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from matplotlib.patches import Rectangle               # noqa: E402

# ---- style ------------------------------------------------------------------------------------------
# Colours: a validated categorical palette (blue/orange/aqua pass all pairwise colour-vision checks);
# the low-contrast aqua always comes with a text label. Sequential matrices: one blue ramp.
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, ZEBRA, SURFACE = "#e1e0d9", "#c3c2b7", "#f4f4f1", "#ffffff"
CATEGORY_COLORS = {"standard": "#2a78d6", "parking": "#eb6834", "scouting": "#1baf7a",
                   "other": "#898781", "none": "#c3c2b7", "dataset_path": "#c3c2b7"}
CATEGORY_ORDER = ("standard", "parking", "scouting", "other", "none")
BLUE_DARK, BLUE_LIGHT = "#256abf", "#9ec5f4"
SEQ_RAMP = ["#ffffff", "#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
SEQ_CMAP = LinearSegmentedColormap.from_list("steamrates_blue", SEQ_RAMP)
SATURATED = 0.99            # fraction of the input above which an item is left out (see module doc)


def _style():
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 9, "text.color": INK,
        "axes.edgecolor": AXIS, "axes.labelcolor": INK2, "axes.titlesize": 11, "axes.titleweight": "bold",
        "axes.titlelocation": "left", "axes.titlecolor": INK, "axes.spines.top": False,
        "axes.spines.right": False, "axes.facecolor": SURFACE, "figure.facecolor": SURFACE,
        "xtick.color": INK2, "ytick.color": INK2, "xtick.labelsize": 8, "ytick.labelsize": 8,
        "grid.color": GRID, "grid.linewidth": 0.6, "legend.frameon": False, "legend.fontsize": 8,
        "savefig.facecolor": SURFACE, "pdf.fonttype": 42,
    })


def fmt_rate(x):
    """Rate in Hz with a sensible number of digits: 9,312 / 377.2 / 6.13 / 0.53."""
    if x is None:
        return ""
    ax = abs(x)
    if ax >= 1000:
        return f"{x:,.0f}"
    if ax >= 100:
        return f"{x:.1f}"
    if ax >= 1:
        return f"{x:.2f}"
    return f"{x:.3f}" if ax > 0 else "0"


def fmt_rate_err(x, err):
    """Value ± error, the error with two significant digits and the value with the same decimals."""
    if not err or err <= 0:
        return fmt_rate(x)
    decimals = max(0, 1 - int(np.floor(np.log10(err))))
    return f"{x:,.{decimals}f} ± {err:,.{decimals}f}"


def fmt_pct(f):
    return "" if f is None else (f"{100 * f:.0f}%" if f >= 0.1 else f"{100 * f:.1f}%")


# ---- data ---------------------------------------------------------------------------------------------
class Overlap:
    """One overlap matrix of the results (``overlaps_<name>.json``), dense (the matrices are small
    enough: ~1000 x 1000 paths at most)."""

    def __init__(self, doc):
        self.name = doc["name"]
        self.rows, self.cols = doc["rows"], doc["cols"]
        self.symmetric = doc["symmetric"]
        self.row_index = {n: i for i, n in enumerate(self.rows)}
        self.col_index = {n: i for i, n in enumerate(self.cols)}
        cols = doc["entry_columns"]
        e = np.asarray(doc["entries"], dtype=float).reshape(-1, len(cols))
        r, c = e[:, cols.index("row")].astype(int), e[:, cols.index("col")].astype(int)
        self.count = np.zeros((len(self.rows), len(self.cols)))
        self.rate = np.zeros_like(self.count)
        self.count[r, c] = e[:, cols.index("count")]
        self.rate[r, c] = e[:, cols.index("rate_hz")]
        if self.symmetric:                    # only row <= col is stored
            self.count[c, r] = self.count[r, c]
            self.rate[c, r] = self.rate[r, c]

    def submatrix(self, names, what="count"):
        idx = [self.row_index[n] for n in names]
        m = getattr(self, what)
        return m[np.ix_(idx, idx)]

    def fraction_of_row(self, names):
        """M[i, j] = fraction of the events of names[i] in which names[j] also fired."""
        m = self.submatrix(names)
        d = np.diag(m).copy()
        return np.divide(m, d[:, None], out=np.zeros_like(m), where=d[:, None] > 0)


class Results:
    """The JSON outputs of ``merge`` in one results directory (loaded on first use)."""

    def __init__(self, path):
        self.path = path
        self._docs = {}
        if not os.path.exists(os.path.join(path, "summary.json")):
            raise FileNotFoundError(f"{path}: no summary.json (run 'merge' first)")

    def doc(self, name):
        if name not in self._docs:
            with open(os.path.join(self.path, name + ".json")) as f:
                self._docs[name] = json.load(f)
        return self._docs[name]

    def has(self, name):
        return os.path.exists(os.path.join(self.path, name + ".json"))

    @property
    def summary(self):
        return self.doc("summary")

    def records(self, name):
        return self.doc(name)["records"]

    def overlap(self, name):
        return Overlap(self.doc("overlaps_" + name))

    @property
    def n_input(self):
        return self.summary["totals"]["input"]["count"]

    def saturated(self, rec):
        return rec.get("count", 0) >= SATURATED * self.n_input

    def context(self):
        """One line describing the measurement (figure subtitles)."""
        s = self.summary
        nt = s.get("input", {}).get("ntuples", {})
        norm = s.get("normalization", {})
        parts = [s.get("measurement", "")]
        if nt.get("hlt_menu"):
            parts.append(nt["hlt_menu"])
        if norm.get("mode") == "data":
            parts.append(f"L = {norm.get('target_lumi')}e34")
            parts.append(f"{norm.get('n_ls')} LS")
        elif norm.get("target_lumi"):
            parts.append(f"L = {norm.get('target_lumi')}e34")
        parts.append(f"{s.get('events', {}).get('selected', 0):,} events")
        return "  ·  ".join(str(p) for p in parts if p)


# ---- generic pieces ---------------------------------------------------------------------------------
def _titles(fig, title, subtitle, top_in, height_in):
    fig.text(0.25 / fig.get_figwidth(), 1 - 0.32 / height_in, title, fontsize=12, fontweight="bold", va="baseline")
    if subtitle:
        fig.text(0.25 / fig.get_figwidth(), 1 - 0.56 / height_in, subtitle, fontsize=8, color=INK2, va="baseline")


def fig_table(columns, rows, title, subtitle=None, footnote=None):
    """A typeset table as a figure.

    ``columns``: list of (header, width in inches, "l" or "r"). ``rows``: list of lists of cells; a
    cell is a string, or ``(text, colour)`` to draw a colour marker before the text (categories).
    """
    row_h, head_h, top, left = 0.27, 0.34, 0.80, 0.25
    width = left * 2 + sum(w for _, w, _ in columns)
    foot_lines = textwrap.wrap(footnote, int(width * 16)) if footnote else []
    height = top + head_h + row_h * len(rows) + 0.15 + 0.17 * len(foot_lines) + 0.1
    fig = plt.figure(figsize=(width, height))
    X = lambda x: x / width                      # noqa: E731  (inches -> figure fraction)
    Y = lambda y: 1 - y / height                 # noqa: E731  (inches from the top)
    _titles(fig, title, subtitle, top, height)
    # header
    x = left
    y_head = top + head_h * 0.62
    for header, w, align in columns:
        fig.text(X(x + (w - 0.08 if align == "r" else 0.04)), Y(y_head), header, ha="right" if align == "r" else "left",
                 fontsize=8, fontweight="bold", color=INK2, va="baseline")
        x += w
    fig.add_artist(plt.Line2D([X(left), X(width - left)], [Y(top + head_h)] * 2, color=AXIS, lw=0.8))
    # rows
    for i, row in enumerate(rows):
        y0 = top + head_h + i * row_h
        if i % 2 == 1:
            fig.add_artist(Rectangle((X(left), Y(y0 + row_h)), X(width - 2 * left) - X(0), row_h / height,
                                     transform=fig.transFigure, color=ZEBRA, lw=0, zorder=0))
        x = left
        for (header, w, align), cell in zip(columns, row):
            text, colour = cell if isinstance(cell, tuple) else (cell, None)
            tx = x + (w - 0.08 if align == "r" else 0.04)
            if colour:
                fig.text(X(tx), Y(y0 + row_h * 0.68), "●", color=colour, fontsize=9, va="baseline")
                tx += 0.17
            fig.text(X(tx), Y(y0 + row_h * 0.68), text, ha="right" if align == "r" else "left", fontsize=8.5,
                     va="baseline", color=INK, family="DejaVu Sans")
            x += w
    y_end = top + head_h + row_h * len(rows)
    fig.add_artist(plt.Line2D([X(left), X(width - left)], [Y(y_end)] * 2, color=AXIS, lw=0.8))
    for k, line in enumerate(foot_lines):
        fig.text(X(left), Y(y_end + 0.28 + 0.17 * k), line, fontsize=7, color=MUTED, va="baseline")
    return fig


def _hidden_note(names, what=None):
    if not names:
        return None
    verb = "it fires" if len(names) == 1 else "they fire"
    return (f"Not shown: {', '.join(names)} ({verb} in ≥ {SATURATED:.0%} of the events of this sample: the sample "
            "was selected with HLT_Physics or a similar path).")


def _category_cell(cat):
    return (cat, CATEGORY_COLORS.get(cat, MUTED))


def _short(name, n=45):
    return name if len(name) <= n else name[: n - 1] + "…"


# ---- tables ---------------------------------------------------------------------------------------------
def fig_top_paths(res, kind="hlt", n=10):
    """Table of the ``n`` HLT paths (kind "hlt") or L1 seeds (kind "l1") with the highest rates.
    Dataset_ bits and excluded paths that fire in (nearly) every event are left out."""
    recs = res.records("paths_hlt" if kind == "hlt" else "paths_l1")
    if kind == "hlt":
        recs = [r for r in recs if r.get("kind", "hlt") == "hlt"]
    else:
        recs = [r for r in recs if r.get("seed", True)]
    hidden = [r["name"] for r in recs if res.saturated(r)]
    recs = sorted((r for r in recs if not res.saturated(r)), key=lambda r: -r["rate_hz"])[:n]
    if kind == "hlt":
        cols = [("#", 0.35, "r"), ("HLT path", 3.9, "l"), ("category", 1.15, "l"), ("rate [Hz]", 1.45, "r"),
                ("pure [Hz]", 1.15, "r"), ("PS", 0.55, "r"), ("datasets", 2.6, "l")]
        rows = [[str(i + 1), _short(r["name"], 52), _category_cell(r["category"]), fmt_rate_err(r["rate_hz"], r["rate_err_hz"]),
                 fmt_rate(r["pure_rate_hz"]), str(r.get("menu_prescale", "")) if r.get("menu_prescale") is not None else "",
                 _short(", ".join(r.get("datasets") or []) or "–", 30)] for i, r in enumerate(recs)]
        title = f"Top {len(recs)} HLT paths by rate"
        note = ("pure: events in which no other path of the same reference group fired (standard+parking paths w.r.t. "
                "standard+parking, scouting w.r.t. scouting; other paths: 0). PS: menu prescale (default column).")
    else:
        cols = [("#", 0.35, "r"), ("L1 seed", 3.6, "l"), ("rate [Hz]", 1.6, "r"), ("pure [Hz]", 1.2, "r"),
                ("pure / rate", 1.0, "r")]
        rows = [[str(i + 1), r["name"], fmt_rate_err(r["rate_hz"], r["rate_err_hz"]), fmt_rate(r["pure_rate_hz"]),
                 fmt_pct(r["pure_rate_hz"] / r["rate_hz"] if r["rate_hz"] else None)] for i, r in enumerate(recs)]
        title = f"Top {len(recs)} L1 seeds by rate"
        note = ("Rates of the L1 decision bits stored in the ntuples (Run-3 NanoAOD: after L1 prescales and masks). "
                "pure: no other L1 seed fired.")
    hidden_note = _hidden_note(hidden, "they")
    return fig_table(cols, rows, title, res.context(), " ".join(x for x in (note, hidden_note) if x))


def fig_top_groups(res, level="datasets", n=10, categories=None):
    """Table of the ``n`` datasets or streams (split copies combined) with the highest rates."""
    recs = res.records(level)
    if categories:
        recs = [r for r in recs if r["category"] in categories]
    hidden = [r["name"] for r in recs if res.saturated(r)]
    recs = sorted((r for r in recs if not res.saturated(r)), key=lambda r: -r["rate_hz"])[:n]
    member = "paths" if level == "datasets" else "datasets"
    cols = [("#", 0.35, "r"), (level[:-1], 2.6, "l"), ("category", 1.15, "l"), ("rate [Hz]", 1.45, "r"),
            ("pure [Hz]", 1.15, "r"), ("pure / rate", 0.95, "r"), ("split", 0.6, "r"),
            (member, 0.75 if level == "datasets" else 2.9, "r" if level == "datasets" else "l")]
    rows = []
    for i, r in enumerate(recs):
        members = str(r.get("n_paths", "")) if level == "datasets" else _short(", ".join(r.get("datasets") or []), 34)
        rows.append([str(i + 1), r["name"], _category_cell(r["category"]), fmt_rate_err(r["rate_hz"], r["rate_err_hz"]),
                     fmt_rate(r["pure_rate_hz"]), fmt_pct(r["pure_rate_hz"] / r["rate_hz"] if r["rate_hz"] else None),
                     str(r.get("n_split", 1)), members])
    note = (f"Split {level} (e.g. EGamma0-5) are combined into one row; 'split' = number of copies. "
            "pure: no other standard/parking (scouting: scouting) " + level + " fired; other " + level + ": 0.")
    return fig_table(cols, rows, f"Top {len(recs)} {level} by rate", res.context(),
                     " ".join(x for x in (note, _hidden_note(hidden, "they")) if x))


# ---- bar charts -----------------------------------------------------------------------------------------
TOTALS_SHOWN = [("all_datasets", "all datasets (written)"), ("physics", "physics (std + parking + scouting)"),
                ("standard_parking", "standard + parking"), ("standard", "standard"), ("parking", "parking"),
                ("scouting", "scouting"), ("scouting_and_standard_parking", "scouting ∩ (standard + parking)"),
                ("other", "calibration / monitoring")]


def fig_rate_budget(res):
    """Output totals as horizontal bars (an event counts once per total, overlaps removed)."""
    totals = res.summary["totals"]
    items = [(label, totals[key]) for key, label in TOTALS_SHOWN if key in totals]
    colours = {"standard": CATEGORY_COLORS["standard"], "parking": CATEGORY_COLORS["parking"],
               "scouting": CATEGORY_COLORS["scouting"], "calibration / monitoring": CATEGORY_COLORS["other"]}
    fig, ax = plt.subplots(figsize=(8.5, 0.42 * len(items) + 1.5))
    y = np.arange(len(items))[::-1]
    vals = [t["rate_hz"] for _, t in items]
    # combined totals in neutral ink, single categories in their colours
    ax.barh(y, vals, height=0.62, color=[colours.get(lbl, INK2) for lbl, _ in items], edgecolor=SURFACE, lw=1)
    for yi, (lbl, t) in zip(y, items):
        ax.text(t["rate_hz"] + max(vals) * 0.01, yi, f"{fmt_rate(t['rate_hz'])} Hz  ({fmt_pct(t['fraction_of_input'])} of input)",
                va="center", fontsize=8, color=INK2)
    ax.set_yticks(y, [lbl for lbl, _ in items])
    ax.set_xlim(0, max(vals) * 1.32)
    ax.set_xlabel("rate [Hz]")
    ax.grid(axis="x")
    ax.set_axisbelow(True)
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    _thousands(ax)
    inp = totals["input"]
    top = _header(fig, "Output rates by category", [res.context(), f"HLT input (L1 accept) rate: "
                  f"{fmt_rate_err(inp['rate_hz'], inp['rate_err_hz'])} Hz; each event counts once per bar"])
    fig.tight_layout(rect=(0, 0, 1, top))
    return fig


def _header(fig, title, lines):
    """Title and subtitle lines at the top left of a figure; returns the top of the free area (figure
    fraction) for ``tight_layout(rect=...)``."""
    w, h = fig.get_figwidth(), fig.get_figheight()
    fig.text(0.15 / w, 1 - 0.32 / h, title, fontsize=12, fontweight="bold", va="baseline")
    lines = [ln for ln in lines if ln]
    for k, line in enumerate(lines):
        fig.text(0.15 / w, 1 - (0.56 + 0.2 * k) / h, line, fontsize=8, color=INK2, va="baseline")
    return 1 - (0.62 + 0.2 * len(lines)) / h


def _thousands(ax, axis="x"):
    fmt = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}")
    (ax.xaxis if axis == "x" else ax.yaxis).set_major_formatter(fmt)


def _stacked_hbar(ax, names, parts, colours, labels, right_text):
    y = np.arange(len(names))[::-1]
    left = np.zeros(len(names))
    for vals, c, lab in zip(parts, colours, labels):
        ax.barh(y, vals, left=left, height=0.64, color=c, edgecolor=SURFACE, lw=1, label=lab)
        left = left + np.asarray(vals)
    xmax = left.max() if len(left) else 1
    for yi, t in zip(y, right_text):
        ax.text(left[len(names) - 1 - yi] + xmax * 0.01, yi, t, va="center", fontsize=7.5, color=INK2)
    ax.set_yticks(y, names)
    ax.set_xlim(0, xmax * 1.25)
    ax.grid(axis="x")
    ax.set_axisbelow(True)
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.legend(loc="lower right", ncols=len(parts))


def fig_pure_shared(res, level="datasets", n=15):
    """Standard + parking datasets (or streams): pure part and the part shared with other ones."""
    recs = [r for r in res.records(level) if r["category"] in ("standard", "parking") and not res.saturated(r)
            and r["rate_hz"] > 0]
    recs = sorted(recs, key=lambda r: -r["rate_hz"])[:n]
    names = [r["name"] for r in recs]
    pure = [r["pure_rate_hz"] for r in recs]
    shared = [r["rate_hz"] - r["pure_rate_hz"] for r in recs]
    fig, ax = plt.subplots(figsize=(8.5, 0.32 * len(recs) + 1.7))
    _stacked_hbar(ax, names, [pure, shared], [BLUE_DARK, BLUE_LIGHT],
                  ["pure (no other standard/parking " + level[:-1] + ")", "shared with others"],
                  [f"{fmt_rate(r['rate_hz'])} Hz, {fmt_pct(r['pure_rate_hz'] / r['rate_hz'] if r['rate_hz'] else 0)} pure"
                   for r in recs])
    ax.set_xlabel("rate [Hz]")
    _thousands(ax)
    top = _header(fig, f"Standard and parking {level}: pure vs shared rate",
                  [res.context(), f"top {len(recs)} by rate; split copies combined; pure = no other standard/parking "
                   f"{level[:-1]} took the event"])
    fig.tight_layout(rect=(0, 0, 1, top))
    return fig


def fig_scouting_shared(res, n=15):
    """Scouting paths: the part of their rate in events that standard/parking paths store as well."""
    recs = [r for r in res.records("paths_hlt") if r.get("category") == "scouting" and r.get("kind", "hlt") == "hlt"
            and r.get("shared_with_standard_parking_rate_hz") is not None and r["rate_hz"] > 0]
    recs = sorted(recs, key=lambda r: -r["rate_hz"])[:n]
    if not recs:
        return None
    shared = [r["shared_with_standard_parking_rate_hz"] for r in recs]
    only = [r["rate_hz"] - s for r, s in zip(recs, shared)]
    fig, ax = plt.subplots(figsize=(8.5, 0.32 * len(recs) + 1.6))
    _stacked_hbar(ax, [r["name"] for r in recs], [only, shared],
                  [CATEGORY_COLORS["scouting"], CATEGORY_COLORS["standard"]],
                  ["scouting only", "also stored by standard/parking paths"],
                  [f"{fmt_rate(r['rate_hz'])} Hz, {fmt_pct(r['shared_with_standard_parking_fraction'])} shared" for r in recs])
    t = res.summary["totals"]
    sub = ""
    if "scouting" in t and "scouting_and_standard_parking" in t:
        sub = (f"all scouting: {fmt_rate(t['scouting']['rate_hz'])} Hz, of which "
               f"{fmt_rate(t['scouting_and_standard_parking']['rate_hz'])} Hz "
               f"({fmt_pct(t['scouting_and_standard_parking']['rate_hz'] / t['scouting']['rate_hz'])}) also in standard/parking")
    ax.set_xlabel("rate [Hz]")
    _thousands(ax)
    top = _header(fig, "Scouting paths: overlap with standard + parking", [res.context(), sub])
    fig.tight_layout(rect=(0, 0, 1, top))
    return fig


# ---- overlap matrices -----------------------------------------------------------------------------------
def fig_overlap_matrix(ov, names, title, subtitle, groups=None, rates=None, annotate=True, note=None):
    """Matrix of overlap fractions: cell (i, j) = fraction of the events of row i in which column j
    also fired (so rows are read as "of my events, x% also fired j"). The diagonal is grey.

    ``groups``: optional category per name; consecutive blocks get separator lines and a label.
    ``rates``: optional rate per name, shown in the row labels. ``note``: extra line(s) under the title."""
    frac = ov.fraction_of_row(names)
    k = len(names)
    cell = 0.36 if k <= 15 else 0.3
    notes = textwrap.wrap(note, 150) if note else []
    head = 1.0 + 0.2 * len(notes)
    label_w = max(len(n) for n in names) * 0.056 + (0.95 if rates else 0.2)
    width = max(label_w + cell * k + 1.5, 7.5)
    height = head + cell * k + max(len(n) for n in names) * 0.056 + 0.3
    fig = plt.figure(figsize=(width, height))
    ax = fig.add_axes([label_w / width, 0.25 / height, cell * k / width, cell * k / height])
    shown = frac.copy()
    np.fill_diagonal(shown, np.nan)
    cmap = SEQ_CMAP.copy()
    cmap.set_bad(GRID)
    im = ax.imshow(shown, cmap=cmap, vmin=0, vmax=1, aspect="equal", interpolation="nearest")
    if annotate:
        for i in range(k):
            for j in range(k):
                if i != j and frac[i, j] >= 0.005:
                    v = frac[i, j]
                    ax.text(j, i, f"{100 * v:.0f}" if v >= 0.095 else f"{100 * v:.1f}", ha="center", va="center",
                            fontsize=6.5 if k > 15 else 7, color=SURFACE if v > 0.55 else INK)
    ylabels = [f"{n}  ({fmt_rate(r)} Hz)" for n, r in zip(names, rates)] if rates else names
    ax.set_yticks(range(k), ylabels, fontsize=7.5)
    ax.set_xticks(range(k), names, rotation=90, fontsize=7.5)
    ax.xaxis.tick_top()
    ax.tick_params(length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_xticks(np.arange(-0.5, k, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, k, 1), minor=True)
    ax.grid(which="minor", color=SURFACE, lw=1)
    ax.tick_params(which="minor", length=0)
    if groups:
        bounds = [i for i in range(1, k) if groups[i] != groups[i - 1]]
        for b in bounds:
            ax.axhline(b - 0.5, color=INK2, lw=0.8)
            ax.axvline(b - 0.5, color=INK2, lw=0.8)
        starts = [0] + bounds
        ends = bounds + [k]
        for s, e in zip(starts, ends):
            ax.text(k - 0.3, (s + e - 1) / 2, f" {groups[s]}", va="center", ha="left", fontsize=7.5,
                    color=CATEGORY_COLORS.get(groups[s], INK2), fontweight="bold", clip_on=False)
    cax = fig.add_axes([(label_w + cell * k + (0.85 if groups else 0.25)) / width, 0.25 / height, 0.12 / width,
                        min(2.5, cell * k) / height])
    cb = fig.colorbar(im, cax=cax)
    cb.set_label("fraction of the row's events", fontsize=7.5, color=INK2)
    cb.ax.tick_params(labelsize=7)
    cb.outline.set_visible(False)
    fig.text(0.2 / width, 1 - 0.3 / height, title, fontsize=12, fontweight="bold", va="baseline")
    fig.text(0.2 / width, 1 - 0.52 / height, subtitle, fontsize=8, color=INK2, va="baseline")
    fig.text(0.2 / width, 1 - 0.72 / height, "cell (row, column): % of the row's events in which the column "
             "also fired", fontsize=7.5, color=MUTED, va="baseline")
    for i, line in enumerate(notes):
        fig.text(0.2 / width, 1 - (0.92 + 0.2 * i) / height, line, fontsize=7.5, color=MUTED, va="baseline")
    return fig


def fig_group_overlaps(res, level="streams", n=20, categories=("standard", "parking", "scouting")):
    """Overlap matrix of the ``n`` highest-rate datasets/streams of the given categories, ordered by
    category and rate."""
    ov = res.overlap(level)
    recs = [r for r in res.records(level) if r["category"] in categories and r["name"] in ov.row_index
            and r["rate_hz"] > 0]
    hidden = [r["name"] for r in recs if res.saturated(r)]
    recs = sorted((r for r in recs if not res.saturated(r)), key=lambda r: -r["rate_hz"])[:n]
    recs.sort(key=lambda r: (CATEGORY_ORDER.index(r["category"]) if r["category"] in CATEGORY_ORDER else 9, -r["rate_hz"]))
    names = [r["name"] for r in recs]
    note = (f"not shown (they fire in ≥ {SATURATED:.0%} of the events of this sample): {', '.join(hidden)}"
            if hidden else None)
    return fig_overlap_matrix(ov, names, f"{level.capitalize()}: overlaps (top {len(names)} by rate, split copies combined)",
                              res.context(), groups=[r["category"] for r in recs], rates=[r["rate_hz"] for r in recs],
                              note=note)


WRITTEN = ("standard", "parking", "scouting")


def path_partners(res, path, n=10, kinds=("hlt",), categories=WRITTEN):
    """The ``n`` paths with the largest shared rate with ``path``:
    ``[(name, shared_rate_hz, fraction_of_path, fraction_of_partner, partner_rate_hz)]``.
    ``categories``: only partners of these categories (default: paths in physics datasets, i.e. paths
    whose events are written; None = all)."""
    ov = res.overlap("paths")
    if path not in ov.row_index:
        raise KeyError(f"{path} is not in the path x path overlaps")
    recs = {r["name"]: r for r in res.records("paths_hlt")}
    i = ov.row_index[path]
    ni = ov.count[i, i]
    out = []
    for j, name in enumerate(ov.rows):
        r = recs.get(name)
        if j == i or not r or r.get("kind", "hlt") not in kinds or ov.count[i, j] <= 0 or res.saturated(r):
            continue
        if categories and r.get("category") not in categories:
            continue
        nj = ov.count[j, j]
        out.append((name, ov.rate[i, j], ov.count[i, j] / ni, ov.count[i, j] / nj if nj else 0.0, r["rate_hz"]))
    out.sort(key=lambda t: -t[1])
    return out[:n]


def pick_example_path(res, min_rate_hz=50.0, min_fraction=0.3, categories=("standard",)):
    """A path with many strong overlaps (default for the path figures): among the non-excluded
    standard paths with at least ``min_rate_hz``, the one with the most partners that fire in at
    least ``min_fraction`` of its events."""
    ov = res.overlap("paths")
    best, best_n = None, -1
    for r in res.records("paths_hlt"):
        if (r.get("kind", "hlt") != "hlt" or r.get("excluded") or r.get("category") not in categories
                or r["rate_hz"] < min_rate_hz or r["name"] not in ov.row_index):
            continue
        i = ov.row_index[r["name"]]
        n_strong = int((ov.count[i] >= min_fraction * ov.count[i, i]).sum()) - 1
        if n_strong > best_n:
            best, best_n = r["name"], n_strong
    return best


def _partner_note(categories):
    return ("partners: paths in " + "/".join(categories) + " datasets" if categories else "partners: all HLT paths")


def fig_path_partners(res, path, n=10, categories=WRITTEN):
    """Bars: the paths with the largest overlap with ``path``, both fractions."""
    partners = path_partners(res, path, n, categories=categories)
    rec = next(r for r in res.records("paths_hlt") if r["name"] == path)
    names = [p[0] for p in partners]
    y = np.arange(len(partners))[::-1]
    fig, ax = plt.subplots(figsize=(9.5, 0.42 * len(partners) + 2.2))
    h = 0.36
    ax.barh(y + h / 2, [p[2] for p in partners], height=h, color=CATEGORY_COLORS["standard"], edgecolor=SURFACE,
            lw=0.8, label=f"share of {path}'s events in which the path fired")
    ax.barh(y - h / 2, [p[3] for p in partners], height=h, color=CATEGORY_COLORS["parking"], edgecolor=SURFACE,
            lw=0.8, label=f"share of the path's events in which {path} fired")
    for yi, p in zip(y, partners):
        ax.text(1.02, yi, f"{fmt_rate(p[1])} Hz shared  (path: {fmt_rate(p[4])} Hz)", va="center", fontsize=7.5,
                color=INK2, transform=ax.get_yaxis_transform())
    ax.set_yticks(y, names, fontsize=8)
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax.grid(axis="x")
    ax.set_axisbelow(True)
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08), ncols=1)
    top = _header(fig, f"{path}: the {len(partners)} paths with the largest overlap",
                  [res.context(), f"{path}: {fmt_rate_err(rec['rate_hz'], rec['rate_err_hz'])} Hz, pure "
                   f"{fmt_rate(rec['pure_rate_hz'])} Hz ({rec.get('category')}); sorted by shared rate; "
                   + _partner_note(categories)])
    fig.tight_layout(rect=(0, 0, 1, top))
    return fig


def fig_path_matrix(res, path, n=10, categories=WRITTEN):
    """Overlap matrix of ``path`` and its ``n`` most overlapping paths."""
    names = [path] + [p[0] for p in path_partners(res, path, n, categories=categories)]
    recs = {r["name"]: r for r in res.records("paths_hlt")}
    return fig_overlap_matrix(res.overlap("paths"), names, f"{path} and its {len(names) - 1} most overlapping paths",
                              res.context(), rates=[recs[nm]["rate_hz"] for nm in names],
                              note=_partner_note(categories))


# ---- gallery -----------------------------------------------------------------------------------------------
CAPTIONS = {
    "rate_budget": "Output rates by category. Each event counts once per bar (overlaps removed).",
    "top_hlt_paths": "HLT paths with the highest rates, with pure rates and the datasets they feed.",
    "top_l1_seeds": "L1 seeds with the highest rates; pure = no other L1 seed fired.",
    "top_datasets": "Datasets (split copies combined) with the highest rates.",
    "top_streams": "Streams (split copies combined) with the highest rates.",
    "datasets_pure_shared": "Standard and parking datasets: the part of the rate no other standard/parking dataset takes.",
    "scouting_shared": "Scouting paths: the part of their rate that standard/parking paths store as well.",
    "overlap_streams": "Stream x stream overlaps: % of the row's events that the column stream also takes.",
    "overlap_datasets": "Dataset x dataset overlaps: % of the row's events that the column dataset also takes.",
    "partners_": "The paths with the largest shared rate with this path, as fractions of both.",
    "matrix_": "This path and its most overlapping paths: % of the row's events in which the column fired.",
}


def write_index(out_dir, names, res, fmt="png"):
    """A self-contained HTML page showing the figures (opened next to them, e.g. in a browser)."""
    import html
    items = []
    for name in names:
        caption = next((c for k, c in CAPTIONS.items() if name == k or (k.endswith("_") and name.startswith(k))), "")
        items.append(f'<figure><a href="{name}.{fmt}"><img src="{name}.{fmt}" alt="{html.escape(name)}"></a>'
                     f'<figcaption><b>{html.escape(name)}</b> {html.escape(caption)} '
                     f'<a href="{name}.pdf">PDF</a></figcaption></figure>')
    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(res.summary.get("measurement", "steamrates"))} plots</title>
<style>
 :root {{ --ink: {INK}; --ink2: {INK2}; --muted: {MUTED}; --line: {GRID}; --bg: #f9f9f7; --card: #ffffff; }}
 body {{ margin: 0; padding: 24px 16px; background: var(--bg); color: var(--ink);
        font: 15px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }}
 main {{ max-width: 1100px; margin: 0 auto; }}
 h1 {{ font-size: 22px; margin: 0 0 4px; }}
 p.context {{ color: var(--ink2); margin: 0 0 24px; }}
 figure {{ margin: 0 0 28px; background: var(--card); border: 1px solid var(--line); border-radius: 8px; padding: 12px; }}
 img {{ width: 100%; height: auto; display: block; }}
 figcaption {{ color: var(--ink2); font-size: 14px; margin-top: 8px; }}
 a {{ color: #256abf; }}
</style></head>
<body><main>
<h1>{html.escape(res.summary.get("measurement", ""))}</h1>
<p class="context">{html.escape(res.context())}. Written by <code>python3 -m steamrates plots</code> from the merged
results; click a figure for full size.</p>
{chr(10).join(items)}
</main></body></html>
"""
    with open(os.path.join(out_dir, "index.html"), "w") as f:
        f.write(page)


# ---- driver -----------------------------------------------------------------------------------------------
def _safe(name):
    return "".join(c if c.isalnum() or c in "_-" else "_" for c in name)


def make_all(results_dir, out_dir, top=10, paths=None, formats=("png", "pdf"), partner_categories=WRITTEN):
    """Write every figure; returns the list of written base names."""
    _style()
    res = Results(results_dir)
    os.makedirs(out_dir, exist_ok=True)
    written = []

    def save(fig, name):
        if fig is None:
            return
        for ext in formats:
            fig.savefig(os.path.join(out_dir, f"{name}.{ext}"), dpi=200)
        plt.close(fig)
        written.append(name)

    save(fig_rate_budget(res), "rate_budget")
    save(fig_top_paths(res, "hlt", top), "top_hlt_paths")
    if res.has("paths_l1") and any(r.get("seed", True) for r in res.records("paths_l1")):
        save(fig_top_paths(res, "l1", top), "top_l1_seeds")
    has_groups = res.has("datasets") and bool(res.records("datasets"))
    if has_groups:
        save(fig_top_groups(res, "datasets", top), "top_datasets")
        save(fig_top_groups(res, "streams", top), "top_streams")
        save(fig_pure_shared(res, "datasets", 15), "datasets_pure_shared")
        if res.has("overlaps_streams"):
            save(fig_group_overlaps(res, "streams", 20), "overlap_streams")
        if res.has("overlaps_datasets"):
            save(fig_group_overlaps(res, "datasets", 20), "overlap_datasets")
    save(fig_scouting_shared(res), "scouting_shared")
    if res.has("overlaps_paths"):
        for path in (paths or [pick_example_path(res)]):
            if path is None:
                continue
            save(fig_path_partners(res, path, top, partner_categories), f"partners_{_safe(path)}")
            save(fig_path_matrix(res, path, top, partner_categories), f"matrix_{_safe(path)}")
    if "png" in formats:
        write_index(out_dir, written, res)
    return written


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates plots", description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__.split("\n\n", 1)[1])
    p.add_argument("workdir", help="work dir (uses WORKDIR/results) or a results directory")
    p.add_argument("-o", "--output", help="output directory (default: WORKDIR/plots)")
    p.add_argument("--top", type=int, default=10, help="rows of the top-N tables and partners (default: 10)")
    p.add_argument("--path", action="append", help="HLT path for the partner figures (repeatable; default: "
                   "a standard path with many strong overlaps)")
    p.add_argument("--all-partners", action="store_true", help="partner figures: all HLT paths, also those in "
                   "no physics dataset (default: only paths in standard/parking/scouting datasets)")
    p.add_argument("--format", nargs="+", default=["png", "pdf"], help="file formats (default: png pdf)")
    args = p.parse_args(argv)
    results = args.workdir if os.path.exists(os.path.join(args.workdir, "summary.json")) else \
        os.path.join(args.workdir, "results")
    out = args.output or os.path.join(os.path.dirname(os.path.abspath(results)), "plots")
    written = make_all(results, out, args.top, args.path, tuple(args.format),
                       None if args.all_partners else WRITTEN)
    print(f"wrote {len(written)} figures to {out}/: {', '.join(written)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
