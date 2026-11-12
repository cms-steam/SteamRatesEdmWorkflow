"""Normalization: from counted events (sums of weights) to rates in Hz.

Every rate is ``rate = Σw x scale`` and its statistical uncertainty ``sqrt(Σw²) x scale``, where Σw
is the sum of event weights of the events in question (data: the number of events) and ``scale``
(Hz per unit weight) depends only on the sample. The sample's ``normalization`` (sample catalog,
RAW entry) chooses one of three modes; ``target_lumi`` comes from the measurement config.

Data (``mode: data``)::

    scale = prescale x (target_lumi / lumi) / (N_LS x LS_LENGTH_S)

    prescale     the sample's prescale: all its datasets together record 1 in ``prescale`` events
                 (e.g. 220 for EphemeralHLTPhysics0-7, 5350 for HLTPhysics in run 403663)
    lumi         average instantaneous luminosity of the counted lumi sections (1e34 cm^-2 s^-1)
    target_lumi  luminosity to scale to (default: lumi); the scaling is linear
    N_LS         number of distinct (run, lumi section) of the sample, counted once however many
                 files and datasets it is spread over. Taken from the input files' LuminosityBlocks
                 trees after the lumi JSON (this also counts lumi sections without selected
                 events); if no file has that tree, from the lumi sections with selected events.

    The formula assumes the ntuples hold every event of each counted lumi section (catalog
    ``complete: true``, or a lumi JSON of the complete lumi sections; see steamrates.catalog).

    Per-lumi-section luminosity or prescale tables (e.g. from brilcalc) are not supported yet. They
    would have to become per-event weights at counting time, because the combination table does not
    know which lumi section an event came from.

MC, cross section x luminosity (``mode: cross_section``)::

    scale = xsec_pb x filter_eff x target_lumi x 0.01 / Σw_total        (1 pb x 1e34 cm^-2 s^-1 = 0.01 Hz)

MC, fraction of bunch crossings (``mode: bunch_crossing``; MinBias/NuGun at fixed pileup)::

    scale = n_bunches x F_REV_HZ / Σw_total

    i.e. rate = (fraction of crossings that fire) x (collision rate). No luminosity scaling: the
    pileup, and thus the luminosity, is fixed by the sample.

Σw_total (MC) is the sum of weights of all **generated** events, from the ``Runs`` trees of the
input files: ``genEventSumw`` if the events were counted with weights (``weight_branch``), else
``genEventCount``. These include events that the ntuple production did not keep (e.g. only
triggered events stored), which is what makes such ntuples usable. If no file has these sums, Σw_total
is the sum over the counted events (correct only if the ntuples contain every generated event). If
some files have them and others do not, or a file was not read completely, there is no correct
choice and the merge stops.
"""

from __future__ import annotations

from dataclasses import dataclass, field

F_REV_HZ = 11245.5        # LHC revolution frequency: c / 26658.883 m (= 400.79 MHz RF / 35640 buckets)
LS_ORBITS = 2 ** 18       # orbits per lumi section
LS_LENGTH_S = LS_ORBITS / F_REV_HZ   # 23.3110 s (the legacy code used 23.31 s: rates 4.3e-5 higher)
PB_TIMES_1E34_IN_HZ = 0.01           # 1 pb x 1e34 cm^-2 s^-1 = 1e-36 cm^2 x 1e34 cm^-2 s^-1


class NormalizationError(ValueError):
    pass


@dataclass
class Normalization:
    mode: str
    scale: float                         # Hz per unit weight
    details: dict                        # every input of the formula, and where it came from
    warnings: list = field(default_factory=list)

    def rate(self, sumw):
        return sumw * self.scale

    def to_dict(self):
        return {"mode": self.mode, "scale_hz_per_unit_weight": self.scale, **self.details}


# --------------------------------------------------------------------------------------------------
# Data
# --------------------------------------------------------------------------------------------------

def count_lumi_sections(lumi_sets, lumi_mask_n=None):
    """Choose N_LS from the merged lumi information.

    ``lumi_sets``: ``{"in_file": set, "with_events": set}`` of (run, lumi) of one sample (see
    steamrates.partial). Returns ``(n_ls, details, warnings)``.
    """
    in_file, with_events = lumi_sets["in_file"], lumi_sets["with_events"]
    warnings = []
    details = {"n_ls_in_files": len(in_file), "n_ls_with_events": len(with_events),
               "n_ls_in_lumi_json": lumi_mask_n}
    if in_file:
        n_ls, source = len(in_file), "LuminosityBlocks trees of the input files (after the lumi JSON)"
        without_tree = with_events - in_file
        if without_tree:
            warnings.append(f"{len(without_tree)} lumi section(s) have selected events but are not in any "
                            "LuminosityBlocks tree; they are not counted in N_LS")
    elif with_events:
        n_ls, source = len(with_events), "lumi sections with selected events (no LuminosityBlocks tree)"
        warnings.append("the input files have no LuminosityBlocks tree: N_LS counts only lumi sections "
                        "with at least one selected event")
    else:
        raise NormalizationError("no lumi sections found: no event was selected (check the lumi JSON)")
    if lumi_mask_n is not None and n_ls < lumi_mask_n:
        warnings.append(f"the lumi JSON has {lumi_mask_n} lumi sections, the ntuples only {n_ls}: the "
                        "normalization uses the ones found in the ntuples")
    details.update(n_ls=n_ls, n_ls_source=source,
                   runs=sorted({r for r, _ in (in_file or with_events)}))
    return n_ls, details, warnings


def data_normalization(norm, n_ls, lumi_details=None):
    """``norm``: the RAW entry's normalization plus ``target_lumi`` (steamrates.config)."""
    target = norm.get("target_lumi") or norm["lumi"]
    scale = norm["prescale"] * (target / norm["lumi"]) / (n_ls * LS_LENGTH_S)
    details = dict(lumi_details or {}, prescale=norm["prescale"], n_datasets=norm.get("n_datasets"),
                   lumi=norm["lumi"], target_lumi=target, n_ls=n_ls, ls_length_s=LS_LENGTH_S,
                   formula="prescale x (target_lumi / lumi) / (n_ls x ls_length_s)")
    return Normalization("data", scale, details)


# --------------------------------------------------------------------------------------------------
# MC
# --------------------------------------------------------------------------------------------------

def mc_total_weight(input_records, sumw_processed, weighted):
    """Σw_total of all generated events. Returns ``(value, details, warnings)``.

    ``input_records``: the per-file records of all partials (steamrates.count: ``n_events``,
    ``n_read``, ``mc_sums``); ``sumw_processed``: Σw (or the number) of the counted events;
    ``weighted``: the events were counted with weights.
    """
    key = "genEventSumw" if weighted else "genEventCount"
    n = len(input_records)
    have = [r for r in input_records if key in (r.get("mc_sums") or {})]
    partly_read = [r["path"] for r in input_records if r.get("n_read") != r.get("n_events")]
    details = {"sumw_processed": sumw_processed, "sumw_total_key": key}
    if have and len(have) == n:
        if partly_read:
            raise NormalizationError(f"{len(partly_read)} input file(s) were not read completely (e.g. "
                                     f"{partly_read[0]}): their {key} would not match the counted events")
        total = sum(r["mc_sums"][key] for r in have)
        details.update(sumw_total=total, sumw_total_source=f"{key} of the Runs trees of all {n} input files")
        warnings = []
        if total and abs(sumw_processed - total) > 1e-9 * abs(total):
            details["fraction_kept_in_ntuples"] = sumw_processed / total
        return total, details, warnings
    if have:
        raise NormalizationError(f"only {len(have)} of {n} input files have {key} in their Runs tree; "
                                 "the total number of generated events is unknown")
    details.update(sumw_total=sumw_processed, sumw_total_source="the counted events (no Runs-tree sums)")
    warnings = [f"the input files have no {key}: the normalization assumes the ntuples contain every "
                "generated event (no event selection at production)"]
    return sumw_processed, details, warnings


def cross_section_normalization(norm, sumw_total, sumw_details=None):
    target = norm.get("target_lumi")
    if not sumw_total:
        raise NormalizationError("the total sum of weights is 0")
    scale = norm["xsec_pb"] * norm["filter_eff"] * target * PB_TIMES_1E34_IN_HZ / sumw_total
    details = dict(sumw_details or {}, xsec_pb=norm["xsec_pb"], filter_eff=norm["filter_eff"], target_lumi=target,
                   formula="xsec_pb x filter_eff x target_lumi x 0.01 / sumw_total")
    return Normalization("cross_section", scale, details)


def bunch_crossing_normalization(norm, sumw_total, sumw_details=None):
    if not sumw_total:
        raise NormalizationError("the total sum of weights is 0")
    scale = norm["n_bunches"] * F_REV_HZ / sumw_total
    details = dict(sumw_details or {}, n_bunches=norm["n_bunches"], f_rev_hz=F_REV_HZ,
                   collision_rate_hz=norm["n_bunches"] * F_REV_HZ,
                   formula="n_bunches x f_rev_hz / sumw_total")
    return Normalization("bunch_crossing", scale, details)


def build(norm, *, lumi_sets=None, lumi_mask_n=None, input_records=(), sumw_processed=None, weighted=False):
    """The :class:`Normalization` for a measurement.

    ``norm`` is ``Measurement.normalization()`` (RAW entry normalization + target_lumi); data
    needs ``lumi_sets``, MC needs ``input_records`` and ``sumw_processed``.
    """
    mode = norm["mode"]
    if mode == "data":
        n_ls, details, warnings = count_lumi_sections(lumi_sets, lumi_mask_n)
        result = data_normalization(norm, n_ls, details)
    elif mode in ("cross_section", "bunch_crossing"):
        total, details, warnings = mc_total_weight(list(input_records), sumw_processed, weighted)
        make = cross_section_normalization if mode == "cross_section" else bunch_crossing_normalization
        result = make(norm, total, details)
    else:
        raise NormalizationError(f"unknown normalization mode {mode!r}")
    result.warnings = warnings + result.warnings
    return result
