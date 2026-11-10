"""Cross-check the raw-byte EDM reader (steamrates.readers.edm) against FWLite, event by event.

Usage (repository root, inside cmsenv)::

    python3 validation/check_edm_reader_fwlite.py /eos/.../hlt_7.root [more files]

Compares path names, accept bits and run/lumi/event of every event. Use it whenever a new CMSSW
release or a new ntuple production might have changed the TriggerResults/EventAuxiliary layout.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import numpy as np  # noqa: E402

from steamrates.readers import EdmReader  # noqa: E402


def check(path):
    import ROOT  # noqa: F401  (loads FWLite via DataFormats.FWLite)
    from DataFormats.FWLite import Events, Handle

    reader = EdmReader()
    info = reader.open(path)
    chunks = list(reader.chunks())
    run = np.concatenate([c.run for c in chunks])
    lumi = np.concatenate([c.lumi for c in chunks])
    event = np.concatenate([c.event for c in chunks])
    bits = np.concatenate([c.bits for c in chunks])

    handle = Handle("edm::TriggerResults")
    bad = 0
    n = 0
    for i, e in enumerate(Events(path)):
        e.getByLabel(("TriggerResults", "", info.menu.process), handle)
        tr = handle.product()
        if i == 0:
            tn = e.object().triggerNames(tr)
            if [tn.triggerName(j) for j in range(tr.size())] != chunks[0].bit_names:
                print(f"{path}: PATH NAMES DIFFER")
                bad += 1
        aux = e.object().eventAuxiliary()
        if (aux.run(), aux.luminosityBlock(), aux.event()) != (run[i], lumi[i], event[i]):
            bad += 1
        if any(bool(tr.accept(j)) != bits[i, j] for j in range(tr.size())):
            bad += 1
        n += 1
    print(f"{path}: {n} events, {bits.shape[1]} paths, menu {info.menu.name}: {bad} mismatches")
    return bad


if __name__ == "__main__":
    total = sum(check(p) for p in sys.argv[1:])
    sys.stdout.flush()
    os._exit(1 if total else 0)     # os._exit: PyROOT can hang at interpreter exit
