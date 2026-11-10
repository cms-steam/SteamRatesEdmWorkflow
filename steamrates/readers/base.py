"""Common interface of the ntuple readers.

A reader turns one input file into a sequence of :class:`EventChunk` objects. A chunk holds, for a
block of consecutive events, everything the counting needs:

    run, lumi, event   one entry per event
    bits               bool matrix (events x bits): did trigger j fire in event i?
    bit_names          names of the columns of ``bits`` (same for all chunks of a file)
    weights            per-event weights (MC generator weights) or ``None`` (= all 1)

Readers for different formats (EDM ``TriggerResults`` ntuples, NanoAOD, future formats) only differ
in how they fill these arrays. Everything downstream (lumi selection, combination tables) is
format-independent.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np


@dataclass
class EventChunk:
    run: np.ndarray
    lumi: np.ndarray
    event: np.ndarray
    bits: np.ndarray
    bit_names: list
    weights: np.ndarray | None = None

    def __len__(self):
        return len(self.run)

    def select(self, mask):
        """A new chunk with only the events where ``mask`` is True."""
        return EventChunk(self.run[mask], self.lumi[mask], self.event[mask], self.bits[mask],
                          self.bit_names, None if self.weights is None else self.weights[mask])


@dataclass
class FileInfo:
    """Per-file information that does not depend on the event loop."""

    path: str
    format: str
    n_events: int
    menu: object = None                                  # steamrates.menu.MenuInfo or None
    menu_id: str | None = None                           # EDM: psetid of the trigger-name list
    lumi_blocks: np.ndarray | None = None                # (n, 2) array of (run, lumi) in the file
    mc_sums: dict = field(default_factory=dict)          # e.g. genEventSumw from the Runs tree


class Reader:
    """Base class. Subclasses implement :meth:`open` and :meth:`chunks`."""

    format = "?"

    def __init__(self, chunk_size=200_000):
        self.chunk_size = int(chunk_size)

    def open(self, path):
        """Open ``path`` and return a :class:`FileInfo`."""
        raise NotImplementedError

    def chunks(self, max_events=None):
        """Yield :class:`EventChunk` objects for the file opened last."""
        raise NotImplementedError

    def _ranges(self, n_events, max_events):
        n = n_events if max_events is None else min(n_events, max_events)
        for start in range(0, n, self.chunk_size):
            yield start, min(start + self.chunk_size, n)
