"""Helpers to decode ROOT-serialized CMSSW objects from their raw bytes.

Why this exists
---------------
uproot cannot deserialize some CMSSW classes stored in EDM files (``edm::TriggerResults``,
``edm::EventAuxiliary``, the ``ParameterSets`` tree, ...): they use features such as "memberwise"
streaming that uproot does not implement. The objects themselves have a very simple, fixed layout,
so we read each entry of such a branch as a plain array of bytes and pick the fields out ourselves.

How ROOT writes an object (the parts relevant here; all numbers are big-endian):
  * Most objects and base classes start with a 6-byte header:
        4 bytes  byte count | 0x40000000   (so the first byte is 0x40)
        2 bytes  class version
  * A ``std::string`` is written as one length byte followed by the characters; if the string is
    longer than 254 characters the length byte is 255 and the real length follows as 4 bytes.
  * Plain numbers (unsigned short/int/long long) are written as 2/4/8 big-endian bytes.

Everything in this module is vectorized with numpy over all entries of a branch, so decoding a whole
file costs a fraction of a second.
"""

from __future__ import annotations

import numpy as np
import uproot


class LayoutError(RuntimeError):
    """The bytes do not have the layout this decoder was written for.

    Raised instead of returning garbage. Typical causes: a new CMSSW release changed a class version,
    or a file was written in an unexpected way. The FWLite-based validation scripts can then be used
    to understand the new layout.
    """


def read_raw_entries(branch, entry_start=None, entry_stop=None):
    """Read the entries of ``branch`` as raw bytes.

    Returns ``(data, starts, sizes)``:
      * ``data``  : one flat uint8 array with the bytes of all entries,
      * ``starts``: int64 array, position of the first byte of each entry in ``data``,
      * ``sizes`` : int64 array, number of bytes of each entry.
    """
    arr = branch.array(interpretation=uproot.AsJagged(uproot.AsDtype("u1")),
                       entry_start=entry_start, entry_stop=entry_stop, library="ak")
    offsets = np.asarray(arr.layout.offsets.data, dtype=np.int64)
    data = np.asarray(arr.layout.content.data, dtype=np.uint8)
    return data, offsets[:-1], np.diff(offsets)


def u2(data, pos):
    """Big-endian unsigned 16-bit integers at byte positions ``pos`` (scalar or array)."""
    pos = np.asarray(pos)
    return (data[pos].astype(np.uint32) << 8) | data[pos + 1]


def u4(data, pos):
    """Big-endian unsigned 32-bit integers at byte positions ``pos``."""
    pos = np.asarray(pos)
    b = [data[pos + k].astype(np.uint64) for k in range(4)]
    # shift amounts must be uint64 too: numpy refuses to mix uint64 with Python ints in shifts
    return (b[0] << np.uint64(24)) | (b[1] << np.uint64(16)) | (b[2] << np.uint64(8)) | b[3]


def u8(data, pos):
    """Big-endian unsigned 64-bit integers at byte positions ``pos``."""
    pos = np.asarray(pos)
    return (u4(data, pos) << np.uint64(32)) | u4(data, pos + 4)


def check_header(data, pos, version, what):
    """Check that a 6-byte object header (byte-count flag + class ``version``) sits at every ``pos``."""
    ok = ((data[pos] & 0x40) != 0) & (u2(data, np.asarray(pos) + 4) == version)
    if not np.all(ok):
        n_bad = int(np.size(ok) - np.count_nonzero(ok))
        raise LayoutError(f"{what}: {n_bad} entries do not have the expected header (class version {version})")


def check_class_versions(file, expected):
    """Compare the class versions recorded in the file's streamer info with ``expected``.

    ``expected`` maps a C++ class name (e.g. ``"edm::TriggerResults"``) to the class version the
    decoder was written for. A mismatch raises :class:`LayoutError`.
    """
    for cls, version in expected.items():
        streamer = file.file.streamer_named(cls)
        found = None if streamer is None else streamer.member("fClassVersion")
        if found != version:
            raise LayoutError(f"{cls}: decoder written for class version {version}, file has {found}")


def read_string(data, pos):
    """Decode one ROOT ``std::string`` starting at byte ``pos``. Returns ``(text, next_pos)``."""
    n = int(data[pos])
    pos += 1
    if n == 255:
        n = int(u4(data, pos))
        pos += 4
    return bytes(data[pos:pos + n]).decode("latin-1"), pos + n
