"""Where results come from: git state of the code and of the sample catalog."""

from __future__ import annotations

import os
import subprocess


def git_state(path):
    """``{"commit", "dirty", "branch"}`` of the git checkout containing ``path`` (``None`` if none).

    ``dirty`` refers to changes below ``path`` only (e.g. only the catalog directory).
    """
    path = os.path.abspath(path)
    cwd = path if os.path.isdir(path) else os.path.dirname(path)

    def git(*args):
        return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, timeout=30)

    try:
        commit = git("rev-parse", "HEAD")
        if commit.returncode != 0:
            return None
        status = git("status", "--porcelain", "--", path)
        branch = git("rev-parse", "--abbrev-ref", "HEAD")
    except (OSError, subprocess.SubprocessError):
        return None
    return {"commit": commit.stdout.strip(), "dirty": bool(status.stdout.strip()),
            "branch": branch.stdout.strip() or None}
