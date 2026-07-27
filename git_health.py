"""
git_health.py
=============
Self-heal for the ref corruption that happens when an interrupted git push
(reboot / crash / power loss mid-write) leaves .git/refs/heads/main as blank
or null bytes instead of a 40-char commit hash. When that happens ALL git
operations fail until the ref is repaired by hand.

`repair_if_broken()` runs before any scheduled git work: it detects a
corrupt loose ref and restores the last good commit hash from the reflog
(.git/logs/HEAD), which survives ref corruption. Safe to call every cycle —
a no-op when refs are healthy.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

log = logging.getLogger(__name__)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _reflog_last_sha(git_dir: Path) -> str | None:
    """The most recent commit HEAD pointed at, from .git/logs/HEAD."""
    reflog = git_dir / "logs" / "HEAD"
    if not reflog.exists():
        return None
    try:
        lines = [ln for ln in reflog.read_text(errors="replace").splitlines() if ln.strip()]
    except OSError:
        return None
    if not lines:
        return None
    parts = lines[-1].split()          # "<old_sha> <new_sha> <author> ... <msg>"
    if len(parts) >= 2 and _SHA_RE.match(parts[1]):
        return parts[1]
    return None


def _ref_is_broken(ref_path: Path) -> bool:
    if not ref_path.exists():
        return False                    # missing loose ref falls back to packed-refs — not broken
    try:
        content = ref_path.read_bytes()
    except OSError:
        return True
    text = content.replace(b"\x00", b"").decode("ascii", errors="ignore").strip()
    if text.startswith("ref:"):         # symbolic ref — valid
        return False
    return not _SHA_RE.match(text)


def repair_if_broken(repo: Path | str, branch: str = "main") -> bool:
    """
    Repair a corrupt loose ref for `branch` (and its origin/ mirror) from the
    reflog. Returns True if a repair was performed. No-op when healthy.
    """
    repo = Path(repo)
    git_dir = repo / ".git"
    if not git_dir.is_dir():
        return False

    good_sha = _reflog_last_sha(git_dir)
    repaired = False

    for ref_rel in (f"refs/heads/{branch}", f"refs/remotes/origin/{branch}"):
        ref_path = git_dir / ref_rel
        if _ref_is_broken(ref_path):
            if not good_sha:
                log.error("git ref %s is corrupt but no reflog SHA to recover from — "
                          "manual repair needed", ref_rel)
                continue
            try:
                ref_path.write_text(good_sha + "\n")
                log.warning("Repaired corrupt git ref %s -> %s (from reflog)",
                            ref_rel, good_sha[:8])
                repaired = True
            except OSError as exc:
                log.error("Failed to repair git ref %s: %s", ref_rel, exc)

    return repaired
