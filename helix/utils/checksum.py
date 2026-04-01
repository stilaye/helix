"""Data integrity utilities: checksums for files and directory trees."""

from __future__ import annotations

import hashlib
from pathlib import Path


def compute_file_checksum(path: Path, algorithm: str = "sha256") -> str:
    """
    Compute checksum of a single file.

    Args:
        path: Path to the file.
        algorithm: Hash algorithm — 'sha256', 'md5', or 'xxhash' (fastest for large files).

    Returns:
        Hex digest string.
    """
    if algorithm == "xxhash":
        try:
            import xxhash
            h = xxhash.xxh64()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)
            return h.hexdigest()
        except ImportError:
            algorithm = "sha256"  # graceful fallback

    h = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_tree_checksum(directory: Path, algorithm: str = "sha256") -> str:
    """
    Compute a deterministic checksum for an entire directory tree.
    Walks files in sorted order so the result is stable.

    Args:
        directory: Root directory to hash.
        algorithm: Hash algorithm.

    Returns:
        Hex digest of the combined tree.
    """
    h = hashlib.new(algorithm)
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            # Include relative path so renames are detected
            h.update(str(path.relative_to(directory)).encode())
            h.update(compute_file_checksum(path, algorithm).encode())
    return h.hexdigest()
