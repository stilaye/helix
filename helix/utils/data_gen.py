"""
Test data generation utilities.
Creates files and directory trees with known sizes and checksums for use in backup/restore tests.
"""

from __future__ import annotations

import hashlib
import os
import random
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TestFile:
    path: Path
    size_bytes: int
    checksum: str
    relative_path: str = ""


@dataclass
class TestDataSet:
    """A collection of test files with known checksums."""
    root: Path
    files: list[TestFile] = field(default_factory=list)

    @property
    def total_bytes(self) -> int:
        return sum(f.size_bytes for f in self.files)

    @property
    def total_size_bytes(self) -> int:
        return self.total_bytes

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def tree_checksum(self) -> str:
        from helix.utils.checksum import compute_tree_checksum
        return compute_tree_checksum(self.root)


class DataGenerator:
    """
    Generates test datasets with predictable content for checksum verification.

    Usage:
        dataset = DataGenerator.create_standard_set(tmp_path)
        # ... run backup ...
        restored_checksum = compute_tree_checksum(restore_path)
        assert restored_checksum == dataset.tree_checksum
    """

    @staticmethod
    def create_standard_set(root: Path) -> TestDataSet:
        """
        Create a standard test dataset:
        - Small files (1KB, 4KB, 64KB) for metadata stress
        - Medium files (1MB, 10MB) for throughput tests
        - Nested directory structure (depth 3, width 3)
        """
        dataset = TestDataSet(root=root)
        root.mkdir(parents=True, exist_ok=True)

        # Flat files at root
        specs = [
            ("small_1k.dat", 1 * 1024),
            ("small_4k.dat", 4 * 1024),
            ("medium_64k.dat", 64 * 1024),
            ("medium_1m.dat", 1 * 1024 * 1024),
            ("large_10m.dat", 10 * 1024 * 1024),
        ]
        for name, size in specs:
            tf = DataGenerator._write_file(root / name, size, root)
            dataset.files.append(tf)

        # Nested directory structure
        for d in range(3):
            subdir = root / f"dir_{d:02d}"
            for s in range(3):
                sub_subdir = subdir / f"sub_{s:02d}"
                sub_subdir.mkdir(parents=True, exist_ok=True)
                tf = DataGenerator._write_file(sub_subdir / "data.dat", 4096, root)
                dataset.files.append(tf)

        return dataset

    @staticmethod
    def _write_file(path: Path, size_bytes: int, root: Path | None = None) -> TestFile:
        """Write a file with deterministic content (seed based on filename)."""
        seed = int(hashlib.md5(path.name.encode()).hexdigest(), 16) % (2**32)
        rng = random.Random(seed)
        content = bytes(rng.getrandbits(8) for _ in range(size_bytes))
        path.write_bytes(content)
        checksum = hashlib.sha256(content).hexdigest()
        relative = str(path.relative_to(root)) if root else path.name
        return TestFile(path=path, size_bytes=size_bytes, checksum=checksum, relative_path=relative)

    @staticmethod
    def create_large_file(path: Path, size_gb: float) -> TestFile:
        """Create a large file for throughput testing (written in 1MB chunks)."""
        size_bytes = int(size_gb * 1024 * 1024 * 1024)
        path.parent.mkdir(parents=True, exist_ok=True)
        h = hashlib.sha256()
        chunk = os.urandom(1024 * 1024)  # 1MB random chunk (not seed-based, it's large)
        with open(path, "wb") as f:
            written = 0
            while written < size_bytes:
                write_size = min(len(chunk), size_bytes - written)
                f.write(chunk[:write_size])
                h.update(chunk[:write_size])
                written += write_size
        return TestFile(path=path, size_bytes=size_bytes, checksum=h.hexdigest())
