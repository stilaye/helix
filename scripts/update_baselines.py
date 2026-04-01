#!/usr/bin/env python3
"""
update_baselines.py — CLI to re-run performance suite and commit new baselines.

Usage:
    python scripts/update_baselines.py --cluster-ip=10.0.0.100
    python scripts/update_baselines.py --cluster-ip=10.0.0.100 --dry-run

This script:
  1. Runs the full performance suite with --update-baselines
  2. Shows a diff of what changed in baselines/
  3. Prompts for confirmation
  4. Commits the updated baselines to git

Use after:
  - Hardware upgrades (new baselines will be higher — expected improvement)
  - Tuning changes (document WHY in commit message)
  - New test additions (no previous baseline exists)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run_perf_suite(cluster_ip: str, dry_run: bool = False) -> int:
    """Run performance suite with baseline update mode."""
    cmd = [
        "pytest",
        "-m", "perf",
        f"--cluster-ip={cluster_ip}",
        "--update-baselines",
        "--tb=short",
        "-ra",
        "-v",
        "tests/performance/",
    ]

    print(f"Running: {' '.join(cmd)}")
    if dry_run:
        print("[DRY RUN] Would run the above command")
        return 0

    result = subprocess.run(cmd)
    return result.returncode


def show_baseline_diff() -> str:
    """Show git diff for the baselines/ directory."""
    result = subprocess.run(
        ["git", "diff", "--stat", "baselines/"],
        capture_output=True, text=True
    )
    return result.stdout.strip()


def commit_baselines(message: str, dry_run: bool = False) -> None:
    """Stage and commit the baselines/ directory."""
    if dry_run:
        print(f"[DRY RUN] Would commit baselines/ with message: {message}")
        return

    subprocess.run(["git", "add", "baselines/"], check=True)
    subprocess.run(["git", "commit", "-m", message], check=True)
    print("Baselines committed.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Re-run performance suite and update committed baselines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--cluster-ip", required=True, help="Cluster management IP")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen, don't execute")
    parser.add_argument(
        "--commit-message", default="",
        help="Commit message (prompted if not provided)"
    )
    args = parser.parse_args()

    print("=" * 50)
    print("HELIX Baseline Update")
    print(f"Cluster: {args.cluster_ip}")
    print("=" * 50)

    # Run performance suite
    exit_code = run_perf_suite(args.cluster_ip, dry_run=args.dry_run)
    if exit_code != 0:
        print(f"\nPerformance suite had failures (exit {exit_code}).")
        print("Review failures before committing baselines.")
        answer = input("Commit baselines anyway? [y/N]: ").strip().lower()
        if answer != "y":
            return 1

    # Show diff
    diff = show_baseline_diff()
    if not diff:
        print("\nNo baseline changes detected.")
        return 0

    print("\nBaseline changes:")
    print(diff)

    # Confirm and commit
    if not args.dry_run:
        answer = input("\nCommit these baseline updates? [y/N]: ").strip().lower()
        if answer != "y":
            print("Aborted. Baselines NOT committed.")
            return 0

        commit_msg = args.commit_message or input(
            "Commit message (e.g., 'perf: update baselines after SSD upgrade'): "
        ).strip()
        if not commit_msg:
            commit_msg = f"perf: update performance baselines ({args.cluster_ip})"

        commit_baselines(commit_msg, dry_run=args.dry_run)

    return 0


if __name__ == "__main__":
    sys.exit(main())
