from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _run_script(script_path: Path, args: list[str]) -> subprocess.CompletedProcess[str] | None:
    if not script_path.exists():
        print(f"- skip (missing): {script_path.as_posix()}")
        return None

    cmd = [sys.executable, str(script_path), *args]
    print(f"- run: {' '.join(cmd)}")
    return subprocess.run(cmd, text=True, capture_output=True)


def _warn(msg: str, proc: subprocess.CompletedProcess[str] | None = None) -> None:
    print(f"  WARNING: {msg}")
    if proc is not None:
        if proc.stdout.strip():
            print("  --- stdout ---")
            print(proc.stdout.rstrip())
        if proc.stderr.strip():
            print("  --- stderr ---")
            print(proc.stderr.rstrip())


def _is_missing_raw_file_failure(proc: subprocess.CompletedProcess[str]) -> bool:
    if proc.returncode == 0:
        return False
    text = (proc.stdout or "") + "\n" + (proc.stderr or "")
    return "No suitable raw" in text or "Raw directory not found" in text or "FileNotFoundError" in text


def _required_processed_inputs() -> list[Path]:
    return [
        Path("data") / "processed" / "vnm_daily_market.parquet",
        Path("data") / "processed" / "daily_fx.parquet",
        Path("data") / "processed" / "daily_input_cost.parquet",
        Path("data") / "processed" / "vnm_anchor_valuation.parquet",
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local VNM MVP pipeline (builders -> valuation).")
    parser.add_argument("--as-of-date", dest="as_of_date", default=None, help="YYYY-MM-DD (optional)")
    args = parser.parse_args()

    print("Local pipeline steps:")

    # 1) Build market (best effort)
    proc = _run_script(Path("scripts") / "build_vnm_daily_market.py", [])
    if proc is not None and proc.returncode != 0:
        if _is_missing_raw_file_failure(proc):
            _warn("market builder failed due to missing raw input; continuing", proc)
        else:
            _warn("market builder failed; continuing (will fall back to sample inputs if needed)", proc)

    # 2) Build anchor (best effort)
    proc = _run_script(Path("scripts") / "build_vnm_anchor_valuation.py", [])
    if proc is not None and proc.returncode != 0:
        if _is_missing_raw_file_failure(proc):
            _warn("anchor builder failed due to missing raw input; continuing", proc)
        else:
            _warn("anchor builder failed; continuing (will fall back to sample inputs if needed)", proc)

    # 3) Check processed inputs
    required = _required_processed_inputs()
    missing = [p for p in required if not p.exists()]
    if missing:
        print("- processed inputs missing:")
        for p in missing:
            print(f"  - {p.as_posix()}")

        # 4) Bootstrap fallback
        print("- bootstrap: scripts/create_sample_inputs.py")
        bootstrap = _run_script(Path("scripts") / "create_sample_inputs.py", [])
        if bootstrap is None:
            raise FileNotFoundError("Missing bootstrap script: scripts/create_sample_inputs.py")
        if bootstrap.returncode != 0:
            _warn("bootstrap sample input generation failed", bootstrap)
            raise RuntimeError("Failed to generate sample inputs; cannot continue")
        sample_used = True
    else:
        print("- processed inputs present (no bootstrap needed)")
        sample_used = False

    # 5) Run valuation
    valuation_args: list[str] = []
    if args.as_of_date:
        valuation_args += ["--as-of-date", args.as_of_date]

    print("- valuation: scripts/run_local_valuation.py")
    val = _run_script(Path("scripts") / "run_local_valuation.py", valuation_args)
    if val is None:
        raise FileNotFoundError("Missing valuation runner: scripts/run_local_valuation.py")
    if val.stdout.strip():
        print(val.stdout.rstrip())
    if val.returncode != 0:
        if val.stderr.strip():
            print(val.stderr.rstrip())
        raise RuntimeError("Final valuation step failed")

    print(f"Done. sample_bootstrap_used={sample_used}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

