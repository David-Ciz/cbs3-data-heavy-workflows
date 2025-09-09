#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def render_trajin(in_path: Path, traj_path: str, out_path: Path) -> None:
    src = in_path.read_text(encoding="utf-8")
    lines = src.splitlines()
    replaced = False
    new_lines: list[str] = []
    for line in lines:
        s = line.lstrip()
        if not replaced and s.startswith("trajin "):
            # Preserve original indentation
            indent = line[: len(line) - len(s)]
            new_lines.append(f"{indent}trajin {traj_path}")
            replaced = True
        else:
            new_lines.append(line)
    if not replaced:
        # If no trajin found, prepend one
        new_lines.insert(0, f"trajin {traj_path}")
    out_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Render ptraj input with a parametrized trajin path")
    ap.add_argument("--in", dest="in_file", required=True, help="Input ptraj .in file")
    ap.add_argument("--traj", dest="traj", required=True, help="Trajectory path to set in 'trajin'")
    ap.add_argument("--out", dest="out_file", required=True, help="Output rendered .in file path")
    args = ap.parse_args()

    in_path = Path(args.in_file)
    out_path = Path(args.out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    render_trajin(in_path, args.traj, out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

