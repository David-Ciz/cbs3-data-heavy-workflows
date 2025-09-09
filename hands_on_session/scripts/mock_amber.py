#!/usr/bin/env python3
"""
Mock utility to:
- extend a trajectory to more frames; and/or
- scale up the system by adding more atoms (duplicate the system K times)

It works with Amber prmtop topology and writes an Amber ASCII trajectory (.mdcrd-like),
so existing tools like cpptraj and MDAnalysis can read it. Data is synthetic.

Usage examples:
- Extend frames only (keep topology):
  python mock_amber.py extend \
    --top ../data/rerun_10us-traj/strip-wat-ions.caau-ol3-case-opc_NBfix-both-0BPhs_HMR_1.top \
    --out-traj ../data/rerun_10us-traj/caau_mock_longer.traj \
    --n-frames 200000

- Scale atoms by factor 4 and write new topology + trajectory with 1000 frames:
  python mock_amber.py scale \
    --top ../data/rerun_10us-traj/strip-wat-ions.caau-ol3-case-opc_NBfix-both-0BPhs_HMR_1.top \
    --out-top ../data/rerun_10us-traj/strip-wat-ions.caau_scaled4.top \
    --out-traj ../data/rerun_10us-traj/caau_scaled4.traj \
    --factor 4 \
    --n-frames 1000

Notes:
- Coordinates are generated procedurally (mock). If you need a deterministic run, set --seed.
- Trajectory is streamed to disk; memory use is low even for many frames.
- Box info comes from the topology; MDCRD frames are written without box lengths (cpptraj uses prmtop).
"""
from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path
from typing import Iterator, Tuple, Optional, List

try:
    import parmed as pmd
except Exception:
    pmd = None  # type: ignore


def _ensure_parmed() -> None:
    if pmd is None:
        print(
            "Error: This operation requires 'parmed'. Install it (e.g., pip install parmed).",
            file=sys.stderr,
        )
        sys.exit(2)


def _parse_prmtop_natoms_and_box(path: Path) -> Tuple[int, Optional[Tuple[float, float, float]]]:
    """Very small parser to extract NATOM and optional BOX_DIMENSIONS from Amber prmtop."""
    natoms: Optional[int] = None
    box: Optional[Tuple[float, float, float]] = None
    state = None
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.startswith("%FLAG "):
                flag = line.split()[1]
                state = flag
                continue
            if state == "POINTERS":
                # The first integer is NATOM
                if line.startswith("%FORMAT"):
                    continue
                parts = line.strip().split()
                if parts:
                    try:
                        natoms = int(parts[0])
                        state = None
                    except ValueError:
                        pass
            elif state == "BOX_DIMENSIONS":
                if line.startswith("%FORMAT"):
                    continue
                parts = line.strip().split()
                if len(parts) >= 3:
                    try:
                        a, b, c = float(parts[0]), float(parts[1]), float(parts[2])
                        box = (a, b, c)
                        state = None
                    except ValueError:
                        pass
    if natoms is None:
        raise ValueError(f"Could not parse NATOM from prmtop: {path}")
    return natoms, box


def _rand_coords_stream(
    natoms: int,
    nframes: int,
    seed: Optional[int] = None,
    box_angstrom: Optional[Tuple[float, float, float]] = None,
    jitter_scale: float = 0.5,
) -> Iterator[List[Tuple[float, float, float]]]:
    """Yield nframes lists of (x, y, z) tuples with mock coordinates.

    - If box is provided, positions are uniform within box; otherwise within a 100 A cube.
    - Subsequent frames are previous + small Gaussian-like jitter to mimic motion.
    """
    rng = random.Random(seed)
    if box_angstrom is None:
        Lx = Ly = Lz = 100.0
    else:
        Lx, Ly, Lz = box_angstrom

    # Helper to draw approx Gaussian via central limit theorem (sum of uniforms)
    def gauss() -> float:
        return sum(rng.uniform(-1.0, 1.0) for _ in range(6)) / 6.0

    # Initial frame uniformly in a box
    frame: List[Tuple[float, float, float]] = [
        (rng.uniform(0.0, Lx), rng.uniform(0.0, Ly), rng.uniform(0.0, Lz)) for _ in range(natoms)
    ]
    yield frame

    for _ in range(1, nframes):
        new_frame: List[Tuple[float, float, float]] = []
        for (x, y, z) in frame:
            dx = gauss() * jitter_scale
            dy = gauss() * jitter_scale
            dz = gauss() * jitter_scale
            nx = (x + dx) % Lx
            ny = (y + dy) % Ly
            nz = (z + dz) % Lz
            new_frame.append((nx, ny, nz))
        frame = new_frame
        yield frame


def _write_mdcrd_ascii(out_path: Path, natoms: int, frames: Iterator[List[Tuple[float, float, float]]]) -> None:
    """Write Amber ASCII trajectory (.mdcrd-like): 10 floats per line, width 8, 3 decimals."""
    with out_path.open("w", encoding="ascii") as w:
        w.write(str(out_path.name) + "\n")  # title line
        for frame in frames:
            if len(frame) != natoms:
                raise ValueError(f"Frame has wrong length {len(frame)}, expected {natoms}")
            # Flatten xyz
            vals: List[float] = []
            for (x, y, z) in frame:
                vals.extend((x, y, z))
            # Write 10 values per line
            for i in range(0, len(vals), 10):
                chunk = vals[i : i + 10]
                line = "".join(f"{v:8.3f}" for v in chunk)
                w.write(line + "\n")


def cmd_extend(args: argparse.Namespace) -> None:
    top_path = Path(args.top).resolve()
    if not top_path.exists():
        raise FileNotFoundError(f"Topology not found: {top_path}")

    natoms, box = _parse_prmtop_natoms_and_box(top_path)

    out_traj = Path(args.out_traj).resolve()
    out_traj.parent.mkdir(parents=True, exist_ok=True)

    frames = _rand_coords_stream(
        natoms=natoms,
        nframes=args.n_frames,
        seed=args.seed,
        box_angstrom=box,
        jitter_scale=args.jitter,
    )
    _write_mdcrd_ascii(out_traj, natoms, frames)
    print(f"Wrote mock trajectory with {args.n_frames} frames and {natoms} atoms -> {out_traj}")


def cmd_scale(args: argparse.Namespace) -> None:
    _ensure_parmed()
    assert pmd is not None
    top_path = Path(args.top).resolve()
    if not top_path.exists():
        raise FileNotFoundError(f"Topology not found: {top_path}")
    struct = pmd.load_file(str(top_path))  # type: ignore[union-attr]
    base_natoms = len(struct.atoms)  # some ParmEd classes don’t expose .natoms

    if args.factor < 1:
        raise ValueError("--factor must be >= 1")

    # Build scaled structure by repeating
    scaled_struct = struct if args.factor == 1 else (struct * int(args.factor))  # type: ignore[operator]
    natoms = len(scaled_struct.atoms)

    out_top = Path(args.out_top).resolve()
    out_top.parent.mkdir(parents=True, exist_ok=True)
    # Write new prmtop explicitly as Amber format
    if out_top.exists():
        out_top.unlink()
    scaled_struct.save(str(out_top), format="amber")  # type: ignore[union-attr]

    # Box lengths: reuse from original topology if present
    box: Optional[Tuple[float, float, float]] = None
    if getattr(struct, "box", None) is not None:
        a, b, c = struct.box[:3]
        box = (float(a), float(b), float(c))

    out_traj = Path(args.out_traj).resolve()
    out_traj.parent.mkdir(parents=True, exist_ok=True)

    frames = _rand_coords_stream(
        natoms=natoms,
        nframes=args.n_frames,
        seed=args.seed,
        box_angstrom=box,
        jitter_scale=args.jitter,
    )
    _write_mdcrd_ascii(out_traj, natoms, frames)

    print(
        f"Scaled topology: factor={args.factor}, atoms: {base_natoms} -> {natoms}. "
        f"Wrote: top={out_top}, traj={out_traj} with {args.n_frames} frames."
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Mock Amber utilities: extend frames or scale atoms")
    sub = p.add_subparsers(dest="cmd", required=True)

    # extend
    sp = sub.add_parser("extend", help="Generate a longer trajectory with the same topology")
    sp.add_argument("--top", required=True, help="Path to input Amber prmtop topology")
    sp.add_argument("--out-traj", required=True, help="Output trajectory path (.traj)")
    sp.add_argument("--n-frames", type=int, required=True, help="Number of frames to write")
    sp.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    sp.add_argument("--jitter", type=float, default=0.5, help="Per-frame Gaussian jitter (Angstrom)")
    sp.set_defaults(func=cmd_extend)

    # scale
    sp = sub.add_parser(
        "scale",
        help="Duplicate the system K times (new prmtop) and write a mock trajectory",
    )
    sp.add_argument("--top", required=True, help="Path to input Amber prmtop topology")
    sp.add_argument("--out-top", required=True, help="Output scaled Amber prmtop path")
    sp.add_argument("--out-traj", required=True, help="Output trajectory path (.traj)")
    sp.add_argument("--factor", type=int, required=True, help="Scale factor (K >= 1)")
    sp.add_argument("--n-frames", type=int, required=True, help="Number of frames to write")
    sp.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    sp.add_argument("--jitter", type=float, default=0.5, help="Per-frame Gaussian jitter (Angstrom)")
    sp.set_defaults(func=cmd_scale)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())