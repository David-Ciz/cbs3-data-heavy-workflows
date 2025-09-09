#!/usr/bin/env bash
set -euo pipefail
# Generate mock Amber trajectories using mock_amber.py extend.
# Usage:
#   ./generate_mock_data.sh            # full datasets (may take long and create very large files)
#   SMOKE=1 ./generate_mock_data.sh    # quick tiny smoke test only
#   REGEN_TOPS=1 ./generate_mock_data.sh  # also (re)generate scaled topologies via `scale`
#   SCALE_FACTOR=159 ./generate_mock_data.sh  # override scale factor (default 159 => ~20k atoms)

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PY=python3
MOCK="$ROOT_DIR/scripts/mock_amber.py"

# Base system atoms (unscaled)
BASE_ATOMS=126

# Frame counts
if [[ "${SMOKE:-}" == "1" ]]; then
  FRAMES_BIG=10       # for big_length and big_length_many_atoms
  FRAMES_MANY=10      # for many_atoms
  echo "[SMOKE] Generating tiny mock trajectories: big_length=$FRAMES_BIG, many_atoms=$FRAMES_MANY"
else
  FRAMES_BIG=500000   # big_length and big_length_many_atoms
  FRAMES_MANY=100000  # many_atoms
fi

# Ensure script exists
if [[ ! -f "$MOCK" ]]; then
  echo "mock_amber.py not found at $MOCK" >&2
  exit 1
fi

# Scale factor to reach ~20k atoms (126 * 159 = 20034)
FACTOR=${SCALE_FACTOR:-159}
SCALED_ATOMS=$(( BASE_ATOMS * FACTOR ))
echo "[INFO] Using scale factor=$FACTOR -> scaled atoms=$SCALED_ATOMS"

# Paths
TOP_BASE="$ROOT_DIR/data/rerun_10us-traj/big_length/strip-wat-ions.caau-ol3-case-opc_NBfix-both-0BPhs_HMR_1.top"
TOP_MANY_DIR="$ROOT_DIR/data/rerun_10us-traj/many_atoms"
TOP_BLMANY_DIR="$ROOT_DIR/data/rerun_10us-traj/big_length_many_atoms"
TOP_MANY="$TOP_MANY_DIR/strip-wat-ions.caau_scaled${FACTOR}.top"
TOP_BLMANY="$TOP_BLMANY_DIR/strip-wat-ions.caau_scaled${FACTOR}.top"

# Optionally regenerate scaled topologies from base top
if [[ "${REGEN_TOPS:-}" == "1" ]]; then
  echo "[INFO] Regenerating scaled${FACTOR} topologies using base top: $TOP_BASE"
  mkdir -p "$TOP_MANY_DIR" "$TOP_BLMANY_DIR"
  $PY "$MOCK" scale \
    --top "$TOP_BASE" \
    --out-top "$TOP_MANY" \
    --out-traj "$TOP_MANY_DIR/_tmp.traj" \
    --factor "$FACTOR" \
    --n-frames 1 \
    --seed 1
  $PY "$MOCK" scale \
    --top "$TOP_BASE" \
    --out-top "$TOP_BLMANY" \
    --out-traj "$TOP_BLMANY_DIR/_tmp.traj" \
    --factor "$FACTOR" \
    --n-frames 1 \
    --seed 2
  rm -f "$TOP_MANY_DIR/_tmp.traj" "$TOP_BLMANY_DIR/_tmp.traj"
fi

# 1) big_length: keep original topology, extend to desired frames
#OUT_BIG="$ROOT_DIR/data/rerun_10us-traj/big_length/caau_longer_${FRAMES_BIG}f.traj"
#$PY "$MOCK" extend \
#  --top "$TOP_BASE" \
#  --out-traj "$OUT_BIG" \
#  --n-frames "$FRAMES_BIG" \
#  --seed 42

# 2) many_atoms: use scaled topology, extend to desired frames
OUT_MANY="$ROOT_DIR/data/rerun_10us-traj/many_atoms/caau_scaled${FACTOR}_${FRAMES_MANY}f.traj"
$PY "$MOCK" extend \
  --top "$TOP_MANY" \
  --out-traj "$OUT_MANY" \
  --n-frames "$FRAMES_MANY" \
  --seed 42

# 3) big_length_many_atoms: use scaled topology, extend to desired frames (optional)
#OUT_BLMANY="$ROOT_DIR/data/rerun_10us-traj/big_length_many_atoms/caau_scaled${FACTOR}_longer_${FRAMES_BIG}f.traj"
if [[ -n "${OUT_BLMANY:-}" ]]; then
  $PY "$MOCK" extend \
    --top "$TOP_BLMANY" \
    --out-traj "$OUT_BLMANY" \
    --n-frames "$FRAMES_BIG" \
    --seed 42
else
  echo "[SKIP] Skipping big_length_many_atoms (OUT_BLMANY not set)"
fi

echo "Done. Files written:"
printf "  - %s\n" "$OUT_BIG" "$OUT_MANY"
if [[ -n "${OUT_BLMANY:-}" ]]; then printf "  - %s\n" "$OUT_BLMANY"; fi
