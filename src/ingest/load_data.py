# src/ingest/load_data.py
"""
Creates **three** flat DataFrames (high / mid / low sampling-rate groups)
with explicit metadata columns:
    cycle       → production cycle (1-based)
    t_in_cycle  → index within the cycle (0-based)
Each row = ONE measurement timestamp; each column (apart from metadata)
= ONE sensor.
Parquet outputs:
    data/interim/high_flat.parquet
    data/interim/mid_flat.parquet
    data/interim/low_flat.parquet
"""

from pathlib import Path
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR      = PROJECT_ROOT / "data" / "raw"
INTERIM_DIR  = PROJECT_ROOT / "data" / "interim"
RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# Sensor groups by sampling rate
# ---------------------------------------------------------------------
SENSOR_GROUPS = {
    "high": ["PS1", "PS2", "PS3", "PS4", "PS5", "PS6", "EPS1"],   # 100 Hz  → 6000 samples
    "mid":  ["FS1", "FS2"],                                       # 10 Hz  → 600 samples
    "low":  ["TS1", "TS2", "TS3", "TS4", "VS1", "CE", "CP", "SE"] # 1 Hz   → 60 samples
}

# ---------------------------------------------------------------------
# Helper: build one flat DF for a sensor list
# ---------------------------------------------------------------------
def _build_flat_df(sensors: list[str]) -> pd.DataFrame:
    arrays = {}
    samples_per_cycle = None
    n_cycles = None

    # 1. Διαβάζουμε κάθε αισθητήρα, split στο tab
    for s in sensors:
        df = pd.read_csv(RAW_DIR / f"{s}.txt", sep="\t",
                         header=None, engine="python", dtype=float)
        if samples_per_cycle is None:
            samples_per_cycle = df.shape[1]   # 6000 / 600 / 60
            n_cycles = df.shape[0]            # πάντα 2205
        arrays[s] = df.to_numpy().flatten()   # (cycles*samples,) vector

    # 2. Μετα-στήλες cycle και t_in_cycle
    cycle       = np.repeat(np.arange(1, n_cycles + 1), samples_per_cycle)
    t_in_cycle  = np.tile  (np.arange(samples_per_cycle),  n_cycles)
    arrays["cycle"]      = cycle
    arrays["t_in_cycle"] = t_in_cycle

    # 3. DataFrame και σωστή σειρά στηλών
    df_flat = pd.DataFrame(arrays)
    return df_flat[["cycle", "t_in_cycle"] + sensors]

# ---------------------------------------------------------------------
# Main entry – φτιάχνει & (προαιρετικά) αποθηκεύει τα 3 DF
# ---------------------------------------------------------------------
def main(save: bool = True):
    group_dfs: dict[str, pd.DataFrame] = {}
    for group, sensor_list in SENSOR_GROUPS.items():
        print(f"[ingest] Building flat DF for group '{group}' …")
        df_flat = _build_flat_df(sensor_list)
        print(f"          → shape {df_flat.shape}")
        if save:
            out = INTERIM_DIR / f"{group}_flat.parquet"
            df_flat.to_parquet(out, index=False)
            print(f"[saved]   {out.relative_to(PROJECT_ROOT)}")
        group_dfs[group] = df_flat
    return group_dfs

if __name__ == "__main__":
    main()

