import pandas as pd
from pathlib import Path

RAW_DIR     = Path("../../data/raw")
INTERIM_DIR = Path("../../data/interim")
RAW_DIR.mkdir(parents=True, exist_ok=True)



for f in sorted(RAW_DIR.glob("*.txt")):
    sensor = f.stem
    df = pd.read_csv(f, sep="\t", header=None)
    print(f"{sensor}: shape = {df.shape}")

