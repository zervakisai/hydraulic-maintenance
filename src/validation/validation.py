"""
CLI quick-scan / validation for the *_flat.parquet files produced by load_data.py

Usage:
    python -m src.validation.validate_data  --group high  --profile
    python -m src.validation.validate_data  --all

Outputs:
  • Console: shape, dtypes, NaN counts, min/max per column
  • Optional profiling HTML under reports/<group>_profile.html
"""

import argparse, sys, textwrap
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INTERIM_DIR  = PROJECT_ROOT / "data" / "interim"
REPORT_DIR   = PROJECT_ROOT / "reports"
REPORT_DIR.mkdir(exist_ok=True)

def quick_scan(df: pd.DataFrame, group: str):
    line = "-" * 60
    print(f"{line}\n🗂  {group.upper()}  shape: {df.shape}\n{line}")
    print("dtypes\n", df.dtypes.value_counts(), "\n")
    print("NaNs (top-10)\n", df.isna().sum().sort_values(ascending=False).head(10), "\n")
    print("min / max examples\n", df.describe().loc[["min", "max"]].T.head(10), "\n")

def profile_report(df: pd.DataFrame, group: str):
    try:
        from ydata_profiling import ProfileReport
    except ImportError:
        print("⚠️  Install 'ydata-profiling' for HTML report → pip install ydata-profiling")
        return
    sample = df.sample(min(20_000, len(df)), random_state=0)
    ProfileReport(sample, title=f"{group} quick profile").to_file(
        REPORT_DIR / f"{group}_profile.html"
    )
    print(f"📄  Saved profile ⇒ reports/{group}_profile.html")

def load_group(group: str) -> pd.DataFrame:
    path = INTERIM_DIR / f"{group}_flat.parquet"
    if not path.exists():
        sys.exit(f"❌ Parquet not found: {path}")
    return pd.read_parquet(path)

def main():
    parser = argparse.ArgumentParser(
        description="CLI validation for hydraulic dataset",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--group", choices=["high","mid","low"],
                        help="Validate single group")
    parser.add_argument("--all", action="store_true",
                        help="Validate all groups")
    parser.add_argument("--profile", action="store_true",
                        help="Generate HTML profiling report")
    args = parser.parse_args()

    groups = ["high","mid","low"] if args.all or args.group is None else [args.group]

    for g in groups:
        df = load_group(g)
        quick_scan(df, g)
        if args.profile:
            profile_report(df, g)

if __name__ == "__main__":
    main()

