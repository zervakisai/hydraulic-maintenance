# src/validation/validate_data.py
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
import numpy as np
import pandas as pd

try:
    from ydata_profiling import ProfileReport
    HAS_PROFILING = True
except Exception:
    HAS_PROFILING = False

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INTERIM_DIR  = PROJECT_ROOT / "data" / "interim"
REPORTS_DIR  = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Όρια/κανόνες "ήπιας" λογικής (ασφαλή για αρχή)
NONNEGATIVE_SENSORS = set(["PS1","PS2","PS3","PS4","PS5","PS6","EPS1","FS1","FS2","VS1","CE","CP","SE"])
# soft outlier rules: IQR-based
IQR_MULT = 3.0

def load_group_df(group: str) -> pd.DataFrame:
    path = INTERIM_DIR / f"{group}_flat.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    df = pd.read_parquet(path)
    # Βεβαιώσου ότι υπάρχουν meta-cols
    required = {"cycle","t_in_cycle"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[{group}] Missing required columns: {missing}")
    return df

def get_sensor_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in ("cycle","t_in_cycle")]

def basic_stats(df: pd.DataFrame, group: str) -> dict:
    sensors = get_sensor_cols(df)
    n_rows, n_cols = df.shape
    dtypes_ok = all(np.issubdtype(df[c].dtype, np.number) for c in sensors)
    nans_per_col = df[sensors].isna().sum().to_dict()
    total_nans   = int(np.sum(list(nans_per_col.values())))
    nan_ratio    = float(total_nans) / (len(sensors) * n_rows)

    # IQR-based outlier counts per sensor (soft)
    outlier_counts = {}
    for s in sensors:
        x = df[s].dropna().to_numpy()
        if x.size == 0:
            outlier_counts[s] = 0
            continue
        q1, q3 = np.percentile(x, [25, 75])
        iqr = q3 - q1
        lo = q1 - IQR_MULT * iqr
        hi = q3 + IQR_MULT * iqr
        outlier_counts[s] = int(((x < lo) | (x > hi)).sum())

    # Non-negative rule for specific sensors
    neg_violations = {}
    for s in sensors:
        base = s.split("_")[0]  # in case of future naming
        if base in NONNEGATIVE_SENSORS:
            neg_violations[s] = int((df[s] < 0).sum())
        else:
            neg_violations[s] = 0

    return {
        "group": group,
        "shape": [n_rows, n_cols],
        "n_sensors": len(sensors),
        "dtypes_numeric": dtypes_ok,
        "nan_total": total_nans,
        "nan_ratio": nan_ratio,
        "nan_by_col_top10": dict(sorted(nans_per_col.items(), key=lambda kv: kv[1], reverse=True)[:10]),
        "neg_violations_total": int(sum(neg_violations.values())),
        "neg_violations_by_col_top10": dict(sorted(neg_violations.items(), key=lambda kv: kv[1], reverse=True)[:10]),
        "outliers_iqr_total": int(sum(outlier_counts.values())),
        "outliers_iqr_by_col_top10": dict(sorted(outlier_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]),
    }

def profile_html(df: pd.DataFrame, group: str, sample: int|None) -> Path|None:
    if not HAS_PROFILING:
        print(f"[{group}] ydata-profiling not installed; skipping HTML profile.")
        return None
    dfp = df
    if sample and df.shape[0] > sample:
        dfp = df.sample(sample, random_state=42)
    out = REPORTS_DIR / f"{group}_profile.html"
    print(f"[{group}] Writing HTML profile → {out}")
    ProfileReport(dfp, title=f"{group} profiling", minimal=True).to_file(out)
    return out

def evaluate_thresholds(stats: dict, max_nan_ratio: float, max_neg_violations: int) -> list[str]:
    issues = []
    if not stats["dtypes_numeric"]:
        issues.append("Non-numeric dtypes detected in sensor columns.")
    if stats["nan_ratio"] > max_nan_ratio:
        issues.append(f"NaN ratio {stats['nan_ratio']:.4f} exceeds limit {max_nan_ratio:.4f}.")
    if stats["neg_violations_total"] > max_neg_violations:
        issues.append(f"Negative-value violations {stats['neg_violations_total']} exceed limit {max_neg_violations}.")
    return issues

def main():
    ap = argparse.ArgumentParser(description="Validate interim *_flat.parquet (high/mid/low).")
    ap.add_argument("--groups", nargs="+", default=["high","mid","low"], help="Which groups to validate.")
    ap.add_argument("--profile", action="store_true", help="Also write HTML profiling report.")
    ap.add_argument("--profile-sample", type=int, default=50000, help="Rows to sample for HTML profile.")
    ap.add_argument("--max-nan-ratio", type=float, default=0.001, help="Max allowed overall NaN ratio.")
    ap.add_argument("--max-neg-violations", type=int, default=0, help="Max allowed negative-value violations.")
    ap.add_argument("--report-prefix", default="validation", help="JSON report file prefix.")
    args = ap.parse_args()

    overall_fail = False
    all_reports = {}

    for g in args.groups:
        print(f"\n▶ Validating group: {g}")
        df = load_group_df(g)
        stats = basic_stats(df, g)
        issues = evaluate_thresholds(stats, args.max_nan_ratio, args.max_neg_violations)

        # Write JSON report
        out_json = REPORTS_DIR / f"{args.report_prefix}_{g}.json"
        with open(out_json, "w") as f:
            json.dump({"stats": stats, "issues": issues}, f, indent=2)
        print(f"[{g}] JSON report → {out_json}")

        if args.profile:
            profile_html(df, g, args.profile_sample)

        if issues:
            overall_fail = True
            print(f"[{g}] ❌ Issues: {issues}")
        else:
            print(f"[{g}] ✅ OK (no threshold violations)")

        all_reports[g] = {"stats": stats, "issues": issues}

    # συνολική περίληψη στην κονσόλα
    print("\n=== SUMMARY ===")
    for g, rep in all_reports.items():
        s = rep["stats"]
        print(f"{g}: shape={tuple(s['shape'])} | nan_ratio={s['nan_ratio']:.6f} | neg_violations={s['neg_violations_total']}")

    sys.exit(1 if overall_fail else 0)

if __name__ == "__main__":
    main()

