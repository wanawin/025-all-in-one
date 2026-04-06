#!/usr/bin/env python3
from __future__ import annotations

"""
BUILD: core025_final_dual_lab_daily_split__2026-04-06_v12_exact_independent_baselines | 2026-04-06 UTC

Purpose
-------
Single container app that keeps the original Winner LAB and Skip LAB operationally
independent so they can reproduce their own baselines separately.

Design
------
- Winner LAB section calls the original v14 winner app `main()` directly.
- Skip LAB section calls the original skip app `main()` directly.
- Daily section is a lightweight convenience wrapper only.

Critical rules preserved
------------------------
- Winner baseline comparisons are winner-only and must not be affected by skip.
- Skip baseline comparisons are skip-only and must not be affected by winner.
- No parity diagnostics are shown by default.
- No rolling-window logic is applied to Winner LAB or Skip LAB baseline sections.
- Optional rolling daily window exists only in the Daily convenience section.
"""

import io
import importlib.util
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import streamlit as st

BUILD_MARKER = "BUILD: core025_final_dual_lab_daily_split__2026-04-06_v12_exact_independent_baselines | 2026-04-06 UTC"
WINNER_SOURCE_FILENAME = "core_025_member_specific_top_1_calibration_engine_2026_04_03_v_1 (8).py"
SKIP_SOURCE_FILENAME = "core025_skip_ladder_app_v1__2026-03-26.py"


# -----------------------------------------------------------------------------
# Source loading
# -----------------------------------------------------------------------------

def _resolve_source_path(filename: str) -> Path:
    search_roots = [Path.cwd(), Path(__file__).resolve().parent, Path("/mnt/data")]
    for root in search_roots:
        candidate = root / filename
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Required source file not found: {filename}. Put it beside this app file."
    )


def _load_module_from_path(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@st.cache_resource(show_spinner=False)
def load_source_modules():
    winner_path = _resolve_source_path(WINNER_SOURCE_FILENAME)
    skip_path = _resolve_source_path(SKIP_SOURCE_FILENAME)
    winner_mod = _load_module_from_path("core025_winner_v14_source", winner_path)
    skip_mod = _load_module_from_path("core025_skip_source", skip_path)
    return winner_mod, skip_mod, winner_path, skip_path


# -----------------------------------------------------------------------------
# Daily convenience helpers
# -----------------------------------------------------------------------------

def read_uploaded_table(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    data = uploaded_file.getvalue()
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(data))
    if name.endswith(".txt") or name.endswith(".tsv"):
        try:
            return pd.read_csv(io.BytesIO(data), sep="\t", header=None)
        except Exception:
            return pd.read_csv(io.BytesIO(data), sep=None, engine="python", header=None)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(io.BytesIO(data))
    raise ValueError(f"Unsupported uploaded file type: {uploaded_file.name}")


def winner_default_params() -> Dict[str, float]:
    # Exact defaults copied from the original v14 sidebar controls.
    return {
        "run_mode": "Regular Run",
        "min_stream_history": 20.0,
        "per_rule_cap": 2.50,
        "total_boost_cap": 10.00,
        "diminishing_return_factor": 0.35,
        "rule_count_norm_factor": 1.50,
        "max_rules_per_member": 5.0,
        "compression_alpha": 0.45,
        "exclusivity_rule_bonus": 0.08,
        "exclusivity_boost_bonus": 0.20,
        "exclusivity_cap": 0.35,
        "min_compression_factor": 0.30,
        "dominant_gap_strict": 0.65,
        "dominant_ratio_max_strict": 0.65,
        "dominant_exclusivity_min": 0.24,
        "dominant_rule_gap_min": 3.00,
        "dominant_alignment_min": 0.58,
        "contested_gap_max": 0.12,
        "contested_ratio_min": 0.97,
        "top2_ratio_trigger": 0.97,
        "top2_gap_trigger": 0.08,
        "top2_alignment_ceiling": 0.62,
        "top2_exclusivity_ceiling": 0.22,
        "m0025_boost_gap_min": 0.60,
        "m0025_alignment_min": 0.60,
        "m0025_top2_score_max": 1.75,
        "m0225_boost_gap_min": 0.45,
        "m0225_alignment_min": 0.58,
        "m0225_ratio_max": 0.88,
        "m0255_boost_gap_min": 0.40,
        "m0255_alignment_min": 0.55,
        "m0255_gap_min": 0.18,
        "m0025_penalty_top2_score_min": 1.70,
        "m0025_penalty_alignment_max": 0.58,
        "m0025_penalty_multiplier_top2": 0.88,
        "m0025_penalty_multiplier_align": 0.90,
        "m0225_boost_alignment_min": 0.60,
        "m0225_boost_multiplier": 1.05,
        "m0255_boost_multiplier_gap": 1.30,
        "m0255_boost_multiplier_align": 1.24,
        "weak_top1_score_floor": 0.20,
        "rows_to_show": 50,
        "lab_max_events": 0,
    }


def skip_default_params() -> Dict[str, float]:
    return {
        "min_trait_support": 12,
        "top_negative_traits_to_use": 15,
        "rung_count": 50,
        "target_retention_pct": 0.75,
        "rows_to_show": 25,
    }


def maybe_apply_daily_window(df: pd.DataFrame, enable: bool, locked_count: int) -> pd.DataFrame:
    if not enable:
        return df.copy()
    if locked_count <= 0:
        return df.copy()
    return df.tail(int(locked_count)).copy().reset_index(drop=True)


def render_download(df: pd.DataFrame, label: str, filename: str) -> None:
    if df is None:
        df = pd.DataFrame()
    st.download_button(
        label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )


def run_daily_section(winner_mod, skip_mod) -> None:
    st.subheader("Daily KEEP / STRIP")
    st.caption("Convenience mode only. Winner and Skip baselines are validated separately in their own sections.")

    history_file = st.file_uploader(
        "Upload FULL history file",
        type=["txt", "tsv", "csv", "xlsx", "xls"],
        key="daily_history_file",
    )
    library_file = st.file_uploader(
        "Upload promoted separator library CSV",
        type=["csv", "txt", "tsv", "xlsx", "xls"],
        key="daily_library_file",
    )
    last24_file = st.file_uploader(
        "Optional last 24 file (same raw-history format)",
        type=["txt", "tsv", "csv", "xlsx", "xls"],
        key="daily_last24_file",
    )

    if history_file is None or library_file is None:
        st.info("Upload the full history file and promoted separator library CSV to run Daily.")
        return

    try:
        main_raw_df = read_uploaded_table(history_file)
        sep_lib_df = read_uploaded_table(library_file)
        last24_raw_df = read_uploaded_table(last24_file) if last24_file is not None else None
    except Exception as e:
        st.exception(e)
        return

    st.write(f"Raw history rows uploaded: {len(main_raw_df):,}")

    use_daily_window = st.checkbox(
        "Use locked rolling raw-history window for Daily only",
        value=False,
        key="daily_use_window",
    )
    locked_window_count = st.number_input(
        "Locked raw-history row count for Daily",
        min_value=1,
        value=int(len(main_raw_df)),
        step=1,
        key="daily_locked_window_count",
    )
    rows_to_show = st.number_input(
        "Daily rows to display",
        min_value=5,
        value=50,
        step=5,
        key="daily_rows_to_show",
    )

    if not st.button("Run Daily KEEP / STRIP", type="primary", key="daily_run_btn"):
        return

    try:
        # Apply optional Daily-only rolling window at the raw input boundary.
        daily_main_raw_df = maybe_apply_daily_window(main_raw_df, use_daily_window, int(locked_window_count))

        # Winner-side exact source functions
        winner_hist = winner_mod.prep_history(daily_main_raw_df.copy())
        separator_rules = winner_mod.load_separator_library(sep_lib_df.copy())
        wparams = winner_default_params()

        # Skip-side exact source functions
        skip_main_history = skip_mod.prepare_history(daily_main_raw_df.copy())
        skip_last24_history = skip_mod.prepare_history(last24_raw_df.copy()) if last24_raw_df is not None else None
        current_df = skip_mod.current_seed_rows(skip_main_history, skip_last24_history)

        # Build current survivors from current stream seeds.
        survivors_df = current_df.rename(columns={"stream_id": "stream"})[["stream", "seed"]].copy()
        survivors_prepped = winner_mod.prep_survivors(survivors_df)
        playlist_df, winner_summary_df = winner_mod.run_regular_playlist(
            winner_hist,
            survivors_prepped,
            separator_rules,
            wparams,
        )

        # Skip scoring pipeline using exact defaults.
        sparams = skip_default_params()
        transitions_df = skip_mod.build_transition_events(skip_main_history)
        feat_df = skip_mod.build_feature_table(transitions_df)
        negative_traits_df = skip_mod.mine_negative_traits(
            feat_df,
            min_support=int(sparams["min_trait_support"]),
        )
        scored_df = skip_mod.build_skip_score_table(
            feat_df=feat_df,
            negative_traits_df=negative_traits_df,
            top_negative_traits_to_use=int(sparams["top_negative_traits_to_use"]),
        )
        ladder_df = skip_mod.build_retention_ladder(scored_df, rung_count=int(sparams["rung_count"]))
        recommended_df = skip_mod.recommend_cutoff(
            ladder_df,
            target_retention_pct=float(sparams["target_retention_pct"]),
        )
        chosen_cutoff = float(recommended_df.iloc[0]["max_skip_score_included"]) if len(recommended_df) else 1.0
        current_scored_df = skip_mod.score_current_streams(
            current_df=current_df,
            history_scored_df=scored_df,
            negative_traits_df=negative_traits_df,
            top_negative_traits_to_use=int(sparams["top_negative_traits_to_use"]),
            chosen_skip_score_cutoff=chosen_cutoff,
        )

        merged_df = playlist_df.merge(
            current_scored_df.rename(columns={"stream_id": "stream"}),
            on=["stream", "seed"],
            how="left",
            suffixes=("", "_skip"),
        )
        if "skip_class" not in merged_df.columns and "current_skip_class" in merged_df.columns:
            merged_df["skip_class"] = merged_df["current_skip_class"]
        merged_df["skip_class"] = merged_df.get("skip_class", pd.Series(["PLAY"] * len(merged_df))).fillna("PLAY")
        merged_df["action"] = merged_df["skip_class"].apply(lambda x: "KEEP" if str(x).upper() == "PLAY" else "STRIP")

        keep_df = merged_df[merged_df["action"] == "KEEP"].copy().reset_index(drop=True)
        strip_df = merged_df[merged_df["action"] == "STRIP"].copy().reset_index(drop=True)

    except Exception as e:
        st.exception(e)
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Daily rows", f"{len(merged_df):,}")
    c2.metric("KEEP", f"{len(keep_df):,}")
    c3.metric("STRIP", f"{len(strip_df):,}")

    st.subheader("Winner playlist summary")
    st.dataframe(winner_summary_df, use_container_width=True)

    st.subheader("Merged board preview")
    st.dataframe(merged_df.head(int(rows_to_show)), use_container_width=True)

    st.subheader("Downloads")
    d1, d2, d3 = st.columns(3)
    with d1:
        render_download(merged_df, "Download daily merged board CSV", "core025_daily_merged_board__v12.csv")
    with d2:
        render_download(keep_df, "Download KEEP list CSV", "core025_daily_keep_list__v12.csv")
    with d3:
        render_download(strip_df, "Download STRIP list CSV", "core025_daily_strip_list__v12.csv")


# -----------------------------------------------------------------------------
# Main shell
# -----------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(page_title="Core025 Independent Baseline Shell", layout="wide")
    st.title("Core025 Independent Baseline Shell")
    st.markdown(f"**{BUILD_MARKER}**")
    st.caption(
        "Winner LAB and Skip LAB are intentionally kept operationally isolated so they can reproduce their original baselines independently."
    )

    try:
        winner_mod, skip_mod, winner_path, skip_path = load_source_modules()
    except Exception as e:
        st.exception(e)
        return

    st.write(f"Winner source: `{winner_path.name}`")
    st.write(f"Skip source: `{skip_path.name}`")

    section = st.segmented_control(
        "Section",
        options=[
            "Winner LAB (Exact v14)",
            "Skip LAB (Exact)",
            "Daily KEEP / STRIP",
        ],
        selection_mode="single",
        default="Winner LAB (Exact v14)",
    )

    if section == "Winner LAB (Exact v14)":
        st.info("This section runs the original v14 winner app path directly. For baseline comparison, use LAB Walk-Forward with no skip influence.")
        winner_mod.main()
        return

    if section == "Skip LAB (Exact)":
        st.info("This section runs the original skip-ladder app path directly. Compare only to the skip-only baseline.")
        skip_mod.main()
        return

    run_daily_section(winner_mod, skip_mod)


if __name__ == "__main__":
    main()
