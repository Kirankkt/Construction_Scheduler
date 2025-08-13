import os
import streamlit as st
import pandas as pd
from typing import Dict, Any, List

from data_ingestion import (
    parse_csv_to_tasks,
    load_drawing_notes_from_cache,
    rebuild_drawing_notes_cache,
    match_notes_to_tasks,
)

from scheduling import compute_cpm_baseline, level_resources, compute_project_metrics
from visualization import gantt_figure, resource_timeline

st.set_page_config(page_title="Construction Scheduler", layout="wide")

st.title("🔧 Construction Scheduling Optimizer")
st.caption("Boots instantly. Drawing notes are cached to disk and refreshed on demand (no re-parsing on every run).")

# Fixed, bundled data paths (relative to the repo)
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "13 B Renovation_working.csv")
PDF_PATHS = [
    os.path.join(DATA_DIR, "ROY - CIVIL WORKS - DEMOLISION AND EXTENSION.pdf"),
    os.path.join(DATA_DIR, "ROY - CIVIL WORKS - FABRICATION.pdf"),
]

# Validate files exist
missing = [p for p in [CSV_PATH] + PDF_PATHS if not os.path.exists(p)]
if missing:
    st.error("Missing bundled files: " + ", ".join([os.path.basename(p) for p in missing]))
    st.stop()

# -----------------------------
# Sidebar controls
# -----------------------------
with st.sidebar:
    st.header("Scenario Settings")
    hours_per_day = st.radio("Working hours per day", options=[7.0, 8.0], index=1, horizontal=True)
    start_date = st.date_input("Project start date", pd.to_datetime("today"))
    auto_chain = st.toggle("Auto-chain tasks within Section/Subsection by day order", value=True)
    pool_by_cat = st.toggle("Pool by category (ignore exact crew codes)", value=False)
    target_days = st.number_input("Target duration (days)", min_value=1, value=30)
    enforce_target = st.toggle("Enforce target (warn/advise to crash/overlap)", value=False)

    st.subheader("Drawings")
    use_notes = st.toggle("Use drawing notes", value=True)
    refresh_notes = st.button("Refresh notes from PDFs (parse/cache)")

# -----------------------------
# Cached CSV parse (fast reruns)
# -----------------------------
@st.cache_data(show_spinner=False)
def _parse_csv_cached(path: str, hours_per_day: float, auto_chain: bool):
    return parse_csv_to_tasks(path, working_hours_per_day=hours_per_day, auto_chain_within_subsection=auto_chain)

with st.spinner("Parsing CSV into tasks..."):
    tasks, warnings = _parse_csv_cached(CSV_PATH, hours_per_day, auto_chain)

if warnings:
    for w in warnings:
        st.warning(w)

if not tasks:
    st.error("No tasks parsed from CSV.")
    st.stop()

sections = sorted({t["section"] for t in tasks if t.get("section")})
crew_cats = sorted({t["crew_category"] for t in tasks if t.get("crew_category")})

# -----------------------------
# Drawings: cached notes
# -----------------------------
notes: List[str] = []
note_matches: List[Dict[str, Any]] = []

if use_notes:
    # If the user pressed "Refresh", rebuild (parses only changed PDFs) and cache.
    if refresh_notes:
        with st.spinner("Parsing PDFs and updating cache (one-time / when changed)..."):
            notes = rebuild_drawing_notes_cache(PDF_PATHS, cache_dir=DATA_DIR)
    else:
        notes = load_drawing_notes_from_cache(cache_dir=DATA_DIR)

    if not notes and not refresh_notes:
        st.info("No cached drawing notes found yet. Click **Refresh notes from PDFs** to parse once and cache.")
    elif notes:
        note_matches = match_notes_to_tasks(notes, tasks, limit=3)

# -----------------------------
# Filters
# -----------------------------
st.subheader("Filters")
sel_sections = st.multiselect("Sections", sections, default=sections)
f_tasks = [t for t in tasks if (not sel_sections or (t.get("section") in sel_sections))]

# -----------------------------
# Crew capacities
# -----------------------------
st.subheader("Crew Availability")
cap_cols = st.columns(min(4, max(1, len(crew_cats)))) if crew_cats else st.columns(1)
capacity_by_category: Dict[str, int] = {}
if crew_cats:
    for i, cat in enumerate(crew_cats):
        with cap_cols[i % len(cap_cols)]:
            capacity_by_category[cat] = st.number_input(
                f"Category {cat} crews",
                min_value=1, max_value=10, value=1, key=f"cap_{cat}"
            )
else:
    st.info("No crew categories detected in CSV (Labor (workers) cells like '2.01'). You can still view the CPM baseline.")

# -----------------------------
# Baseline CPM + Leveling
# -----------------------------
with st.spinner("Computing schedule..."):
    base = compute_cpm_baseline(f_tasks)
    schedule = level_resources(
        f_tasks, base,
        pool_by_category=pool_by_cat,
        capacity_by_category=capacity_by_category
    )

# Target enforcement (advice)
metrics = compute_project_metrics(schedule, hours_per_day=hours_per_day)
if enforce_target and metrics["duration_days"] > target_days:
    st.warning(
        f"Schedule exceeds target ({metrics['duration_days']:.1f} d > {target_days} d). "
        "Consider adding crews (increase capacities), allowing pooling by category, or enabling overlap."
    )

# -----------------
