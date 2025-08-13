import os
import streamlit as st
import pandas as pd
from typing import Dict, Any, List
from data_ingestion import parse_csv_to_tasks, extract_drawing_notes, match_notes_to_tasks
from scheduling import compute_cpm_baseline, level_resources, compute_project_metrics
from visualization import gantt_figure, resource_timeline

st.set_page_config(page_title="Construction Scheduler", layout="wide")

st.title("🔧 Construction Scheduling Optimizer")
st.caption("Pre-wired to local CSV/PDFs (no uploads). CPM + Resource Leveling with drawing-note insights.")

# Fixed, bundled data paths (relative to the repo)
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "13 B Renovation_working.csv")
PDF_PATHS = [
    os.path.join(DATA_DIR, "ROY - CIVIL WORKS - DEMOLISION AND EXTENSION.pdf"),
    os.path.join(DATA_DIR, "ROY - CIVIL WORKS - FABRICATION.pdf"),
]

# Sidebar: just scenario settings (no file upload)
with st.sidebar:
    st.header("Scenario Settings")
    hours_per_day = st.radio("Working hours per day", options=[7.0, 8.0], index=1, horizontal=True)
    start_date = st.date_input("Project start date", pd.to_datetime("today"))
    auto_chain = st.toggle("Auto-chain tasks within Section/Subsection by day order", value=True)
    pool_by_cat = st.toggle("Pool by category (ignore exact crew codes)", value=False)
    target_days = st.number_input("Target duration (days)", min_value=1, value=30)
    enforce_target = st.toggle("Enforce target (warn/advise to crash/overlap)", value=False)

# Validate files exist
missing = [p for p in [CSV_PATH] + PDF_PATHS if not os.path.exists(p)]
if missing:
    st.error("Missing bundled files: " + ", ".join([os.path.basename(p) for p in missing]))
    st.stop()

# Parse CSV
with st.spinner("Parsing CSV into tasks..."):
    tasks, warnings = parse_csv_to_tasks(CSV_PATH, working_hours_per_day=hours_per_day, auto_chain_within_subsection=auto_chain)

if warnings:
    for w in warnings:
        st.warning(w)

if not tasks:
    st.error("No tasks parsed from CSV.")
    st.stop()

sections = sorted({t["section"] for t in tasks if t.get("section")})
crew_cats = sorted({t["crew_category"] for t in tasks if t.get("crew_category")})

# PDFs -> notes
note_matches = []
with st.spinner("Extracting drawing notes..."):
    try:
        notes = extract_drawing_notes(PDF_PATHS) if PDF_PATHS else []
    except Exception as e:
        notes = []
        st.info(f"PDF note extraction skipped: {e}")
if notes:
    note_matches = match_notes_to_tasks(notes, tasks, limit=3)

# Filters
st.subheader("Filters")
sel_sections = st.multiselect("Sections", sections, default=sections)
f_tasks = [t for t in tasks if (not sel_sections or (t.get("section") in sel_sections))]

# Crew capacities
st.subheader("Crew Availability")
cap_cols = st.columns(min(4, max(1, len(crew_cats)))) if crew_cats else st.columns(1)
capacity_by_category: Dict[str, int] = {}
if crew_cats:
    for i, cat in enumerate(crew_cats):
        with cap_cols[i % len(cap_cols)]:
            capacity_by_category[cat] = st.number_input(f"Category {cat} crews", min_value=1, max_value=10, value=1, key=f"cap_{cat}")
else:
    st.info("No crew categories detected in CSV (Labor (workers) cells like '2.01'). You can still view the CPM baseline.")

# Baseline CPM + Leveling
with st.spinner("Computing schedule..."):
    base = compute_cpm_baseline(f_tasks)
    schedule = level_resources(f_tasks, base, pool_by_category=pool_by_cat, capacity_by_category=capacity_by_category)

# Target enforcement (advice)
metrics = compute_project_metrics(schedule, hours_per_day=hours_per_day)
if enforce_target and metrics["duration_days"] > target_days:
    st.warning(
        f"Schedule exceeds target ({metrics['duration_days']:.1f} d > {target_days} d). "
        "Consider adding crews (increase capacities), allowing pooling by category, or enabling overlap."
    )

# Charts
st.subheader("Gantt")
fig = gantt_figure(schedule, start_date=str(start_date), hours_per_day=hours_per_day)
st.plotly_chart(fig, use_container_width=True, theme="streamlit")

with st.expander("Resource Utilization", expanded=False):
    fig2 = resource_timeline(schedule, start_date=str(start_date))
    st.plotly_chart(fig2, use_container_width=True, theme="streamlit")

# Summary + Missing durations
st.subheader("Summary")
colA, colB = st.columns(2)
colA.metric("Estimated Duration (days)", f"{metrics['duration_days']:.1f}")
total_cost = 0.0
colB.metric("Total Cost", f"₹{total_cost:,.0f}")

missing_dur = [t for t in f_tasks if (t["duration_hours"] is None)]
if missing_dur:
    st.info(f"{len(missing_dur)} tasks have missing durations (no imputation).")
    md = pd.DataFrame([
        {
            "Task ID": t["id"], "Section": t["section"], "Subsection": t["subsection"],
            "Name": t["name"], "Planned Day": t["planned_day"], "Crew": t.get("crew_code") or t.get("crew_category") or ""
        } for t in missing_dur
    ])
    st.dataframe(md, use_container_width=True, hide_index=True)

if notes:
    st.subheader("Drawing Notes -> Likely Tasks (suggestions)")
    nm_rows = []
    for rec in note_matches:
        for tid, tname, score in rec["matches"]:
            nm_rows.append({"Note": rec["note"], "Match Task": tname, "Task ID": tid, "Score": score})
    import pandas as pd
    nmdf = pd.DataFrame(nm_rows).sort_values(["Note", "Score"], ascending=[True, False])
    st.dataframe(nmdf, use_container_width=True, hide_index=True)
    st.caption("Suggestions only; dependencies are not auto-applied.")

st.success("Ready. Bundled files loaded automatically.")
