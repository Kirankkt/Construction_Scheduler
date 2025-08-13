# Construction Scheduling Optimizer (Streamlit)

Interactive, real-time scheduling tool for your renovation project. It:
- Parses your CSV with Day 1–91, Time (hours), and Labor (workers) triplets.
- Extracts drawing notes from the provided Demolition and Fabrication PDFs.
- Builds tasks per Section -> Subsection -> Day with durations and crew codes (e.g., 2.01).
- Computes CPM and applies resource leveling.
- Lets you tune labour availability, target duration, and filters.
- Visualizes a Gantt and Resource Utilization timeline.
- Flags missing durations (no imputation).

## Project layout

```text
construction_scheduler/
├─ app.py
├─ data_ingestion.py
├─ scheduling.py
├─ visualization.py
├─ requirements.txt
└─ README.md
```

## Quick start

1) Install:
   ```bash
   pip install -r requirements.txt
   ```
2) Run:
   ```bash
   streamlit run app.py
   ```
3) Use default file paths or upload your own in the sidebar.


## Bundled data
The app loads these files from `./data/` automatically (no uploads needed):
- `13 B Renovation_working.csv`
- `ROY - CIVIL WORKS - DEMOLISION AND EXTENSION.pdf`
- `ROY - CIVIL WORKS - FABRICATION.pdf`
