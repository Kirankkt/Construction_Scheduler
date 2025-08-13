from typing import Dict, Any, Optional
import pandas as pd
import plotly.express as px

def _hours_to_datetime(base_date: pd.Timestamp, hours: float) -> pd.Timestamp:
    return base_date + pd.to_timedelta(hours, unit="h")

def gantt_figure(schedule: Dict[str, Dict[str, Any]], start_date: Optional[str], hours_per_day: float):
    if not schedule:
        return px.timeline(pd.DataFrame(columns=["Task","Start","Finish"]), x_start="Start", x_end="Finish", y="Task")
    base = pd.to_datetime(start_date) if start_date else pd.to_datetime("2025-01-01")
    rows = []
    for tid, s in schedule.items():
        rows.append({
            "Task ID": tid,
            "Task": f"{s['task']} ({s.get('subsection')})",
            "Section": s.get("section") or "N/A",
            "Crew": s.get("crew_code") or (s.get("crew_category") or "N/A"),
            "Start": _hours_to_datetime(base, s["start"]),
            "Finish": _hours_to_datetime(base, s["finish"]),
            "Duration (h)": s["duration"]
        })
    df = pd.DataFrame(rows)
    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task", color="Section",
                      hover_data=["Task ID", "Crew", "Duration (h)", "Section"])
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(title="Project Schedule (Gantt)", xaxis_title="Date/Time", yaxis_title="Tasks")
    return fig

def resource_timeline(schedule: Dict[str, Dict[str, Any]], start_date: Optional[str]):
    if not schedule:
        return px.timeline(pd.DataFrame(columns=["Crew","Start","Finish"]), x_start="Start", x_end="Finish", y="Crew")
    base = pd.to_datetime(start_date) if start_date else pd.to_datetime("2025-01-01")
    rows = []
    for tid, s in schedule.items():
        crew = s.get("crew_code") or (s.get("crew_category") or "N/A")
        rows.append({
            "Crew": crew,
            "Start": base + pd.to_timedelta(s["start"], unit="h"),
            "Finish": base + pd.to_timedelta(s["finish"], unit="h"),
            "Task": s["task"]
        })
    df = pd.DataFrame(rows)
    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Crew", color="Crew", hover_data=["Task"])
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(title="Resource Utilization", xaxis_title="Date/Time", yaxis_title="Crew / Category")
    return fig
