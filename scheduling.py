from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict, deque

def topological_order(tasks: List[Dict[str, Any]]) -> List[str]:
    """Return task IDs in a dependency-respecting order."""
    indeg = defaultdict(int)
    deps = {t["id"]: set(t.get("dependencies", [])) for t in tasks}
    for t in tasks:
        indeg[t["id"]] = len(deps[t["id"]])
    q = deque([tid for tid, d in indeg.items() if d == 0])
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v, dv in deps.items():
            if u in dv:
                dv.remove(u)
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
    remaining = [tid for tid, d in indeg.items() if d > 0 and tid not in order]
    return order + remaining

def compute_cpm_baseline(tasks: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """
    Compute an ASAP CPM baseline ignoring resources.
    Returns dict task_id -> {es, ef, ls, lf, duration}
    Missing durations are treated as 0 (marker tasks).
    """
    info = {t["id"]: {"duration": float(t["duration_hours"] or 0.0)} for t in tasks}
    deps = {t["id"]: set(t.get("dependencies", [])) for t in tasks}
    order = topological_order(tasks)

    # forward pass (ES/EF)
    for tid in order:
        es = 0.0
        for d in deps[tid]:
            es = max(es, info[d]["ef"])
        ef = es + info[tid]["duration"]
        info[tid]["es"] = es
        info[tid]["ef"] = ef

    # project finish
    proj_finish = max((info[tid]["ef"] for tid in info), default=0.0)

    # backward pass (LS/LF)
    rev = list(reversed(order))
    for tid in rev:
        # successors
        succ_ls = [info[s]["ls"] for s in info.keys() if tid in deps.get(s, set()) and "ls" in info[s]]
        lf = min(succ_ls) if succ_ls else proj_finish
        ls = lf - info[tid]["duration"]
        info[tid]["ls"] = ls
        info[tid]["lf"] = lf

    return info

def level_resources(tasks: List[Dict[str, Any]],
                    base_info: Dict[str, Dict[str, float]],
                    pool_by_category: bool,
                    capacity_by_category: Dict[str, int]) -> Dict[str, Dict[str, float]]:
    """
    Apply simple resource leveling.
    If pool_by_category=True: limit concurrent tasks by crew_category capacity (e.g., '2' => 2 crews max).
    Else: respect exact crew_code: each code is exclusive (capacity=1).
    Returns schedule dict: task_id -> {start, finish, duration}
    """
    tasks_by_id = {t["id"]: t for t in tasks}
    order = sorted(tasks, key=lambda t: (base_info[t["id"]]["es"], t["planned_day"], t["name"]))

    if pool_by_category:
        # track active intervals per category
        scheduled = []
    else:
        code_busy_until = defaultdict(float)  # crew_code -> time

    schedule = {}
    for t in order:
        tid = t["id"]
        dur = float(t["duration_hours"] or 0.0)
        # earliest by dependencies
        est = 0.0
        for dep in t.get("dependencies", []):
            if dep in schedule:
                est = max(est, schedule[dep]["finish"])
            else:
                est = max(est, base_info[dep]["ef"])
        est = max(est, base_info[tid]["es"])

        if not t.get("crew_category") and not t.get("crew_code"):
            start = est
        else:
            if pool_by_category:
                cat = t.get("crew_category") or "UNSPEC"
                cap = max(1, int(capacity_by_category.get(cat, 1)))
                start = est
                # find category intervals
                def cat_active_at(timepoint: float) -> int:
                    cnt = 0
                    for rec in scheduled:
                        if rec["cat"] == cat and rec["start"] < timepoint < rec["finish"]:
                            cnt += 1
                    return cnt
                while dur > 0 and cat_active_at(start) >= cap:
                    # push to earliest finish among same category
                    finishes = [rec["finish"] for rec in scheduled if rec["cat"] == cat and rec["finish"] > start]
                    start = min(finishes) if finishes else start
            else:
                code = t.get("crew_code") or "UNSPEC"
                ready = code_busy_until[code]
                start = max(est, ready)

        finish = start + dur
        schedule[tid] = {
            "task": t["name"],
            "section": t.get("section"),
            "subsection": t.get("subsection"),
            "start": start,
            "finish": finish,
            "duration": dur,
            "crew_code": t.get("crew_code"),
            "crew_category": t.get("crew_category")
        }
        if pool_by_category:
            scheduled.append({"cat": t.get("crew_category") or "UNSPEC", "start": start, "finish": finish})
        else:
            code = t.get("crew_code") or "UNSPEC"
            code_busy_until[code] = finish

    return schedule

def compute_project_metrics(schedule: Dict[str, Dict[str, float]], hours_per_day: float) -> Dict[str, float]:
    if not schedule:
        return {"duration_days": 0.0}
    proj_finish_hours = max(v["finish"] for v in schedule.values())
    return {"duration_days": proj_finish_hours / max(1.0, hours_per_day)}
