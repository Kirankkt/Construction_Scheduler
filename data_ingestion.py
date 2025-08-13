import os
import re
import json
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from rapidfuzz import fuzz, process

# -----------------------------
# CSV helpers
# -----------------------------

DAY_COL_PATTERN = re.compile(r"^Day\s*(\d+)$", re.IGNORECASE)

def _safe_series_get(row: pd.Series, key: Optional[str]):
    if key is None:
        return None
    try:
        return row.get(key, None)
    except Exception:
        return None

def _detect_day_triplets(columns: List[str]) -> List[Tuple[str, Optional[str], Optional[str], int]]:
    """
    Return a list of (day_col, time_col, labour_col, day_index) sorted by day number.
    Tolerates Excel 'Unnamed' columns after Day N:
      - Try canonical names: Time (hours)[.k], Labor (workers)[.k]
      - Else, use the next two physical columns after Day N (if not another Day column)
      - Else, set them to None (duration, labour treated as missing)
    """
    days_idx = []
    for i, c in enumerate(columns):
        m = DAY_COL_PATTERN.match(str(c).strip())
        if m:
            days_idx.append((i, c, int(m.group(1))))
    days_idx.sort(key=lambda x: x[2])

    triplets: List[Tuple[str, Optional[str], Optional[str], int]] = []
    for ordinal, (i, day_col, dnum) in enumerate(days_idx):
        suffix = "" if ordinal == 0 else f".{ordinal}"
        canon_time = f"Time (hours){suffix}"
        canon_lab  = f"Labor (workers){suffix}"

        time_col = canon_time if canon_time in columns else None
        labour_col = canon_lab if canon_lab in columns else None

        if time_col is None or labour_col is None:
            nxt1 = columns[i+1] if i + 1 < len(columns) else None
            nxt2 = columns[i+2] if i + 2 < len(columns) else None
            if time_col is None and nxt1 and not DAY_COL_PATTERN.match(str(nxt1)):
                time_col = nxt1
            if labour_col is None and nxt2 and not DAY_COL_PATTERN.match(str(nxt2)):
                labour_col = nxt2

        triplets.append((day_col, time_col, labour_col, dnum))
    return triplets

def _is_section_header(row: pd.Series, triplets: List[Tuple[str, Optional[str], Optional[str], int]]) -> bool:
    """A header row has no entries in any Day/Time/Labour columns."""
    for (day_col, time_col, labour_col, _) in triplets:
        if pd.notna(_safe_series_get(row, day_col)) \
           or pd.notna(_safe_series_get(row, time_col)) \
           or pd.notna(_safe_series_get(row, labour_col)):
            return False
    return True

def _clean_str(x: Any) -> Optional[str]:
    if pd.isna(x): return None
    s = str(x).strip()
    return s if s else None

def parse_csv_to_tasks(csv_path: str,
                       working_hours_per_day: float = 8.0,
                       auto_chain_within_subsection: bool = True) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Parse the wide CSV into a flat list of tasks.
    Returns (tasks, warnings). No imputation - missing durations remain None.
    Task schema:
      id, section, subsection, name, planned_day (int), duration_hours (float|None),
      crew_code (str|None), crew_category (str|None), dependencies: List[str]
    """
    df = pd.read_csv(csv_path)
    columns = list(df.columns)
    row_label_col = columns[0]  # leftmost label column (often 'Unnamed: 0')
    triplets = _detect_day_triplets(columns)
    warnings = []
    if not triplets:
        warnings.append("No 'Day N' columns found. Please verify the CSV structure.")
        return [], warnings

    tasks: List[Dict[str, Any]] = []
    current_section = None
    task_counter = 0

    for _, row in df.iterrows():
        label = _clean_str(_safe_series_get(row, row_label_col))
        # Decide if this is a header (Section) or a Subsection row
        if _is_section_header(row, triplets):
            current_section = label
            continue  # next row

        # Non-header row: treat 'label' as subsection name
        subsection = label
        for (day_col, time_col, labour_col, dnum) in triplets:
            name = _clean_str(_safe_series_get(row, day_col))
            if not name:
                continue  # no task that day for this subsection
            name = re.sub(r"\s+,", ",", name).strip().rstrip(",")
            dur_val = _safe_series_get(row, time_col)
            duration_hours = float(dur_val) if pd.notna(dur_val) else None
            labour_val = _clean_str(_safe_series_get(row, labour_col))
            crew_code = None
            crew_cat = None
            if labour_val:
                crew_code = str(labour_val).strip()
                m = re.match(r"^\s*(\d+)(?:\.\d+)?\s*$", crew_code)
                if m:
                    crew_cat = m.group(1)

            task_id = f"T{task_counter:04d}"
            task_counter += 1
            tasks.append({
                "id": task_id,
                "section": current_section,
                "subsection": subsection,
                "name": name,
                "planned_day": int(dnum),
                "duration_hours": duration_hours,   # may be None (no imputation)
                "crew_code": crew_code,
                "crew_category": crew_cat,
                "dependencies": []  # filled later
            })

    # Auto-chain dependencies within (section, subsection) by ascending planned_day
    if auto_chain_within_subsection:
        from collections import defaultdict
        by_group = defaultdict(list)
        for t in tasks:
            key = (t["section"], t["subsection"])
            by_group[key].append(t)
        for key, items in by_group.items():
            items.sort(key=lambda x: (x["planned_day"], x["name"]))
            for prev, cur in zip(items, items[1:]):
                cur["dependencies"].append(prev["id"])

    return tasks, warnings

# -----------------------------
# PDF note caching + parsers
# -----------------------------

def _pdf_cache_file(cache_dir: str = "data") -> str:
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "drawing_notes_cache.json")

def _quick_sig(path: str) -> Dict[str, int]:
    """Fast file signature: size + mtime (good enough to detect changes)."""
    st = os.stat(path)
    return {"size": int(st.st_size), "mtime": int(st.st_mtime)}

def _parse_pdf_notes_with_pymupdf(pdf_path: str) -> List[str]:
    try:
        import fitz  # PyMuPDF
    except Exception:
        return []
    notes, seen = [], set()
    doc = fitz.open(pdf_path)
    for page in doc:
        text = page.get_text()
        for raw in text.splitlines():
            line = raw.strip()
            if line.lower().startswith("note -"):
                content = line[6:].strip()
                if content and content not in seen:
                    notes.append(content)
                    seen.add(content)
    doc.close()
    return notes

def _parse_pdf_notes_with_pdfplumber(pdf_path: str) -> List[str]:
    try:
        import pdfplumber
    except Exception:
        return []
    notes, seen = [], set()
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw in text.splitlines():
                line = raw.strip()
                if line.lower().startswith("note -"):
                    content = line[6:].strip()
                    if content and content not in seen:
                        notes.append(content)
                        seen.add(content)
    return notes

def _parse_pdf_notes(pdf_path: str) -> List[str]:
    # Prefer PyMuPDF if available, else fallback to pdfplumber
    out = _parse_pdf_notes_with_pymupdf(pdf_path)
    if out:
        return out
    return _parse_pdf_notes_with_pdfplumber(pdf_path)

def load_drawing_notes_from_cache(cache_dir: str = "data") -> List[str]:
    """Load all cached notes (no parsing). Returns [] if cache missing/empty."""
    cache_path = _pdf_cache_file(cache_dir)
    if not os.path.exists(cache_path):
        return []
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
    except Exception:
        return []
    notes: List[str] = []
    for rec in cache.values():
        notes.extend(rec.get("notes", []))
    return notes

def rebuild_drawing_notes_cache(pdf_paths: List[str], cache_dir: str = "data") -> List[str]:
    """
    Parse PDFs (only those that changed), update cache, and return all notes.
    Uses PyMuPDF if present; otherwise uses pdfplumber (pure Python).
    """
    cache_path = _pdf_cache_file(cache_dir)
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
    except Exception:
        cache = {}

    changed = False
    for p in pdf_paths:
        key = os.path.basename(p)
        sig = _quick_sig(p)
        rec = cache.get(key)
        if rec and rec.get("sig") == sig:
            continue  # unchanged

        notes = _parse_pdf_notes(p)
        cache[key] = {"sig": sig, "notes": notes}
        changed = True

    if changed:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

    return load_drawing_notes_from_cache(cache_dir)

# (Optional legacy API)
def extract_drawing_notes(pdf_paths: List[str]) -> List[str]:
    notes: List[str] = []
    for p in pdf_paths:
        notes.extend(_parse_pdf_notes(p))
    return notes

# -----------------------------
# Fuzzy matching
# -----------------------------

def match_notes_to_tasks(notes: List[str], tasks: List[Dict[str, Any]], limit: int = 3) -> List[Dict[str, Any]]:
    """
    Fuzzy-match drawing notes to likely task names (best-effort suggestions).
    Returns list of dicts: {note, matches:[(task_id, task_name, score), ...]}
    """
    names = {t["id"]: t["name"] for t in tasks}
    name_list = list(names.values())
    id_by_name = {names[k]: k for k in names}
    results = []
    for note in notes:
        matches = process.extract(note, name_list, scorer=fuzz.token_set_ratio, limit=limit)
        results.append({
            "note": note,
            "matches": [(id_by_name[name], name, int(score)) for name, score, _ in matches]
        })
    return results
