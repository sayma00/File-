import os
import re
import json
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from typing import List, Dict, Optional, Tuple

# ---------- helpers ----------

def num_clean(s: str) -> Optional[int]:
    """Convert OCR number strings like '1,342,700', '3.900.07', '15,773' -> int."""
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"[^0-9,.\-]", "", s)  # keep digits, comma, dot, sign

    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", "")

    if s.count(".") > 1:
        s = s.replace(".", "")

    try:
        val = float(s)
        return int(round(val))
    except Exception:
        return None


_CANON_MAP = {
    "property, plant and equipment": "property, plant and equipment",
    "intangible assets": "intangible assets",
    "right-of-use (rou) asset": "right-of-use (rou) asset",
    "advances, deposits and prepayments": "advances, deposits and prepayments",
    "non-current assets": "non-current assets",
    "inventories": "inventories",
    "trade and other receivables": "trade and other receivables",
    "cash and cash equivalents": "cash and cash equivalents",
    "current assets": "current assets",
    "total assets": "total assets",
    "share capital": "share capital",
    "other component of equity": "other component of equity",
    "general reserve/retained earnings": "general reserve/retained earnings",
    "equity attributable to owners of the company": "equity attributable to owners of the company",
    "non-controlling interest": "non-controlling interest",
    "total equity": "total equity",
    "employee benefits-non current portion": "employee benefits-non current portion",
    "deferred tax liabilities": "deferred tax liabilities",
    "lease liability-non current portion": "lease liability-non current portion",
    "other non-current liabilities": "other non-current liabilities",
    "non-current liabilities": "non-current liabilities",
    "lease liabilities-current portion": "lease liabilities-current portion",
    "employee benefits-current portion": "employee benefits-current portion",
    "trade and other payables": "trade and other payables",
    "provision for expenses": "provision for expenses",
    "current tax liabilities": "current tax liabilities",
    "unclaimed dividend": "unclaimed dividend",
    "current liabilities": "current liabilities",
    "total liabilities": "total liabilities",
    "total equity and liabilities": "total equity and liabilities",
    "net assets value (nav) per share": "net assets value (nav) per share",
}

_FIXES = [
    (r"\bequny\b", "equity"),
    (r"\bbabies\b", "liabilities"),
    (r"\babies\b", "liabilities"),
    (r"\babities\b", "liabilities"),
    (r"\bbenetis\b", "benefits"),
    (r"\bcavent\b", "current"),
    (r"\bnon[\s\-]*cunent\b", "non current"),
    (r"\bnon[\s\-]*conent\b", "non current"),
    (r"\bprepaymens\b", "prepayments"),
    (r"\bdeposis\b", "deposits"),
    (r"\btra[ \-]*de\b", "trade"),
    (r"\bgenera reserve ?/ ?tetained ening\b", "general reserve/retained earnings"),
    (r"\brou ?asset\b", "right-of-use (rou) asset"),
]

def clean_label(text: str) -> str:
    t = text.lower().strip(" :\t-—_.")
    t = re.sub(r"\s+", " ", t)
    for pat, rep in _FIXES:
        t = re.sub(pat, rep, t)
    t = t.strip(" :\t-—_.")
    for key in _CANON_MAP:
        if key in t:
            return _CANON_MAP[key]
    return t

def extract_two_years_from_line(line: str) -> Optional[Tuple[int, int, str]]:
    """Take a line, extract last two numeric tokens as (2022, 2021), return with label."""
    s = line.strip()
    if not s:
        return None
    if re.search(r"^\s*(assets|equity|liabilities|notes|net assets value)", s, re.I):
        return None

    nums = re.findall(r"[-+]?\d[\d,.\-]*", s)
    if len(nums) < 2:
        return None

    v2022 = num_clean(nums[-2])
    v2021 = num_clean(nums[-1])

    label_part = re.sub(r"[-+]?\d[\d,.\-]*\s*$", "", s)            # drop last number
    label_part = re.sub(r"[-+]?\d[\d,.\-]*\s*$", "", label_part)   # drop second last
    label = clean_label(label_part)

    if not label:
        return None
    if v2022 is None and v2021 is None:
        return None
    return (v2022, v2021, label)

def parse_balance_sheet_text(raw_text: str) -> Dict:
    rows: List[Dict] = []
    seen = set()
    for raw_line in raw_text.splitlines():
        res = extract_two_years_from_line(raw_line)
        if not res:
            continue
        v2022, v2021, label = res
        if label in seen:
            continue
        seen.add(label)
        rows.append({"label": label, "2022": v2022, "2021": v2021})
    return {"table_years": ["2022", "2021"], "rows": rows}

# ---------- DB ops ----------

def get_conn_from_env():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )

def fetch_latest_ocr_row(conn, row_id: Optional[int] = None):
    sql = "SELECT id, filename, raw_text, fields FROM ocr_documents "
    params = ()
    if row_id is None:
        sql += "ORDER BY id DESC LIMIT 1"
    else:
        sql += "WHERE id=%s"
        params = (row_id,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()

def upsert_balance_sheet(conn, row_id: int, balance_sheet: Dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ocr_documents
               SET fields = COALESCE(fields, '{}'::jsonb) || %s::jsonb
             WHERE id = %s
            """,
            (Json({"balance_sheet": balance_sheet}), row_id),
        )

# ---------- main ----------

def main():
    conn = get_conn_from_env()
    conn.autocommit = True

    row = fetch_latest_ocr_row(conn, row_id=None)  # put a specific id if needed
    if not row:
        print("No rows in ocr_documents.")
        return

    _id, filename, raw_text, fields = row
    print(f"[INFO] Parsing id={_id}, file={filename}")

    parsed = parse_balance_sheet_text(raw_text)

    print(json.dumps(parsed, indent=2, ensure_ascii=False))

    upsert_balance_sheet(conn, _id, parsed)
    print(f"[OK] Updated fields.balance_sheet for id={_id}")

if __name__ == "__main__":
    main()
