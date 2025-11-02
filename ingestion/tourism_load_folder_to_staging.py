import os, re, glob
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

PG_HOST = os.getenv("PG_HOST"); PG_PORT = int(os.getenv("PG_PORT"))
PG_DB = os.getenv("PG_DB"); PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD"); PG_SSLMODE = os.getenv("PG_SSLMODE", "require")

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "tourism_raw")
REGION_DEFAULT = "Drohobych"
DEBUG = False   # можна увімкнути True для діагностики

UA_MONTHS = {
    "січень":1,"лютий":2,"березень":3,"квітень":4,"травень":5,"червень":6,
    "липень":7,"серпень":8,"вересень":9,"жовтень":10,"листопад":11,"грудень":12
}
UA_UP = {k.upper(): v for k, v in UA_MONTHS.items()}

def to_int_safe(x):
    try:
        s = str(x).replace(" ", "").replace("\xa0","").replace(",", "").replace("’","").replace("'","")
        return int(float(s))
    except: return None

def detect_header_row(df):
    for i in range(min(10, len(df))):
        row = [str(v).strip().lower() for v in df.iloc[i].tolist()]
        line = " ".join(row)
        if ("рік" in line or "year" in line) and ("місяц" in line or "month" in line) and ("кількість" in line or "відвідувач" in line or "visitors" in line):
            return i
    return None

def month_to_num(val):
    if pd.isna(val): return None
    s = str(val).strip()
    if re.fullmatch(r"\d{1,2}", s):
        m = int(s);  return m if 1 <= m <= 12 else None
    up = s.upper()
    if up in UA_UP: return UA_UP[up]
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.notna(dt): return int(dt.month)
    except: pass
    return None

def year_from_name(path):
    m = re.search(r"(19|20)\d{2}", os.path.basename(path))
    return int(m.group(0)) if m else None

def read_all_sheets(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return list(pd.read_excel(path, sheet_name=None, header=None).values())
    return [pd.read_csv(path)]

def rows_from_file(path):
    y_from_name = year_from_name(path)
    out = []
    for raw in read_all_sheets(path):
        if raw is None or raw.empty: 
            continue
        raw = raw.dropna(how="all").dropna(axis=1, how="all")
        if raw.empty: 
            continue

        hdr_idx = detect_header_row(raw)
        if hdr_idx is None:
            if DEBUG:
                print(f"[DEBUG] header not found in {os.path.basename(path)}; first row:", raw.head(1).to_dict(orient="records"))
            continue

        df = pd.read_excel(path, header=hdr_idx) if isinstance(raw, pd.DataFrame) else raw
        df = df.dropna(how="all").dropna(axis=1, how="all")
        df.columns = [str(c).strip() for c in df.columns]
        lower = {c.lower(): c for c in df.columns}

        # must-have
        ycol = next((lower[k] for k in lower if "рік" in k or k == "year"), None)
        mcol = next((lower[k] for k in lower if "місяц" in k or k == "month"), None)
        vcol = next((lower[k] for k in lower if "кількість" in k or "відвідувач" in k or "visitors" in k), None)

        # optional (female/male)
        fcol = next((lower[k] for k in lower if "жін" in k or "жiн" in k or "female" in k or "women" in k), None)
        mcol2 = next((lower[k] for k in lower if "чолов" in k or "male" in k or "men" in k), None)

        if not (mcol and vcol):
            if DEBUG: print(f"[DEBUG] columns not found in {os.path.basename(path)} -> cols: {list(df.columns)}")
            continue

        for _, r in df.iterrows():
            mo = month_to_num(r[mcol])
            if mo is None: 
                continue
            y = None
            if ycol and pd.notna(r.get(ycol)):
                try: y = int(str(r[ycol])[:4])
                except: y = None
            if y is None:
                y = y_from_name

            total = to_int_safe(r[vcol])
            female = to_int_safe(r[fcol]) if fcol in df.columns else None
            male   = to_int_safe(r[mcol2]) if mcol2 in df.columns else None

            if y and mo and total is not None:
                out.append({
                    "period_month": datetime(y, mo, 1).date(),
                    "region": "Drohobych",
                    "tourists_total": total,
                    "female_count": female,
                    "male_count": male
                })
    return out

def upsert_rows(rows):
    if not rows:
        print("[TOURISM] Nothing to insert."); return
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                            user=PG_USER, password=PG_PASSWORD, sslmode=PG_SSLMODE)
    cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE IF NOT EXISTS staging.tourism_monthly (
            period_month date PRIMARY KEY,
            region text,
            tourists_total integer,
            female_count integer,
            male_count integer
        );
    """)
    for r in rows:
        cur.execute("""
            INSERT INTO staging.tourism_monthly (period_month, region, tourists_total, female_count, male_count)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (period_month) DO UPDATE SET
                region = EXCLUDED.region,
                tourists_total = EXCLUDED.tourists_total,
                female_count = EXCLUDED.female_count,
                male_count   = EXCLUDED.male_count;
        """, (r["period_month"], r["region"], r["tourists_total"], r["female_count"], r["male_count"]))
    conn.commit(); cur.close(); conn.close()
    print(f"[TOURISM] upserted {len(rows)} rows into staging.tourism_monthly")

def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    files = []
    for p in [os.path.join(RAW_DIR, "*.xlsx"), os.path.join(RAW_DIR, "*.xls"), os.path.join(RAW_DIR, "*.csv")]:
        files.extend(glob.glob(p))
    if not files:
        print(f"[TOURISM] No files in {RAW_DIR}"); return
    all_rows = []
    for f in sorted(files):
        r = rows_from_file(f)
        print(f"[TOURISM] {os.path.basename(f)} -> {len(r)} rows")
        all_rows.extend(r)
    upsert_rows(all_rows)

if __name__ == "__main__":
    main()
