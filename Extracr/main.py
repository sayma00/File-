import os
import sys
import re
import json
import glob
import traceback
from typing import Dict, Optional

import cv2
import pytesseract
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from PIL import Image


def load_env():
    load_dotenv()
    tess_path = os.getenv("TESSERACT_PATH", "").strip()
    if tess_path:
        pytesseract.pytesseract.tesseract_cmd = tess_path

    pg = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "postgres"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", ""),
    }
    return pg


def connect_db(pg_cfg):
    conn = psycopg2.connect(
        host=pg_cfg["host"],
        port=pg_cfg["port"],
        dbname=pg_cfg["dbname"],
        user=pg_cfg["user"],
        password=pg_cfg["password"],
    )
    conn.autocommit = True
    return conn


def ensure_table(conn):
    ddl = '''
    CREATE TABLE IF NOT EXISTS ocr_documents (
        id          SERIAL PRIMARY KEY,
        filename    TEXT NOT NULL,
        raw_text    TEXT NOT NULL,
        fields      JSONB,
        created_at  TIMESTAMPTZ DEFAULT NOW()
    );
    '''
    with conn.cursor() as cur:
        cur.execute(ddl)


def preprocess_image(img_path: str) -> Optional[Image.Image]:
    try:
        image = cv2.imread(img_path)
        if image is None:
            return None
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        gray = cv2.bilateralFilter(gray, d=11, sigmaColor=17, sigmaSpace=17)
        thr = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 31, 2
        )
        thr = cv2.resize(thr, None, fx=1.25, fy=1.25, interpolation=cv2.INTER_CUBIC)
        return Image.fromarray(thr)
    except Exception:
        traceback.print_exc()
        return None


def ocr_image(pil_img: Image.Image) -> str:
    config = r'--oem 1 --psm 6'
    text = pytesseract.image_to_string(pil_img, config=config)
    return '\n'.join(line.strip() for line in text.splitlines() if line.strip())


def parse_fields(raw_text: str) -> Dict:
    fields: Dict[str, object] = {}

    kv_pattern = re.compile(r'^\s*([A-Za-z0-9 _\-/\.]+)\s*[:=]\s*(.+?)\s*$', re.M)
    for m in kv_pattern.finditer(raw_text):
        key = m.group(1).strip().lower()
        key = key.replace(' ', '_').replace('-', '_').replace('/', '_').replace('.', '_')
        key = re.sub(r'[^a-z0-9_]+', '', key)[:64]
        val = m.group(2).strip()
        if key:
            fields[key] = val

    emails = re.findall(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}', raw_text)
    if emails:
        fields['emails'] = list(dict.fromkeys(emails))

    phones = re.findall(r'(?:\+?\d{1,3}[\s\-\.]?)?(?:\(?\d{2,4}\)?[\s\-\.]?)?\d{3,4}[\s\-\.]?\d{4}', raw_text)
    clean_phones = [p.strip() for p in phones if len(re.sub(r'\D', '', p)) >= 8]
    if clean_phones:
        fields['phones'] = list(dict.fromkeys(clean_phones))

    return fields


def insert_document(conn, filename: str, raw_text: str, fields: Dict):
    sql = '''
    INSERT INTO ocr_documents (filename, raw_text, fields)
    VALUES (%s, %s, %s)
    RETURNING id;
    '''
    with conn.cursor() as cur:
        cur.execute(sql, (filename, raw_text, Json(fields)))
        new_id = cur.fetchone()[0]
        return new_id


def process_image_path(conn, path: str):
    pil_img = preprocess_image(path)
    if pil_img is None:
        print(f"[WARN] Could not read image: {path}")
        return

    text = ocr_image(pil_img)
    if not text.strip():
        print(f"[WARN] No text detected: {path}")
        return

    fields = parse_fields(text)
    new_id = insert_document(conn, os.path.basename(path), text, fields)
    print(f"[OK] Saved OCR (id={new_id}) for: {path}")
    if fields:
        print("     Parsed fields:")
        print(json.dumps(fields, ensure_ascii=False, indent=2))


def main():
    pg_cfg = load_env()
    try:
        conn = connect_db(pg_cfg)
    except Exception as e:
        print("[FATAL] Could not connect to PostgreSQL. Check .env settings.")
        print(str(e))
        sys.exit(1)

    ensure_table(conn)

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py <image_or_folder_path> [more_paths...]")
        print("\nExamples:")
        print("  python main.py samples/invoice1.jpg")
        print("  python main.py samples/")
        sys.exit(0)

    input_paths = []
    for arg in sys.argv[1:]:
        if os.path.isdir(arg):
            input_paths.extend(
                glob.glob(os.path.join(arg, "**", "*.png"), recursive=True) +
                glob.glob(os.path.join(arg, "**", "*.jpg"), recursive=True) +
                glob.glob(os.path.join(arg, "**", "*.jpeg"), recursive=True) +
                glob.glob(os.path.join(arg, "**", "*.tif"), recursive=True) +
                glob.glob(os.path.join(arg, "**", "*.tiff"), recursive=True) +
                glob.glob(os.path.join(arg, "**", "*.bmp"), recursive=True) +
                glob.glob(os.path.join(arg, "**", "*.webp"), recursive=True)
            )
        else:
            input_paths.append(arg)

    if not input_paths:
        print("[INFO] No images found to process.")
        return

    for p in input_paths:
        try:
            process_image_path(conn, p)
        except Exception:
            print(f"[ERROR] Failed to process: {p}")
            traceback.print_exc()


if __name__ == "__main__":
    main()
