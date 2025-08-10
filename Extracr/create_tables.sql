CREATE TABLE IF NOT EXISTS ocr_documents (
    id          SERIAL PRIMARY KEY,
    filename    TEXT NOT NULL,
    raw_text    TEXT NOT NULL,
    fields      JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
