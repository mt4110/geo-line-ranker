ALTER TABLE schools
    ADD COLUMN IF NOT EXISTS prefecture_name TEXT;

CREATE INDEX IF NOT EXISTS idx_schools_prefecture_name_lower
    ON schools (lower(prefecture_name));
