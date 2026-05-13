CREATE TABLE IF NOT EXISTS session_context_summaries (
    session_id_hash TEXT PRIMARY KEY
        CHECK (
            session_id_hash ~ '^[0-9A-Fa-f]{64}$'
        ),
    context_source TEXT NOT NULL CHECK (btrim(context_source) <> ''),
    confidence DOUBLE PRECISION NOT NULL
        CHECK (
            confidence >= 0
            AND confidence < 'Infinity'::double precision
            AND confidence <> 'NaN'::double precision
        ),
    privacy_level TEXT NOT NULL CHECK (btrim(privacy_level) <> ''),
    primary_kind TEXT NOT NULL CHECK (btrim(primary_kind) <> ''),
    evidence_count BIGINT NOT NULL DEFAULT 0 CHECK (evidence_count >= 0),
    search_execute_count BIGINT NOT NULL DEFAULT 0 CHECK (search_execute_count >= 0),
    warning_count BIGINT NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
    area_id TEXT CHECK (area_id IS NULL OR btrim(area_id) <> ''),
    line_id TEXT CHECK (line_id IS NULL OR btrim(line_id) <> ''),
    station_id TEXT CHECK (station_id IS NULL OR btrim(station_id) <> ''),
    summary_payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(summary_payload) = 'object'),
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (last_seen_at >= first_seen_at)
);

CREATE INDEX IF NOT EXISTS session_context_summaries_recent_idx
    ON session_context_summaries (last_seen_at DESC, session_id_hash ASC);

CREATE INDEX IF NOT EXISTS session_context_summaries_source_idx
    ON session_context_summaries (context_source, last_seen_at DESC, session_id_hash ASC);

CREATE INDEX IF NOT EXISTS session_context_summaries_primary_kind_idx
    ON session_context_summaries (primary_kind, last_seen_at DESC, session_id_hash ASC);

CREATE INDEX IF NOT EXISTS session_context_summaries_area_idx
    ON session_context_summaries (area_id, last_seen_at DESC, session_id_hash ASC)
    WHERE area_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS session_context_summaries_line_idx
    ON session_context_summaries (line_id, last_seen_at DESC, session_id_hash ASC)
    WHERE line_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS session_context_summaries_station_idx
    ON session_context_summaries (station_id, last_seen_at DESC, session_id_hash ASC)
    WHERE station_id IS NOT NULL;
