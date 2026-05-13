CREATE TABLE IF NOT EXISTS recommendation_trace_context_evidence (
    recommendation_trace_id BIGINT PRIMARY KEY REFERENCES recommendation_traces (id) ON DELETE CASCADE,
    context_source TEXT NOT NULL CHECK (btrim(context_source) <> ''),
    confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0),
    privacy_level TEXT NOT NULL CHECK (btrim(privacy_level) <> ''),
    primary_kind TEXT NOT NULL CHECK (btrim(primary_kind) <> ''),
    evidence_count BIGINT NOT NULL CHECK (evidence_count >= 0),
    strongest_strength DOUBLE PRECISION NOT NULL CHECK (strongest_strength >= 0),
    has_search_execute BOOLEAN NOT NULL,
    warning_count BIGINT NOT NULL CHECK (warning_count >= 0),
    evidence_payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(evidence_payload) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS recommendation_trace_context_evidence_kind_idx
    ON recommendation_trace_context_evidence (primary_kind, created_at DESC, recommendation_trace_id DESC);

CREATE INDEX IF NOT EXISTS recommendation_trace_context_evidence_source_idx
    ON recommendation_trace_context_evidence (context_source, created_at DESC, recommendation_trace_id DESC);

CREATE TABLE IF NOT EXISTS recommendation_trace_candidate_plans (
    recommendation_trace_id BIGINT PRIMARY KEY REFERENCES recommendation_traces (id) ON DELETE CASCADE,
    minimum_candidate_count BIGINT NOT NULL CHECK (minimum_candidate_count >= 0),
    selected_stage TEXT NOT NULL CHECK (btrim(selected_stage) <> ''),
    stop_reason TEXT NOT NULL CHECK (btrim(stop_reason) <> ''),
    area_context_usable BOOLEAN NOT NULL,
    plan_payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(plan_payload) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS recommendation_trace_candidate_plans_stage_idx
    ON recommendation_trace_candidate_plans (selected_stage, created_at DESC, recommendation_trace_id DESC);

CREATE TABLE IF NOT EXISTS recommendation_trace_candidate_plan_stages (
    id BIGSERIAL PRIMARY KEY,
    recommendation_trace_id BIGINT NOT NULL
        REFERENCES recommendation_trace_candidate_plans (recommendation_trace_id)
        ON DELETE CASCADE,
    stage_order INTEGER NOT NULL CHECK (stage_order >= 0),
    stage TEXT NOT NULL CHECK (btrim(stage) <> ''),
    candidate_count BIGINT NOT NULL CHECK (candidate_count >= 0),
    required_min_candidates BIGINT NOT NULL CHECK (required_min_candidates >= 0),
    status TEXT NOT NULL CHECK (status IN ('selected', 'insufficient', 'skipped')),
    reason_code TEXT NOT NULL CHECK (btrim(reason_code) <> ''),
    stage_payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(stage_payload) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (recommendation_trace_id, stage_order)
);

CREATE INDEX IF NOT EXISTS recommendation_trace_candidate_plan_stages_lookup_idx
    ON recommendation_trace_candidate_plan_stages (recommendation_trace_id, stage_order);
