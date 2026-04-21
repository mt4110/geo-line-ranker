ALTER TABLE popularity_snapshots
    ADD COLUMN IF NOT EXISTS search_execute_count BIGINT NOT NULL DEFAULT 0;

ALTER TABLE area_affinity_snapshots
    ADD COLUMN IF NOT EXISTS search_execute_count BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS user_events_search_execute_station_idx
    ON user_events (target_station_id, occurred_at DESC)
    WHERE event_type = 'search_execute'
      AND target_station_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS school_station_links_station_school_idx
    ON school_station_links (station_id, school_id);
