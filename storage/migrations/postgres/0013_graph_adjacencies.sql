CREATE TABLE IF NOT EXISTS area_adjacencies (
    id BIGSERIAL PRIMARY KEY,
    from_area_id TEXT NOT NULL
        REFERENCES areas(area_id) ON DELETE CASCADE
        CHECK (btrim(from_area_id) <> ''),
    to_area_id TEXT NOT NULL
        REFERENCES areas(area_id) ON DELETE CASCADE
        CHECK (btrim(to_area_id) <> ''),
    adjacency_kind TEXT NOT NULL CHECK (btrim(adjacency_kind) <> ''),
    distance_meters DOUBLE PRECISION CHECK (distance_meters IS NULL OR distance_meters >= 0),
    area_cluster_id TEXT CHECK (area_cluster_id IS NULL OR btrim(area_cluster_id) <> ''),
    source_id TEXT CHECK (source_id IS NULL OR btrim(source_id) <> ''),
    source_version TEXT CHECK (source_version IS NULL OR btrim(source_version) <> ''),
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(attributes) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (btrim(from_area_id) <> btrim(to_area_id))
);

CREATE UNIQUE INDEX IF NOT EXISTS area_adjacencies_unique_edge_idx
    ON area_adjacencies (from_area_id, to_area_id, adjacency_kind);

CREATE INDEX IF NOT EXISTS area_adjacencies_lookup_idx
    ON area_adjacencies (from_area_id, adjacency_kind, to_area_id);

CREATE INDEX IF NOT EXISTS area_adjacencies_reverse_lookup_idx
    ON area_adjacencies (to_area_id, adjacency_kind, from_area_id);

CREATE INDEX IF NOT EXISTS area_adjacencies_cluster_idx
    ON area_adjacencies (area_cluster_id, from_area_id, to_area_id)
    WHERE area_cluster_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS line_adjacencies (
    id BIGSERIAL PRIMARY KEY,
    from_line_id TEXT NOT NULL
        REFERENCES lines(line_id) ON DELETE CASCADE
        CHECK (btrim(from_line_id) <> ''),
    to_line_id TEXT NOT NULL
        REFERENCES lines(line_id) ON DELETE CASCADE
        CHECK (btrim(to_line_id) <> ''),
    adjacency_kind TEXT NOT NULL CHECK (btrim(adjacency_kind) <> ''),
    interchange_station_id TEXT
        REFERENCES stations(id) ON DELETE CASCADE
        CHECK (interchange_station_id IS NULL OR btrim(interchange_station_id) <> ''),
    station_hop_count INTEGER CHECK (station_hop_count IS NULL OR station_hop_count >= 0),
    requires_transfer BOOLEAN NOT NULL DEFAULT TRUE,
    source_id TEXT CHECK (source_id IS NULL OR btrim(source_id) <> ''),
    source_version TEXT CHECK (source_version IS NULL OR btrim(source_version) <> ''),
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(attributes) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (btrim(from_line_id) <> btrim(to_line_id))
);

CREATE UNIQUE INDEX IF NOT EXISTS line_adjacencies_unique_edge_idx
    ON line_adjacencies (
        from_line_id,
        to_line_id,
        adjacency_kind,
        COALESCE(interchange_station_id, '')
    );

CREATE INDEX IF NOT EXISTS line_adjacencies_lookup_idx
    ON line_adjacencies (from_line_id, adjacency_kind, to_line_id);

CREATE INDEX IF NOT EXISTS line_adjacencies_reverse_lookup_idx
    ON line_adjacencies (to_line_id, adjacency_kind, from_line_id);

CREATE INDEX IF NOT EXISTS line_adjacencies_interchange_station_idx
    ON line_adjacencies (interchange_station_id, from_line_id, to_line_id)
    WHERE interchange_station_id IS NOT NULL;
