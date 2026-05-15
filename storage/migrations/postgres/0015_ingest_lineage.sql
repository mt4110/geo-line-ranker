ALTER TABLE import_runs
    ADD COLUMN IF NOT EXISTS profile_id TEXT,
    ADD COLUMN IF NOT EXISTS profile_manifest_lineage_id BIGINT,
    ADD COLUMN IF NOT EXISTS connector_type TEXT,
    ADD COLUMN IF NOT EXISTS source_class TEXT,
    ADD COLUMN IF NOT EXISTS manifest_kind TEXT,
    ADD COLUMN IF NOT EXISTS manifest_schema_version INTEGER,
    ADD COLUMN IF NOT EXISTS field_mapping TEXT,
    ADD COLUMN IF NOT EXISTS lineage_evidence JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE crawl_runs
    ADD COLUMN IF NOT EXISTS profile_id TEXT,
    ADD COLUMN IF NOT EXISTS profile_manifest_lineage_id BIGINT,
    ADD COLUMN IF NOT EXISTS connector_type TEXT,
    ADD COLUMN IF NOT EXISTS source_class TEXT,
    ADD COLUMN IF NOT EXISTS manifest_kind TEXT,
    ADD COLUMN IF NOT EXISTS manifest_schema_version INTEGER,
    ADD COLUMN IF NOT EXISTS field_mapping TEXT,
    ADD COLUMN IF NOT EXISTS lineage_evidence JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_profile_id_not_blank'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_profile_id_not_blank
            CHECK (profile_id IS NULL OR btrim(profile_id) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_profile_manifest_lineage_requires_profile_id'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_profile_manifest_lineage_requires_profile_id
            CHECK (profile_manifest_lineage_id IS NULL OR profile_id IS NOT NULL);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_connector_type_not_blank'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_connector_type_not_blank
            CHECK (connector_type IS NULL OR btrim(connector_type) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_source_class_not_blank'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_source_class_not_blank
            CHECK (source_class IS NULL OR btrim(source_class) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_manifest_kind_not_blank'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_manifest_kind_not_blank
            CHECK (manifest_kind IS NULL OR btrim(manifest_kind) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_manifest_schema_version_positive'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_manifest_schema_version_positive
            CHECK (manifest_schema_version IS NULL OR manifest_schema_version > 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_field_mapping_not_blank'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_field_mapping_not_blank
            CHECK (field_mapping IS NULL OR btrim(field_mapping) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_lineage_evidence_object'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_lineage_evidence_object
            CHECK (jsonb_typeof(lineage_evidence) = 'object');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_profile_id_fk'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_profile_id_fk
            FOREIGN KEY (profile_id)
            REFERENCES profile_registry (profile_id)
            ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'import_runs_profile_manifest_lineage_fk'
          AND conrelid = 'import_runs'::regclass
    ) THEN
        ALTER TABLE import_runs
            ADD CONSTRAINT import_runs_profile_manifest_lineage_fk
            FOREIGN KEY (profile_id, profile_manifest_lineage_id)
            REFERENCES profile_pack_manifest_lineage (profile_id, id)
            ON DELETE SET NULL (profile_manifest_lineage_id);
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_profile_id_not_blank'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_profile_id_not_blank
            CHECK (profile_id IS NULL OR btrim(profile_id) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_profile_manifest_lineage_requires_profile_id'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_profile_manifest_lineage_requires_profile_id
            CHECK (profile_manifest_lineage_id IS NULL OR profile_id IS NOT NULL);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_connector_type_not_blank'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_connector_type_not_blank
            CHECK (connector_type IS NULL OR btrim(connector_type) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_source_class_not_blank'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_source_class_not_blank
            CHECK (source_class IS NULL OR btrim(source_class) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_manifest_kind_not_blank'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_manifest_kind_not_blank
            CHECK (manifest_kind IS NULL OR btrim(manifest_kind) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_manifest_schema_version_positive'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_manifest_schema_version_positive
            CHECK (manifest_schema_version IS NULL OR manifest_schema_version > 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_field_mapping_not_blank'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_field_mapping_not_blank
            CHECK (field_mapping IS NULL OR btrim(field_mapping) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_lineage_evidence_object'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_lineage_evidence_object
            CHECK (jsonb_typeof(lineage_evidence) = 'object');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_profile_id_fk'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_profile_id_fk
            FOREIGN KEY (profile_id)
            REFERENCES profile_registry (profile_id)
            ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crawl_runs_profile_manifest_lineage_fk'
          AND conrelid = 'crawl_runs'::regclass
    ) THEN
        ALTER TABLE crawl_runs
            ADD CONSTRAINT crawl_runs_profile_manifest_lineage_fk
            FOREIGN KEY (profile_id, profile_manifest_lineage_id)
            REFERENCES profile_pack_manifest_lineage (profile_id, id)
            ON DELETE SET NULL (profile_manifest_lineage_id);
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS import_runs_profile_lineage_idx
    ON import_runs (profile_id, profile_manifest_lineage_id, started_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS import_runs_connector_lineage_idx
    ON import_runs (connector_type, source_class, manifest_kind, started_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS crawl_runs_profile_lineage_idx
    ON crawl_runs (profile_id, profile_manifest_lineage_id, started_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS crawl_runs_connector_lineage_idx
    ON crawl_runs (connector_type, source_class, manifest_kind, started_at DESC, id DESC);
