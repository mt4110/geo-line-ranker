use anyhow::Result;
use config::AppSettings;
use generic_csv::PreparedSourceFile;
use serde_json::json;
use storage_postgres::{
    record_import_report, upsert_import_run_file, ImportReportEntry, ImportRunFileAudit,
    ImportSummary, PgRepository,
};

pub(super) fn pg_repository(settings: &AppSettings) -> Result<PgRepository> {
    PgRepository::with_pool_max_size(
        settings.database_url.clone(),
        settings.postgres_pool_max_size,
    )
}

pub(super) async fn register_staged_files(
    settings: &AppSettings,
    import_run_id: i64,
    prepared_files: &[PreparedSourceFile],
) -> Result<()> {
    for file in prepared_files {
        upsert_import_run_file(
            &settings.database_url,
            &ImportRunFileAudit {
                import_run_id,
                logical_name: file.logical_name.clone(),
                staged_path: file.staged_path.display().to_string(),
                checksum_sha256: file.checksum_sha256.clone(),
                size_bytes: file.size_bytes as i64,
                row_count: None,
                status: "staged".to_string(),
            },
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn update_file_row_counts(
    settings: &AppSettings,
    import_run_id: i64,
    prepared_files: &[PreparedSourceFile],
    row_counts: &[(&str, i64)],
) -> Result<()> {
    for file in prepared_files {
        let row_count = row_counts.iter().find_map(|(logical_name, count)| {
            (*logical_name == file.logical_name).then_some(*count)
        });
        upsert_import_run_file(
            &settings.database_url,
            &ImportRunFileAudit {
                import_run_id,
                logical_name: file.logical_name.clone(),
                staged_path: file.staged_path.display().to_string(),
                checksum_sha256: file.checksum_sha256.clone(),
                size_bytes: file.size_bytes as i64,
                row_count,
                status: "imported".to_string(),
            },
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn persist_success_reports(
    database_url: &str,
    import_run_id: i64,
    parser_version: &str,
    summary: &ImportSummary,
) -> Result<()> {
    record_import_report(
        database_url,
        import_run_id,
        &ImportReportEntry {
            level: "info".to_string(),
            code: "parser_version".to_string(),
            message: "Recorded parser version for this import run.".to_string(),
            row_count: None,
            details: json!({ "parser_version": parser_version }),
        },
    )
    .await?;

    for report in &summary.report_entries {
        record_import_report(database_url, import_run_id, report).await?;
    }
    Ok(())
}
