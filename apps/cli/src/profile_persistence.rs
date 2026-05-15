use std::path::Path;

use anyhow::{Context, Result};
use config::{ProfilePackLintFile, ProfilePackManifest};
use sha2::{Digest, Sha256};
use storage::ProfileManifestRecord;

pub fn build_profile_manifest_record(
    manifest: &ProfilePackManifest,
    lint_file: &ProfilePackLintFile,
) -> Result<ProfileManifestRecord> {
    let raw = std::fs::read(&lint_file.path)
        .with_context(|| format!("failed to read profile pack {}", lint_file.path.display()))?;
    let checksum = format!("{:x}", Sha256::digest(&raw));
    Ok(ProfileManifestRecord {
        profile_id: manifest.profile_id.clone(),
        display_name: manifest.display_name.clone(),
        schema_version: manifest.schema_version.try_into()?,
        manifest_kind: manifest.kind.as_str().to_string(),
        manifest_version: manifest.manifest_version.try_into()?,
        compatibility_level: manifest.compatibility_level.as_str().to_string(),
        default_locale: manifest.default_locale.clone(),
        description: manifest.description.clone(),
        manifest_path: canonicalize_profile_registry_path("profile pack", &lint_file.path)?,
        manifest_checksum_sha256: checksum,
        manifest_payload: serde_json::to_value(manifest)?,
        ranking_config_dir: canonicalize_profile_registry_path(
            "ranking config dir",
            &lint_file.ranking_config_dir,
        )?,
        reason_catalog_path: canonicalize_profile_registry_path(
            "reason catalog",
            &lint_file.reason_catalog_path,
        )?,
        content_kind_registry: lint_file
            .content_kind_registry
            .iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        supported_content_kinds: lint_file
            .supported_content_kinds
            .iter()
            .map(|kind| kind.as_str().to_string())
            .collect(),
        context_inputs: manifest
            .context_inputs
            .iter()
            .map(|input| input.as_str().to_string())
            .collect(),
        placements: lint_file
            .placements
            .iter()
            .map(|placement| placement.as_str().to_string())
            .collect(),
        fallback_policy: manifest.fallback_policy.display(),
        fixture_count: usize_to_i32("fixture_count", lint_file.fixture_count)?,
        connector_count: usize_to_i32("connector_count", lint_file.connector_count)?,
        evaluation_reference_count: usize_to_i32(
            "evaluation_reference_count",
            lint_file.evaluation_reference_count,
        )?,
    })
}

fn canonicalize_profile_registry_path(field: &str, path: &Path) -> Result<String> {
    Ok(path
        .canonicalize()
        .with_context(|| {
            format!(
                "failed to canonicalize profile registry {field} {}",
                path.display()
            )
        })?
        .display()
        .to_string())
}

fn usize_to_i32(field: &str, value: usize) -> Result<i32> {
    value
        .try_into()
        .with_context(|| format!("{field} is too large for storage"))
}
