use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context, Result};
use csv::Reader;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceManifest {
    pub source_id: String,
    pub source_name: String,
    #[serde(default = "default_manifest_version")]
    pub manifest_version: u32,
    #[serde(default)]
    pub parser_version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub files: Vec<SourceFileSpec>,
}

impl SourceManifest {
    pub fn effective_parser_version(&self, default: &str) -> String {
        self.parser_version
            .clone()
            .unwrap_or_else(|| default.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceFileSpec {
    pub logical_name: String,
    pub path: String,
    #[serde(default = "default_format")]
    pub format: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSourceFile {
    pub logical_name: String,
    pub format: String,
    pub source_path: PathBuf,
    pub staged_path: PathBuf,
    pub checksum_sha256: String,
    pub size_bytes: u64,
}

pub fn load_manifest(path: impl AsRef<Path>) -> Result<SourceManifest> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read source manifest {}", path.display()))?;
    let manifest: SourceManifest = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse source manifest {}", path.display()))?;
    ensure!(
        !manifest.files.is_empty(),
        "source manifest {} does not list any files",
        path.display()
    );
    Ok(manifest)
}

pub fn stage_raw_files(
    manifest_path: impl AsRef<Path>,
    manifest: &SourceManifest,
    raw_root: impl AsRef<Path>,
) -> Result<Vec<PreparedSourceFile>> {
    let manifest_path = manifest_path.as_ref();
    let manifest_dir = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let raw_root = raw_root.as_ref();

    manifest
        .files
        .iter()
        .map(|file| {
            prepare_csv_file(
                &manifest.source_id,
                &file.logical_name,
                &file.format,
                manifest_dir.join(&file.path),
                raw_root,
            )
        })
        .collect()
}

pub fn stage_single_csv_file(
    source_id: &str,
    logical_name: &str,
    source_path: impl AsRef<Path>,
    raw_root: impl AsRef<Path>,
) -> Result<PreparedSourceFile> {
    prepare_csv_file(
        source_id,
        logical_name,
        "csv",
        source_path.as_ref(),
        raw_root.as_ref(),
    )
}

pub fn read_csv_rows<T: DeserializeOwned>(file: &PreparedSourceFile) -> Result<Vec<T>> {
    let mut reader = Reader::from_path(&file.staged_path)
        .with_context(|| format!("failed to open {}", file.staged_path.display()))?;
    let mut rows = Vec::new();
    for row in reader.deserialize() {
        rows.push(row.with_context(|| format!("failed to parse {}", file.staged_path.display()))?);
    }
    Ok(rows)
}

pub fn count_csv_rows(file: &PreparedSourceFile) -> Result<i64> {
    let mut reader = Reader::from_path(&file.staged_path)
        .with_context(|| format!("failed to open {}", file.staged_path.display()))?;
    let mut count = 0_i64;
    for row in reader.records() {
        row.with_context(|| format!("failed to parse {}", file.staged_path.display()))?;
        count += 1;
    }
    Ok(count)
}

fn prepare_csv_file(
    source_id: &str,
    logical_name: &str,
    format: &str,
    source_path: impl AsRef<Path>,
    raw_root: impl AsRef<Path>,
) -> Result<PreparedSourceFile> {
    if format != "csv" {
        bail!("unsupported format {format} for {logical_name}");
    }

    let source_path = fs::canonicalize(source_path.as_ref()).with_context(|| {
        format!(
            "failed to resolve source file {}",
            source_path.as_ref().display()
        )
    })?;
    let bytes = fs::read(&source_path)
        .with_context(|| format!("failed to read source file {}", source_path.display()))?;
    let checksum_sha256 = format!("{:x}", Sha256::digest(&bytes));
    let size_bytes = bytes.len() as u64;
    let file_name = source_path
        .file_name()
        .context("source file name is missing")?;
    let staged_dir = raw_root
        .as_ref()
        .join(source_id)
        .join(&checksum_sha256[..12]);
    fs::create_dir_all(&staged_dir)
        .with_context(|| format!("failed to create {}", staged_dir.display()))?;
    let staged_path = staged_dir.join(file_name);
    if !staged_path.exists() {
        fs::write(&staged_path, &bytes)
            .with_context(|| format!("failed to stage {}", staged_path.display()))?;
    }

    Ok(PreparedSourceFile {
        logical_name: logical_name.to_string(),
        format: format.to_string(),
        source_path,
        staged_path,
        checksum_sha256,
        size_bytes,
    })
}

fn default_manifest_version() -> u32 {
    1
}

fn default_format() -> String {
    "csv".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stages_raw_files_by_checksum() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_dir = temp.path().join("manifests");
        let fixture_dir = temp.path().join("fixtures");
        let raw_dir = temp.path().join(".storage").join("raw");
        fs::create_dir_all(&manifest_dir).expect("manifest dir");
        fs::create_dir_all(&fixture_dir).expect("fixture dir");
        fs::write(fixture_dir.join("demo.csv"), "id,name\n1,Example School\n").expect("fixture");
        fs::write(
            manifest_dir.join("example.yaml"),
            r#"
source_id: jp-school-codes
source_name: Demo school codes
files:
  - logical_name: school_codes
    path: ../fixtures/demo.csv
"#,
        )
        .expect("manifest");

        let manifest = load_manifest(manifest_dir.join("example.yaml")).expect("load manifest");
        let files =
            stage_raw_files(manifest_dir.join("example.yaml"), &manifest, &raw_dir).expect("stage");

        assert_eq!(files.len(), 1);
        assert!(files[0].staged_path.starts_with(&raw_dir));
        assert_eq!(files[0].logical_name, "school_codes");
        assert_eq!(files[0].size_bytes, 25);
    }
}
