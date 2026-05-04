use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use config::{is_profile_id, PROFILE_ID_RULE_DESCRIPTION};
use csv::Reader;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const FIXTURE_SET_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FixtureManifestKind {
    FixtureSet,
}

impl FixtureManifestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FixtureSet => "fixture_set",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FixtureSetManifest {
    pub schema_version: u32,
    pub kind: FixtureManifestKind,
    pub manifest_version: u32,
    pub fixture_set_id: String,
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub files: Vec<FixtureFileManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FixtureFileManifest {
    pub logical_name: String,
    pub path: String,
    pub format: String,
    pub checksum_sha256: String,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureDoctorSummary {
    pub manifest_path: PathBuf,
    pub fixture_set_id: String,
    pub profile_id: Option<String>,
    pub manifest_version: u32,
    pub files: Vec<FixtureDoctorFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureDoctorFile {
    pub logical_name: String,
    pub path: PathBuf,
    pub format: String,
    pub checksum_sha256: String,
    pub row_count: u64,
}

pub fn run_fixture_doctor(path: impl AsRef<Path>) -> Result<FixtureDoctorSummary> {
    let manifest_path = resolve_fixture_manifest_path(path.as_ref());
    let manifest_dir = parent_or_current_dir(&manifest_path);
    let canonical_manifest_dir = manifest_dir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize fixture directory {}",
            manifest_dir.display()
        )
    })?;
    let raw = fs::read_to_string(&manifest_path).with_context(|| {
        format!(
            "failed to read fixture manifest {}",
            manifest_path.display()
        )
    })?;
    let manifest: FixtureSetManifest = serde_yaml::from_str(&raw).with_context(|| {
        format!(
            "failed to parse fixture manifest {}",
            manifest_path.display()
        )
    })?;

    ensure!(
        manifest.schema_version == FIXTURE_SET_SCHEMA_VERSION,
        "fixture manifest {} schema_version {} is unsupported; expected {}",
        manifest_path.display(),
        manifest.schema_version,
        FIXTURE_SET_SCHEMA_VERSION
    );
    ensure!(
        manifest.kind == FixtureManifestKind::FixtureSet,
        "fixture manifest {} kind {} is invalid; expected {}",
        manifest_path.display(),
        manifest.kind.as_str(),
        FixtureManifestKind::FixtureSet.as_str()
    );
    ensure!(
        !manifest.fixture_set_id.trim().is_empty(),
        "fixture manifest {} is missing fixture_set_id",
        manifest_path.display()
    );
    if let Some(profile_id) = manifest.profile_id.as_deref() {
        ensure!(
            is_profile_id(profile_id),
            "fixture manifest {} invalid profile_id '{}': {}",
            manifest_path.display(),
            profile_id,
            PROFILE_ID_RULE_DESCRIPTION
        );
    }
    ensure!(
        !manifest.files.is_empty(),
        "fixture manifest {} does not list any files",
        manifest_path.display()
    );

    let mut seen_logical_names = BTreeSet::new();
    let mut seen_paths = BTreeSet::new();
    let mut files = Vec::new();
    for file in &manifest.files {
        ensure!(
            !file.logical_name.trim().is_empty(),
            "fixture manifest {} contains a file with empty logical_name",
            manifest_path.display()
        );
        ensure!(
            seen_logical_names.insert(file.logical_name.clone()),
            "fixture manifest {} contains duplicate logical_name {}",
            manifest_path.display(),
            file.logical_name
        );
        ensure!(
            !file.path.trim().is_empty(),
            "fixture manifest {} file {} has an empty path",
            manifest_path.display(),
            file.logical_name
        );
        let normalized_path =
            normalize_fixture_manifest_path(&manifest_path, &file.logical_name, &file.path)?;
        let normalized_path_key = manifest_path_value(&normalized_path);
        ensure!(
            seen_paths.insert(normalized_path_key.clone()),
            "fixture manifest {} contains duplicate path {}",
            manifest_path.display(),
            normalized_path_key
        );
        ensure!(
            matches!(file.format.as_str(), "csv" | "ndjson"),
            "fixture manifest {} file {} uses unsupported format {}; expected csv or ndjson",
            manifest_path.display(),
            file.logical_name,
            file.format
        );

        let fixture_path = manifest_dir.join(&normalized_path);
        ensure!(
            fixture_path.is_file(),
            "fixture manifest {} file {} points to missing fixture file {}",
            manifest_path.display(),
            file.logical_name,
            fixture_path.display()
        );
        let canonical_fixture_path = fixture_path.canonicalize().with_context(|| {
            format!(
                "failed to canonicalize fixture manifest {} file {} path {}",
                manifest_path.display(),
                file.logical_name,
                fixture_path.display()
            )
        })?;
        ensure!(
            canonical_fixture_path.starts_with(&canonical_manifest_dir),
            "fixture manifest {} file {} path {} must stay inside fixture directory {}",
            manifest_path.display(),
            file.logical_name,
            fixture_path.display(),
            canonical_manifest_dir.display()
        );
        let checksum_sha256 = checksum_file(&canonical_fixture_path)?;
        ensure!(
            checksum_sha256 == file.checksum_sha256,
            "fixture manifest {} file {} checksum mismatch: expected {}, got {}",
            manifest_path.display(),
            file.logical_name,
            file.checksum_sha256,
            checksum_sha256
        );
        let row_count = count_fixture_rows(&canonical_fixture_path, &file.format)?;
        ensure!(
            row_count == file.row_count,
            "fixture manifest {} file {} row_count mismatch: expected {}, got {}",
            manifest_path.display(),
            file.logical_name,
            file.row_count,
            row_count
        );
        files.push(FixtureDoctorFile {
            logical_name: file.logical_name.clone(),
            path: fixture_path,
            format: file.format.clone(),
            checksum_sha256,
            row_count,
        });
    }

    Ok(FixtureDoctorSummary {
        manifest_path,
        fixture_set_id: manifest.fixture_set_id,
        profile_id: manifest.profile_id,
        manifest_version: manifest.manifest_version,
        files,
    })
}

pub fn generate_demo_jp_fixture(output_dir: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let files = vec![
        (
            "school_codes",
            output_dir.join("jp_school_codes.csv"),
            "school_code,name,prefecture_name,city_name,school_type\n13101A,Minato Science High,Tokyo,Minato,high_school\n13101B,Harbor Commerce High,Tokyo,Minato,high_school\n13103A,Shinagawa Technical College,Tokyo,Shinagawa,college\n",
        ),
        (
            "school_geodata",
            output_dir.join("jp_school_geodata.csv"),
            "school_code,name,prefecture_name,city_name,address,school_type,latitude,longitude\n13101A,Minato Science High,Tokyo,Minato,芝浦1-1-1,high_school,35.6412,139.7487\n13101B,Harbor Commerce High,Tokyo,Minato,海岸1-2-3,high_school,35.6376,139.7604\n13103A,Shinagawa Technical College,Tokyo,Shinagawa,港南2-16-1,college,35.6289,139.7393\n",
        ),
        (
            "rail_stations",
            output_dir.join("jp_rail_stations.csv"),
            "station_code,station_name,line_name,prefecture_name,latitude,longitude\n1130217,Tamachi,JR Yamanote Line,Tokyo,35.6456,139.7476\n1130218,Shinagawa,JR Yamanote Line,Tokyo,35.6285,139.7388\n1130104,Shimbashi,JR Yamanote Line,Tokyo,35.6663,139.7587\n",
        ),
        (
            "postal_codes",
            output_dir.join("jp_postal_codes.csv"),
            "postal_code,prefecture_name,city_name,town_name\n1080023,Tokyo,Minato,Shibaura\n1050022,Tokyo,Minato,Kaigan\n1080075,Tokyo,Minato,Konan\n",
        ),
    ];

    let mut written = Vec::new();
    let mut manifest_files = Vec::new();
    for (logical_name, path, contents) in files {
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        manifest_files.push(fixture_file_manifest(logical_name, &path, "csv")?);
        written.push(path);
    }
    let manifest_path = output_dir.join("fixture_manifest.yaml");
    write_fixture_manifest(
        &manifest_path,
        "demo_jp",
        Some("school-event-jp"),
        "Small JP adapter fixture set for deterministic import smoke tests.",
        manifest_files,
    )?;
    written.push(manifest_path);
    Ok(written)
}

fn parent_or_current_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn normalize_fixture_manifest_path(
    manifest_path: &Path,
    logical_name: &str,
    raw_path: &str,
) -> Result<PathBuf> {
    ensure!(
        !raw_path.contains('\\') && !has_windows_drive_prefix(raw_path),
        "fixture manifest {} file {} path must use portable POSIX relative syntax",
        manifest_path.display(),
        logical_name
    );
    let path = Path::new(raw_path);
    ensure!(
        !path.is_absolute(),
        "fixture manifest {} file {} path must be relative",
        manifest_path.display(),
        logical_name
    );

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir => {
                anyhow::bail!(
                    "fixture manifest {} file {} path must stay inside the fixture directory",
                    manifest_path.display(),
                    logical_name
                );
            }
            Component::Prefix(_) | Component::RootDir => {
                anyhow::bail!(
                    "fixture manifest {} file {} path must be relative",
                    manifest_path.display(),
                    logical_name
                );
            }
        }
    }
    ensure!(
        !normalized.as_os_str().is_empty(),
        "fixture manifest {} file {} has an empty path",
        manifest_path.display(),
        logical_name
    );
    Ok(normalized)
}

fn has_windows_drive_prefix(raw_path: &str) -> bool {
    let bytes = raw_path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn manifest_path_value(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn resolve_fixture_manifest_path(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join("fixture_manifest.yaml")
    } else {
        path.to_path_buf()
    }
}

fn fixture_file_manifest(
    logical_name: &str,
    path: &Path,
    format: &str,
) -> Result<FixtureFileManifest> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("fixture file name is invalid for {}", path.display()))?;
    Ok(FixtureFileManifest {
        logical_name: logical_name.to_string(),
        path: file_name.to_string(),
        format: format.to_string(),
        checksum_sha256: checksum_file(path)?,
        row_count: count_fixture_rows(path, format)?,
    })
}

fn write_fixture_manifest(
    manifest_path: &Path,
    fixture_set_id: &str,
    profile_id: Option<&str>,
    description: &str,
    files: Vec<FixtureFileManifest>,
) -> Result<()> {
    let manifest = FixtureSetManifest {
        schema_version: FIXTURE_SET_SCHEMA_VERSION,
        kind: FixtureManifestKind::FixtureSet,
        manifest_version: 2,
        fixture_set_id: fixture_set_id.to_string(),
        profile_id: profile_id.map(str::to_string),
        description: Some(description.to_string()),
        files,
    };
    let raw = serde_yaml::to_string(&manifest)?;
    fs::write(manifest_path, raw)
        .with_context(|| format!("failed to write {}", manifest_path.display()))
}

fn checksum_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

fn count_fixture_rows(path: &Path, format: &str) -> Result<u64> {
    match format {
        "csv" => {
            let mut reader = Reader::from_path(path)
                .with_context(|| format!("failed to open fixture CSV {}", path.display()))?;
            let mut count = 0_u64;
            for row in reader.records() {
                row.with_context(|| format!("failed to parse {}", path.display()))?;
                count += 1;
            }
            Ok(count)
        }
        "ndjson" => {
            let raw = fs::read_to_string(path)
                .with_context(|| format!("failed to read fixture NDJSON {}", path.display()))?;
            Ok(raw.lines().filter(|line| !line.trim().is_empty()).count() as u64)
        }
        _ => anyhow::bail!("unsupported fixture format {format}"),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        checksum_file, generate_demo_jp_fixture, parent_or_current_dir, run_fixture_doctor,
    };

    #[test]
    fn writes_demo_fixture_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let written = generate_demo_jp_fixture(temp.path()).expect("fixture generation");
        assert_eq!(written.len(), 5);
        assert!(written.iter().all(|path| path.exists()));
        let summary = run_fixture_doctor(temp.path()).expect("fixture doctor");
        assert_eq!(summary.fixture_set_id, "demo_jp");
        assert_eq!(summary.profile_id.as_deref(), Some("school-event-jp"));
        assert_eq!(summary.files.len(), 4);
    }

    #[test]
    fn fixture_doctor_rejects_checksum_mismatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("data.csv"), "id,name\n1,Example\n").expect("fixture");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
files:
  - logical_name: data
    path: data.csv
    format: csv
    checksum_sha256: deadbeef
    row_count: 1
"#,
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("checksum mismatch");
        assert!(format!("{error:#}").contains("checksum mismatch"));
    }

    #[test]
    fn fixture_doctor_rejects_unknown_manifest_keys() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("data.csv"), "id,name\n1,Example\n").expect("fixture");
        let checksum = checksum_file(&temp.path().join("data.csv")).expect("checksum");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            format!(
                r#"
schema_version: 1
kind: fixture_set
manifest_version: 2
fixture_set_id: test
unknown_key: true
files:
  - logical_name: data
    path: data.csv
    format: csv
    checksum_sha256: {checksum}
    row_count: 1
"#
            ),
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("unknown key");
        assert!(format!("{error:#}").contains("unknown field `unknown_key`"));
    }

    #[test]
    fn fixture_doctor_rejects_unknown_file_manifest_keys() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("data.csv"), "id,name\n1,Example\n").expect("fixture");
        let checksum = checksum_file(&temp.path().join("data.csv")).expect("checksum");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            format!(
                r#"
schema_version: 1
kind: fixture_set
manifest_version: 2
fixture_set_id: test
files:
  - logical_name: data
    path: data.csv
    format: csv
    checksum_sha256: {checksum}
    row_count: 1
    unknown_file_key: true
"#
            ),
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("unknown key");
        assert!(format!("{error:#}").contains("unknown field `unknown_file_key`"));
    }

    #[test]
    fn fixture_doctor_invalid_profile_id_error_includes_value() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
profile_id: "school-event-jp "
files: []
"#,
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("invalid profile id");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("invalid profile_id 'school-event-jp '"));
        assert!(rendered.contains("must be non-empty and trimmed"));
    }

    #[test]
    fn fixture_doctor_rejects_duplicate_paths_after_normalization() {
        let temp = tempfile::tempdir().expect("tempdir");
        let fixture_path = temp.path().join("data.csv");
        std::fs::write(&fixture_path, "id,name\n1,Example\n").expect("fixture");
        let checksum = checksum_file(&fixture_path).expect("checksum");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            format!(
                r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
files:
  - logical_name: data_a
    path: data.csv
    format: csv
    checksum_sha256: {checksum}
    row_count: 1
  - logical_name: data_b
    path: ./data.csv
    format: csv
    checksum_sha256: {checksum}
    row_count: 1
"#
            ),
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("duplicate normalized path");
        assert!(format!("{error:#}").contains("duplicate path data.csv"));
    }

    #[test]
    fn fixture_doctor_rejects_windows_style_fixture_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join("fixture_manifest.yaml"),
            r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
files:
  - logical_name: data
    path: C:/fixtures/data.csv
    format: csv
    checksum_sha256: deadbeef
    row_count: 1
"#,
        )
        .expect("manifest");

        let error = run_fixture_doctor(temp.path()).expect_err("windows-style path");
        assert!(format!("{error:#}").contains("portable POSIX relative syntax"));
    }

    #[cfg(unix)]
    #[test]
    fn fixture_doctor_rejects_symlink_escape() {
        let temp = tempfile::tempdir().expect("tempdir");
        let fixture_dir = temp.path().join("fixtures");
        std::fs::create_dir_all(&fixture_dir).expect("fixture dir");
        let outside_path = temp.path().join("outside.csv");
        std::fs::write(&outside_path, "id,name\n1,Outside\n").expect("outside fixture");
        std::os::unix::fs::symlink(&outside_path, fixture_dir.join("data.csv")).expect("symlink");
        let checksum = checksum_file(&outside_path).expect("checksum");
        std::fs::write(
            fixture_dir.join("fixture_manifest.yaml"),
            format!(
                r#"
schema_version: 1
kind: fixture_set
manifest_version: 1
fixture_set_id: test
files:
  - logical_name: data
    path: data.csv
    format: csv
    checksum_sha256: {checksum}
    row_count: 1
"#
            ),
        )
        .expect("manifest");

        let error = run_fixture_doctor(&fixture_dir).expect_err("symlink escape");
        assert!(format!("{error:#}").contains("must stay inside fixture directory"));
    }

    #[test]
    fn parent_or_current_dir_treats_bare_manifest_filename_as_current_dir() {
        assert_eq!(
            parent_or_current_dir(Path::new("fixture_manifest.yaml")),
            Path::new(".")
        );
    }
}
