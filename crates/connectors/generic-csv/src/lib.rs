use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};

use anyhow::{bail, ensure, Context, Result};
use csv::Reader;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SOURCE_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const ARCHIVE_SOURCE_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const ARCHIVE_MAX_ENTRY_UNPACK_BYTES: u64 = 64 * 1024 * 1024;
pub const ARCHIVE_MAX_TOTAL_UNPACK_BYTES: u64 = 256 * 1024 * 1024;
pub const SOURCE_ID_RULE_DESCRIPTION: &str = "must be non-empty and trimmed, use only lowercase letters, digits, and hyphens, and must not start or end with a hyphen";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceManifestKind {
    ImportSource,
}

impl SourceManifestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ImportSource => "import_source",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceManifest {
    pub schema_version: u32,
    pub kind: SourceManifestKind,
    pub source_id: String,
    pub source_name: String,
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
#[serde(deny_unknown_fields)]
pub struct SourceFileSpec {
    pub logical_name: String,
    pub path: String,
    #[serde(default = "default_format")]
    pub format: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveSourceManifestKind {
    ArchiveSource,
}

impl ArchiveSourceManifestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ArchiveSource => "archive_source",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveFormat {
    Zip,
    Tar,
    TarGz,
}

impl ArchiveFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zip => "zip",
            Self::Tar => "tar",
            Self::TarGz => "tar_gz",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArchiveSourceManifest {
    pub schema_version: u32,
    pub kind: ArchiveSourceManifestKind,
    pub source_id: String,
    pub source_name: String,
    pub manifest_version: u32,
    #[serde(default)]
    pub parser_version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub archive: ArchiveFileSpec,
    pub files: Vec<ArchiveEntrySpec>,
}

impl ArchiveSourceManifest {
    pub fn effective_parser_version(&self, default: &str) -> String {
        self.parser_version
            .clone()
            .unwrap_or_else(|| default.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArchiveFileSpec {
    pub path: String,
    pub format: ArchiveFormat,
    #[serde(default)]
    pub checksum_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArchiveEntrySpec {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceManifestLintFile {
    pub path: PathBuf,
    pub source_id: String,
    pub schema_version: u32,
    pub kind: SourceManifestKind,
    pub manifest_version: u32,
    pub file_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceManifestLintSummary {
    pub files: Vec<SourceManifestLintFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveSourceManifestLintFile {
    pub path: PathBuf,
    pub source_id: String,
    pub schema_version: u32,
    pub kind: ArchiveSourceManifestKind,
    pub manifest_version: u32,
    pub archive_format: ArchiveFormat,
    pub archive_path: PathBuf,
    pub archive_checksum_sha256: String,
    pub archive_size_bytes: u64,
    pub file_count: usize,
    pub files: Vec<ArchiveSourceManifestLintEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveSourceManifestLintEntry {
    pub logical_name: String,
    pub path: String,
    pub format: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnpackedArchiveSource {
    pub archive_path: PathBuf,
    pub archive_format: ArchiveFormat,
    pub archive_checksum_sha256: String,
    pub archive_size_bytes: u64,
    pub files: Vec<UnpackedArchiveFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnpackedArchiveFile {
    pub logical_name: String,
    pub format: String,
    pub archive_entry_path: String,
    pub source_path: PathBuf,
}

pub fn load_manifest(path: impl AsRef<Path>) -> Result<SourceManifest> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read source manifest {}", path.display()))?;
    let manifest: SourceManifest = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse source manifest {}", path.display()))?;
    ensure!(
        manifest.schema_version == SOURCE_MANIFEST_SCHEMA_VERSION,
        "source manifest {} schema_version {} is unsupported; expected {}",
        path.display(),
        manifest.schema_version,
        SOURCE_MANIFEST_SCHEMA_VERSION
    );
    ensure!(
        manifest.kind == SourceManifestKind::ImportSource,
        "source manifest {} kind {} is invalid; expected {}",
        path.display(),
        manifest.kind.as_str(),
        SourceManifestKind::ImportSource.as_str()
    );
    ensure!(
        is_source_id(&manifest.source_id),
        "source manifest {} source_id '{}' is invalid; {}",
        path.display(),
        manifest.source_id,
        SOURCE_ID_RULE_DESCRIPTION
    );
    ensure!(
        !manifest.files.is_empty(),
        "source manifest {} does not list any files",
        path.display()
    );
    let mut logical_names = std::collections::BTreeSet::new();
    for file in &manifest.files {
        ensure!(
            !file.logical_name.trim().is_empty(),
            "source manifest {} contains a file with empty logical_name",
            path.display()
        );
        ensure!(
            logical_names.insert(file.logical_name.clone()),
            "source manifest {} contains duplicate logical_name {}",
            path.display(),
            file.logical_name
        );
        ensure!(
            !file.path.trim().is_empty(),
            "source manifest {} file {} has an empty path",
            path.display(),
            file.logical_name
        );
        let source_path = Path::new(&file.path);
        ensure!(
            !file.path.contains('\\') && !has_windows_drive_prefix(&file.path),
            "source manifest {} file {} path must use portable POSIX relative syntax",
            path.display(),
            file.logical_name
        );
        ensure!(
            !source_path.is_absolute(),
            "source manifest {} file {} path must be relative",
            path.display(),
            file.logical_name
        );
        ensure!(
            !source_path.components().any(|component| {
                matches!(component, Component::Prefix(_) | Component::RootDir)
            }),
            "source manifest {} file {} path must be relative without a root or prefix",
            path.display(),
            file.logical_name
        );
        ensure!(
            file.format == "csv",
            "source manifest {} file {} uses unsupported format {}; expected csv",
            path.display(),
            file.logical_name,
            file.format
        );
    }
    Ok(manifest)
}

pub fn load_archive_manifest(path: impl AsRef<Path>) -> Result<ArchiveSourceManifest> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read archive source manifest {}", path.display()))?;
    let manifest: ArchiveSourceManifest = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse archive source manifest {}", path.display()))?;
    validate_archive_manifest(path, &manifest)?;
    Ok(manifest)
}

pub fn lint_archive_manifest_file(path: impl AsRef<Path>) -> Result<ArchiveSourceManifestLintFile> {
    let path = path.as_ref();
    let manifest = load_archive_manifest(path)?;
    let archive_path = resolve_archive_source_path(path, &manifest.archive.path)?;
    ensure!(
        archive_path.is_file(),
        "archive source manifest {} archive {} is missing or not a file",
        path.display(),
        archive_path.display()
    );
    let (archive_checksum_sha256, archive_size_bytes) = file_checksum_and_size(&archive_path)?;
    validate_archive_checksum(
        path,
        &archive_path,
        manifest.archive.checksum_sha256.as_deref(),
        &archive_checksum_sha256,
    )?;
    ensure_archive_entries_exist(path, &manifest, &archive_path)?;
    Ok(ArchiveSourceManifestLintFile {
        path: path.to_path_buf(),
        source_id: manifest.source_id,
        schema_version: manifest.schema_version,
        kind: manifest.kind,
        manifest_version: manifest.manifest_version,
        archive_format: manifest.archive.format,
        archive_path,
        archive_checksum_sha256,
        archive_size_bytes,
        file_count: manifest.files.len(),
        files: manifest
            .files
            .into_iter()
            .map(|file| ArchiveSourceManifestLintEntry {
                logical_name: file.logical_name,
                path: file.path,
                format: file.format,
            })
            .collect(),
    })
}

pub fn unpack_archive_manifest(
    manifest_path: impl AsRef<Path>,
    destination_dir: impl AsRef<Path>,
) -> Result<UnpackedArchiveSource> {
    let manifest_path = manifest_path.as_ref();
    let manifest = load_archive_manifest(manifest_path)?;
    unpack_loaded_archive_manifest(manifest_path, &manifest, destination_dir)
}

pub fn unpack_loaded_archive_manifest(
    manifest_path: &Path,
    manifest: &ArchiveSourceManifest,
    destination_dir: impl AsRef<Path>,
) -> Result<UnpackedArchiveSource> {
    validate_archive_manifest(manifest_path, manifest)?;
    let archive_path = resolve_archive_source_path(manifest_path, &manifest.archive.path)?;
    ensure!(
        archive_path.is_file(),
        "archive source manifest {} archive {} is missing or not a file",
        manifest_path.display(),
        archive_path.display()
    );
    let (archive_checksum_sha256, archive_size_bytes) = file_checksum_and_size(&archive_path)?;
    validate_archive_checksum(
        manifest_path,
        &archive_path,
        manifest.archive.checksum_sha256.as_deref(),
        &archive_checksum_sha256,
    )?;
    ensure_archive_entries_exist(manifest_path, manifest, &archive_path)?;

    let destination_dir = destination_dir.as_ref();
    fs::create_dir_all(destination_dir).with_context(|| {
        format!(
            "failed to create archive unpack destination {}",
            destination_dir.display()
        )
    })?;
    let files = match manifest.archive.format {
        ArchiveFormat::Zip => {
            unpack_zip_archive(manifest_path, manifest, &archive_path, destination_dir)?
        }
        ArchiveFormat::Tar => {
            unpack_tar_archive(manifest_path, manifest, &archive_path, destination_dir)?
        }
        ArchiveFormat::TarGz => {
            unpack_tar_gz_archive(manifest_path, manifest, &archive_path, destination_dir)?
        }
    };

    Ok(UnpackedArchiveSource {
        archive_path,
        archive_format: manifest.archive.format,
        archive_checksum_sha256,
        archive_size_bytes,
        files,
    })
}

fn has_windows_drive_prefix(raw_path: &str) -> bool {
    let bytes = raw_path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn validate_archive_manifest(path: &Path, manifest: &ArchiveSourceManifest) -> Result<()> {
    ensure!(
        manifest.schema_version == ARCHIVE_SOURCE_MANIFEST_SCHEMA_VERSION,
        "archive source manifest {} schema_version {} is unsupported; expected {}",
        path.display(),
        manifest.schema_version,
        ARCHIVE_SOURCE_MANIFEST_SCHEMA_VERSION
    );
    ensure!(
        manifest.kind == ArchiveSourceManifestKind::ArchiveSource,
        "archive source manifest {} kind {} is invalid; expected {}",
        path.display(),
        manifest.kind.as_str(),
        ArchiveSourceManifestKind::ArchiveSource.as_str()
    );
    ensure!(
        is_source_id(&manifest.source_id),
        "archive source manifest {} source_id '{}' is invalid; {}",
        path.display(),
        manifest.source_id,
        SOURCE_ID_RULE_DESCRIPTION
    );
    ensure!(
        !manifest.source_name.trim().is_empty(),
        "archive source manifest {} source_name must not be empty",
        path.display()
    );
    validate_archive_relative_path(path, "archive.path", &manifest.archive.path, false)?;
    let checksum = manifest
        .archive
        .checksum_sha256
        .as_deref()
        .with_context(|| {
            format!(
                "archive source manifest {} archive checksum_sha256 is required",
                path.display()
            )
        })?;
    ensure!(
        is_sha256_hex(checksum),
        "archive source manifest {} archive checksum_sha256 '{}' is invalid; expected 64 lowercase hex characters",
        path.display(),
        checksum
    );
    ensure!(
        !manifest.files.is_empty(),
        "archive source manifest {} does not list any files",
        path.display()
    );
    let mut logical_names = BTreeSet::new();
    let mut entry_paths = BTreeSet::new();
    for file in &manifest.files {
        ensure!(
            !file.logical_name.trim().is_empty(),
            "archive source manifest {} contains a file with empty logical_name",
            path.display()
        );
        ensure!(
            is_archive_logical_name(&file.logical_name),
            "archive source manifest {} file logical_name '{}' is invalid; expected lowercase letters, digits, underscores, or hyphens, starting and ending with a letter or digit",
            path.display(),
            file.logical_name
        );
        ensure!(
            logical_names.insert(file.logical_name.clone()),
            "archive source manifest {} contains duplicate logical_name {}",
            path.display(),
            file.logical_name
        );
        validate_archive_relative_path(path, "files.path", &file.path, false)?;
        ensure!(
            entry_paths.insert(file.path.clone()),
            "archive source manifest {} contains duplicate archive entry path {}",
            path.display(),
            file.path
        );
        ensure!(
            matches!(file.format.as_str(), "csv" | "ndjson"),
            "archive source manifest {} file {} uses unsupported format {}; expected csv or ndjson",
            path.display(),
            file.logical_name,
            file.format
        );
    }
    Ok(())
}

fn validate_archive_relative_path(
    manifest_path: &Path,
    field: &str,
    raw_path: &str,
    allow_parent_dirs: bool,
) -> Result<()> {
    ensure!(
        !raw_path.trim().is_empty() && raw_path.trim() == raw_path,
        "archive source manifest {} {} must be non-empty and trimmed",
        manifest_path.display(),
        field
    );
    ensure!(
        !raw_path.contains('\\')
            && !has_windows_drive_prefix(raw_path)
            && !raw_path.contains("://"),
        "archive source manifest {} {} must use local portable POSIX relative syntax",
        manifest_path.display(),
        field
    );
    let source_path = Path::new(raw_path);
    ensure!(
        !source_path.is_absolute(),
        "archive source manifest {} {} must be relative",
        manifest_path.display(),
        field
    );
    ensure!(
        !source_path
            .components()
            .any(|component| { matches!(component, Component::Prefix(_) | Component::RootDir) }),
        "archive source manifest {} {} must be relative without a root or prefix",
        manifest_path.display(),
        field
    );
    ensure!(
        allow_parent_dirs
            || !source_path
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::CurDir)),
        "archive source manifest {} {} must not contain . or .. components",
        manifest_path.display(),
        field
    );
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn is_archive_logical_name(value: &str) -> bool {
    if value.is_empty() || value.trim() != value {
        return false;
    }
    let bytes = value.as_bytes();
    let Some(first) = bytes.first() else {
        return false;
    };
    let Some(last) = bytes.last() else {
        return false;
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return false;
    }
    if !last.is_ascii_lowercase() && !last.is_ascii_digit() {
        return false;
    }
    bytes.iter().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
    })
}

fn resolve_archive_source_path(manifest_path: &Path, archive_path: &str) -> Result<PathBuf> {
    let manifest_dir = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let resolved = manifest_dir.join(archive_path);
    fs::canonicalize(&resolved).with_context(|| {
        format!(
            "failed to resolve archive source manifest {} archive {}",
            manifest_path.display(),
            resolved.display()
        )
    })
}

fn file_checksum_and_size(path: &Path) -> Result<(String, u64)> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut reader = io::BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut buffer = [0_u8; 8192];

    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size_bytes += read as u64;
    }

    Ok((format!("{:x}", hasher.finalize()), size_bytes))
}

fn validate_archive_checksum(
    manifest_path: &Path,
    archive_path: &Path,
    declared_checksum: Option<&str>,
    actual_checksum: &str,
) -> Result<()> {
    let declared_checksum = declared_checksum.with_context(|| {
        format!(
            "archive source manifest {} archive {} checksum_sha256 is required",
            manifest_path.display(),
            archive_path.display()
        )
    })?;
    ensure!(
        declared_checksum == actual_checksum,
        "archive source manifest {} archive {} checksum mismatch: declared {}, actual {}",
        manifest_path.display(),
        archive_path.display(),
        declared_checksum,
        actual_checksum
    );
    Ok(())
}

fn ensure_archive_entries_exist(
    manifest_path: &Path,
    manifest: &ArchiveSourceManifest,
    archive_path: &Path,
) -> Result<()> {
    let expected = manifest
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<BTreeSet<_>>();
    let present = archive_entry_paths(manifest_path, manifest.archive.format, archive_path)?;
    for path in expected {
        ensure!(
            present.contains(path),
            "archive source manifest {} archive {} is missing declared entry {}",
            manifest_path.display(),
            archive_path.display(),
            path
        );
    }
    Ok(())
}

fn archive_entry_paths(
    manifest_path: &Path,
    format: ArchiveFormat,
    archive_path: &Path,
) -> Result<BTreeSet<String>> {
    match format {
        ArchiveFormat::Zip => zip_entry_paths(manifest_path, archive_path),
        ArchiveFormat::Tar => tar_entry_paths(manifest_path, archive_path),
        ArchiveFormat::TarGz => tar_gz_entry_paths(manifest_path, archive_path),
    }
}

fn zip_entry_paths(manifest_path: &Path, archive_path: &Path) -> Result<BTreeSet<String>> {
    let file = fs::File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    let mut archive = zip::ZipArchive::new(file).with_context(|| {
        format!(
            "failed to read zip archive {} for manifest {}",
            archive_path.display(),
            manifest_path.display()
        )
    })?;
    let mut paths = BTreeSet::new();
    for index in 0..archive.len() {
        let entry = archive.by_index(index).with_context(|| {
            format!(
                "failed to read zip archive {} entry {}",
                archive_path.display(),
                index
            )
        })?;
        let entry_path = entry.name().to_string();
        validate_archive_relative_path(manifest_path, "archive entry path", &entry_path, false)?;
        if !entry.is_dir() {
            ensure_zip_entry_is_regular_file(
                manifest_path,
                archive_path,
                &entry_path,
                entry.is_file(),
            )?;
            insert_archive_entry_path(manifest_path, archive_path, &mut paths, &entry_path)?;
        }
    }
    Ok(paths)
}

fn ensure_zip_entry_is_regular_file(
    manifest_path: &Path,
    archive_path: &Path,
    entry_path: &str,
    is_file: bool,
) -> Result<()> {
    ensure!(
        is_file,
        "archive source manifest {} archive {} entry {} is not a regular file; archive import supports regular files only",
        manifest_path.display(),
        archive_path.display(),
        entry_path
    );
    Ok(())
}

fn tar_entry_paths(manifest_path: &Path, archive_path: &Path) -> Result<BTreeSet<String>> {
    let file = fs::File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    let mut archive = tar::Archive::new(file);
    collect_tar_entry_paths(manifest_path, archive_path, &mut archive)
}

fn tar_gz_entry_paths(manifest_path: &Path, archive_path: &Path) -> Result<BTreeSet<String>> {
    let file = fs::File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    collect_tar_entry_paths(manifest_path, archive_path, &mut archive)
}

fn collect_tar_entry_paths<R: io::Read>(
    manifest_path: &Path,
    archive_path: &Path,
    archive: &mut tar::Archive<R>,
) -> Result<BTreeSet<String>> {
    let mut paths = BTreeSet::new();
    for entry in archive.entries().with_context(|| {
        format!(
            "failed to read tar archive {} for manifest {}",
            archive_path.display(),
            manifest_path.display()
        )
    })? {
        let entry = entry
            .with_context(|| format!("failed to read tar archive {}", archive_path.display()))?;
        let entry_type = entry.header().entry_type();
        let entry_path = entry.path()?.to_string_lossy().into_owned();
        validate_archive_relative_path(manifest_path, "archive entry path", &entry_path, false)?;
        if entry_type.is_dir() {
            continue;
        }
        ensure_tar_entry_is_regular_file(manifest_path, archive_path, &entry_path, entry_type)?;
        insert_archive_entry_path(manifest_path, archive_path, &mut paths, &entry_path)?;
    }
    Ok(paths)
}

fn ensure_tar_entry_is_regular_file(
    manifest_path: &Path,
    archive_path: &Path,
    entry_path: &str,
    entry_type: tar::EntryType,
) -> Result<()> {
    ensure!(
        entry_type.is_file(),
        "archive source manifest {} archive {} entry {} is not a regular file; archive import supports regular files only",
        manifest_path.display(),
        archive_path.display(),
        entry_path
    );
    Ok(())
}

fn insert_archive_entry_path(
    manifest_path: &Path,
    archive_path: &Path,
    paths: &mut BTreeSet<String>,
    entry_path: &str,
) -> Result<()> {
    validate_archive_relative_path(manifest_path, "archive entry path", entry_path, false)?;
    ensure!(
        paths.insert(entry_path.to_string()),
        "archive source manifest {} archive {} contains duplicate entry path {}",
        manifest_path.display(),
        archive_path.display(),
        entry_path
    );
    Ok(())
}

fn unpack_zip_archive(
    manifest_path: &Path,
    manifest: &ArchiveSourceManifest,
    archive_path: &Path,
    destination_dir: &Path,
) -> Result<Vec<UnpackedArchiveFile>> {
    let file = fs::File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    let mut archive = zip::ZipArchive::new(file).with_context(|| {
        format!(
            "failed to read zip archive {} for manifest {}",
            archive_path.display(),
            manifest_path.display()
        )
    })?;
    let mut files = Vec::new();
    let mut total_unpacked_bytes = 0_u64;
    for spec in &manifest.files {
        let mut entry = archive.by_name(&spec.path).with_context(|| {
            format!(
                "archive source manifest {} archive {} is missing declared entry {}",
                manifest_path.display(),
                archive_path.display(),
                spec.path
            )
        })?;
        ensure!(
            !entry.is_dir(),
            "archive source manifest {} entry {} is a directory",
            manifest_path.display(),
            spec.path
        );
        ensure_zip_entry_is_regular_file(
            manifest_path,
            archive_path,
            entry.name(),
            entry.is_file(),
        )?;
        validate_archive_relative_path(manifest_path, "archive entry path", entry.name(), false)?;
        let target_path = archive_unpack_target(destination_dir, spec)?;
        let mut output = fs::File::create(&target_path)
            .with_context(|| format!("failed to create {}", target_path.display()))?;
        copy_archive_entry(
            manifest_path,
            archive_path,
            spec,
            &mut entry,
            &mut output,
            &mut total_unpacked_bytes,
        )?;
        files.push(UnpackedArchiveFile {
            logical_name: spec.logical_name.clone(),
            format: spec.format.clone(),
            archive_entry_path: spec.path.clone(),
            source_path: target_path,
        });
    }
    Ok(files)
}

fn unpack_tar_archive(
    manifest_path: &Path,
    manifest: &ArchiveSourceManifest,
    archive_path: &Path,
    destination_dir: &Path,
) -> Result<Vec<UnpackedArchiveFile>> {
    let file = fs::File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    let mut archive = tar::Archive::new(file);
    unpack_tar_entries(
        manifest_path,
        manifest,
        archive_path,
        destination_dir,
        &mut archive,
    )
}

fn unpack_tar_gz_archive(
    manifest_path: &Path,
    manifest: &ArchiveSourceManifest,
    archive_path: &Path,
    destination_dir: &Path,
) -> Result<Vec<UnpackedArchiveFile>> {
    let file = fs::File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    unpack_tar_entries(
        manifest_path,
        manifest,
        archive_path,
        destination_dir,
        &mut archive,
    )
}

fn unpack_tar_entries<R: io::Read>(
    manifest_path: &Path,
    manifest: &ArchiveSourceManifest,
    archive_path: &Path,
    destination_dir: &Path,
    archive: &mut tar::Archive<R>,
) -> Result<Vec<UnpackedArchiveFile>> {
    let requested = manifest
        .files
        .iter()
        .map(|file| (file.path.clone(), file))
        .collect::<BTreeMap<_, _>>();
    let mut files_by_path = BTreeMap::new();
    let mut total_unpacked_bytes = 0_u64;
    for entry in archive.entries().with_context(|| {
        format!(
            "failed to read tar archive {} for manifest {}",
            archive_path.display(),
            manifest_path.display()
        )
    })? {
        let mut entry = entry
            .with_context(|| format!("failed to read tar archive {}", archive_path.display()))?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            continue;
        }
        let entry_path = entry.path()?.to_string_lossy().into_owned();
        ensure_tar_entry_is_regular_file(manifest_path, archive_path, &entry_path, entry_type)?;
        let Some(spec) = requested.get(&entry_path) else {
            continue;
        };
        let target_path = archive_unpack_target(destination_dir, spec)?;
        let mut output = fs::File::create(&target_path)
            .with_context(|| format!("failed to create {}", target_path.display()))?;
        copy_archive_entry(
            manifest_path,
            archive_path,
            spec,
            &mut entry,
            &mut output,
            &mut total_unpacked_bytes,
        )?;
        files_by_path.insert(
            entry_path,
            UnpackedArchiveFile {
                logical_name: spec.logical_name.clone(),
                format: spec.format.clone(),
                archive_entry_path: spec.path.clone(),
                source_path: target_path,
            },
        );
    }

    let mut files = Vec::new();
    for spec in &manifest.files {
        let file = files_by_path.remove(&spec.path).with_context(|| {
            format!(
                "archive source manifest {} archive {} is missing declared entry {}",
                manifest_path.display(),
                archive_path.display(),
                spec.path
            )
        })?;
        files.push(file);
    }
    Ok(files)
}

fn copy_archive_entry<R: Read, W: Write>(
    manifest_path: &Path,
    archive_path: &Path,
    spec: &ArchiveEntrySpec,
    reader: &mut R,
    writer: &mut W,
    total_unpacked_bytes: &mut u64,
) -> Result<()> {
    let total_remaining = ARCHIVE_MAX_TOTAL_UNPACK_BYTES.saturating_sub(*total_unpacked_bytes);
    let copy_limit = ARCHIVE_MAX_ENTRY_UNPACK_BYTES.min(total_remaining);
    let copied = copy_with_limit(reader, writer, copy_limit).with_context(|| {
        format!(
            "failed to unpack archive source manifest {} archive {} entry {}",
            manifest_path.display(),
            archive_path.display(),
            spec.path
        )
    })?;
    *total_unpacked_bytes += copied;
    Ok(())
}

fn copy_with_limit<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    limit_bytes: u64,
) -> Result<u64> {
    let mut copied = 0_u64;
    let mut buffer = [0_u8; 8192];

    loop {
        let remaining = limit_bytes.saturating_sub(copied);
        let read_limit = if remaining == 0 {
            1
        } else {
            remaining.min(buffer.len() as u64) as usize
        };
        let read = reader.read(&mut buffer[..read_limit])?;
        if read == 0 {
            break;
        }
        copied += read as u64;
        ensure!(
            copied <= limit_bytes,
            "archive entry exceeds unpack size limits; max_entry_bytes={}, max_total_bytes={}",
            ARCHIVE_MAX_ENTRY_UNPACK_BYTES,
            ARCHIVE_MAX_TOTAL_UNPACK_BYTES
        );
        writer.write_all(&buffer[..read])?;
    }

    Ok(copied)
}

fn archive_unpack_target(destination_dir: &Path, spec: &ArchiveEntrySpec) -> Result<PathBuf> {
    let file_name = Path::new(&spec.path)
        .file_name()
        .with_context(|| format!("archive entry {} has no file name", spec.path))?;
    let target_dir = destination_dir.join(&spec.logical_name);
    fs::create_dir_all(&target_dir)
        .with_context(|| format!("failed to create {}", target_dir.display()))?;
    Ok(target_dir.join(file_name))
}

pub fn lint_source_manifest_file(path: impl AsRef<Path>) -> Result<SourceManifestLintFile> {
    let path = path.as_ref();
    let manifest = load_manifest(path)?;
    let manifest_dir = path.parent().unwrap_or_else(|| Path::new("."));
    for file in &manifest.files {
        let source_path = manifest_dir.join(&file.path);
        ensure!(
            source_path.is_file(),
            "source manifest {} file {} points to missing source file {}",
            path.display(),
            file.logical_name,
            source_path.display()
        );
    }
    Ok(SourceManifestLintFile {
        path: path.to_path_buf(),
        source_id: manifest.source_id,
        schema_version: manifest.schema_version,
        kind: manifest.kind,
        manifest_version: manifest.manifest_version,
        file_count: manifest.files.len(),
    })
}

pub fn lint_source_manifest_dir(path: impl AsRef<Path>) -> Result<SourceManifestLintSummary> {
    let path = path.as_ref();
    let mut files = Vec::new();
    for manifest_path in list_yaml_paths(path)? {
        files.push(lint_source_manifest_file(manifest_path)?);
    }
    if files.is_empty() {
        if path.is_file() {
            bail!("source manifest path {} is not a yaml file", path.display());
        }
        bail!(
            "source manifest path {} does not contain any yaml manifests",
            path.display()
        );
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(SourceManifestLintSummary { files })
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
            prepare_source_file(
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
    prepare_source_file(
        source_id,
        logical_name,
        "csv",
        source_path.as_ref(),
        raw_root.as_ref(),
    )
}

pub fn stage_single_source_file(
    source_id: &str,
    logical_name: &str,
    format: &str,
    source_path: impl AsRef<Path>,
    raw_root: impl AsRef<Path>,
) -> Result<PreparedSourceFile> {
    prepare_source_file(
        source_id,
        logical_name,
        format,
        source_path.as_ref(),
        raw_root.as_ref(),
    )
}

pub fn is_source_id(value: &str) -> bool {
    !value.is_empty()
        && value == value.trim()
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
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

fn prepare_source_file(
    source_id: &str,
    logical_name: &str,
    format: &str,
    source_path: impl AsRef<Path>,
    raw_root: impl AsRef<Path>,
) -> Result<PreparedSourceFile> {
    ensure!(
        is_source_id(source_id),
        "source_id '{}' is invalid; {}",
        source_id,
        SOURCE_ID_RULE_DESCRIPTION
    );
    if !matches!(format, "csv" | "ndjson") {
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

fn default_format() -> String {
    "csv".to_string()
}

fn list_yaml_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    collect_yaml_paths(path, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_yaml_paths(path: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if is_yaml_path(path) {
            paths.push(path.to_path_buf());
        }
        return Ok(());
    }

    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read source manifest dir {}", path.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry under {}", path.display()))?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_yaml_paths(&entry_path, paths)?;
        } else if is_yaml_path(&entry_path) {
            paths.push(entry_path);
        }
    }
    Ok(())
}

fn is_yaml_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("yaml" | "yml")
    )
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use sha2::{Digest, Sha256};

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
schema_version: 1
kind: import_source
source_id: jp-school-codes
source_name: Demo school codes
manifest_version: 1
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

    #[test]
    fn rejects_unknown_source_manifest_keys() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("example.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: import_source
source_id: jp-school-codes
source_name: Demo school codes
manifest_version: 1
unknown_key: true
files:
  - logical_name: school_codes
    path: demo.csv
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("unknown key");
        assert!(format!("{error:#}").contains("unknown field `unknown_key`"));
    }

    #[test]
    fn rejects_non_portable_source_id() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("example.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: import_source
source_id: ../bad-source
source_name: Demo school codes
manifest_version: 1
files:
  - logical_name: school_codes
    path: demo.csv
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("invalid source id");

        assert!(format!("{error:#}").contains("source_id '../bad-source' is invalid"));
    }

    #[test]
    fn rejects_missing_source_manifest_schema_contract() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("example.yaml");
        fs::write(
            &manifest_path,
            r#"
source_id: jp-school-codes
source_name: Demo school codes
files:
  - logical_name: school_codes
    path: demo.csv
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("missing schema contract");
        assert!(format!("{error:#}").contains("missing field `schema_version`"));
    }

    #[test]
    fn rejects_missing_source_manifest_kind() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("example.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
source_id: jp-school-codes
source_name: Demo school codes
manifest_version: 1
files:
  - logical_name: school_codes
    path: demo.csv
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("missing kind");
        assert!(format!("{error:#}").contains("missing field `kind`"));
    }

    #[test]
    fn rejects_missing_source_manifest_version() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("example.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: import_source
source_id: jp-school-codes
source_name: Demo school codes
files:
  - logical_name: school_codes
    path: demo.csv
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("missing manifest_version");
        assert!(format!("{error:#}").contains("missing field `manifest_version`"));
    }

    #[test]
    fn rejects_windows_style_source_manifest_file_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("example.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: import_source
source_id: jp-school-codes
source_name: Demo school codes
manifest_version: 1
files:
  - logical_name: school_codes
    path: C:/fixtures/demo.csv
"#,
        )
        .expect("manifest");

        let error = load_manifest(&manifest_path).expect_err("windows-style source path");
        assert!(format!("{error:#}").contains("portable POSIX relative syntax"));
    }

    #[test]
    fn lints_source_manifest_dir_recursively() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_dir = temp.path().join("sources").join("jp_school");
        fs::create_dir_all(&manifest_dir).expect("manifest dir");
        fs::write(manifest_dir.join("demo.csv"), "id,name\n1,Example\n").expect("fixture");
        fs::write(
            manifest_dir.join("example.yaml"),
            r#"
schema_version: 1
kind: import_source
source_id: jp-school-codes
source_name: Demo school codes
manifest_version: 1
files:
  - logical_name: school_codes
    path: demo.csv
"#,
        )
        .expect("manifest");

        let summary = lint_source_manifest_dir(temp.path().join("sources")).expect("lint");
        assert_eq!(summary.files.len(), 1);
        assert_eq!(summary.files[0].source_id, "jp-school-codes");
        assert_eq!(summary.files[0].file_count, 1);
    }

    #[test]
    fn lint_rejects_missing_source_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("example.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: import_source
source_id: jp-school-codes
source_name: Demo school codes
manifest_version: 1
files:
  - logical_name: school_codes
    path: missing.csv
"#,
        )
        .expect("manifest");

        let error = lint_source_manifest_file(&manifest_path).expect_err("missing source file");
        assert!(format!("{error:#}").contains("points to missing source file"));
    }

    #[test]
    fn lints_and_unpacks_tar_archive_manifest() {
        let temp = tempfile::tempdir().expect("tempdir");
        let archive_path = temp.path().join("events.tar");
        write_tar_archive(&archive_path, "events.csv", b"event_id,title\n1,Demo\n");
        let checksum = checksum(&archive_path);
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: events.tar
  format: tar
  checksum_sha256: {checksum}
files:
  - logical_name: events
    path: events.csv
    format: csv
"#
            ),
        )
        .expect("manifest");

        let lint = lint_archive_manifest_file(&manifest_path).expect("archive lint");
        assert_eq!(lint.source_id, "event-archive");
        assert_eq!(lint.archive_format, ArchiveFormat::Tar);
        assert_eq!(lint.file_count, 1);
        assert_eq!(lint.archive_checksum_sha256, checksum);
        assert_eq!(lint.files[0].path, "events.csv");

        let unpacked =
            unpack_archive_manifest(&manifest_path, temp.path().join("unpacked")).expect("unpack");
        assert_eq!(unpacked.archive_checksum_sha256, checksum);
        assert_eq!(unpacked.files.len(), 1);
        assert_eq!(unpacked.files[0].logical_name, "events");
        assert_eq!(unpacked.files[0].format, "csv");
        assert_eq!(unpacked.files[0].archive_entry_path, "events.csv");
        assert_eq!(
            fs::read_to_string(&unpacked.files[0].source_path).expect("unpacked csv"),
            "event_id,title\n1,Demo\n"
        );
    }

    #[test]
    fn lints_and_unpacks_zip_archive_manifest() {
        let temp = tempfile::tempdir().expect("tempdir");
        let archive_path = temp.path().join("events.zip");
        write_zip_archive(&archive_path, "events.ndjson", br#"{"event_id":"1"}"#);
        let checksum = checksum(&archive_path);
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: events.zip
  format: zip
  checksum_sha256: {checksum}
files:
  - logical_name: events
    path: events.ndjson
    format: ndjson
"#
            ),
        )
        .expect("manifest");

        let lint = lint_archive_manifest_file(&manifest_path).expect("archive lint");
        assert_eq!(lint.archive_format, ArchiveFormat::Zip);
        assert_eq!(lint.files[0].path, "events.ndjson");

        let unpacked =
            unpack_archive_manifest(&manifest_path, temp.path().join("unpacked")).expect("unpack");
        assert_eq!(unpacked.files[0].format, "ndjson");
        assert_eq!(unpacked.files[0].archive_entry_path, "events.ndjson");
        assert_eq!(
            fs::read_to_string(&unpacked.files[0].source_path).expect("unpacked ndjson"),
            r#"{"event_id":"1"}"#
        );
    }

    #[test]
    fn archive_manifest_rejects_entry_traversal() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: events.tar
  format: tar
  checksum_sha256: 0000000000000000000000000000000000000000000000000000000000000000
files:
  - logical_name: events
    path: ../events.csv
    format: csv
"#,
        )
        .expect("manifest");

        let error = load_archive_manifest(&manifest_path).expect_err("entry traversal");

        assert!(format!("{error:#}").contains("files.path must not contain . or .. components"));
    }

    #[test]
    fn archive_manifest_rejects_archive_parent_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: ../events.tar
  format: tar
  checksum_sha256: 0000000000000000000000000000000000000000000000000000000000000000
files:
  - logical_name: events
    path: events.csv
    format: csv
"#,
        )
        .expect("manifest");

        let error = load_archive_manifest(&manifest_path).expect_err("archive parent path");

        assert!(format!("{error:#}").contains("archive.path must not contain . or .. components"));
    }

    #[test]
    fn archive_manifest_rejects_path_like_logical_names() {
        let temp = tempfile::tempdir().expect("tempdir");
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: events.tar
  format: tar
  checksum_sha256: 0000000000000000000000000000000000000000000000000000000000000000
files:
  - logical_name: ../events
    path: events.csv
    format: csv
"#,
        )
        .expect("manifest");

        let error = load_archive_manifest(&manifest_path).expect_err("logical name");

        assert!(format!("{error:#}").contains("logical_name '../events' is invalid"));
    }

    #[test]
    fn archive_manifest_requires_archive_checksum() {
        let temp = tempfile::tempdir().expect("tempdir");
        let archive_path = temp.path().join("events.tar");
        write_tar_archive(&archive_path, "events.csv", b"event_id,title\n1,Demo\n");
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: events.tar
  format: tar
files:
  - logical_name: events
    path: events.csv
    format: csv
"#,
        )
        .expect("manifest");

        let error = load_archive_manifest(&manifest_path).expect_err("missing checksum");

        assert!(format!("{error:#}").contains("archive checksum_sha256 is required"));
    }

    #[test]
    fn archive_manifest_rejects_checksum_mismatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let archive_path = temp.path().join("events.tar");
        write_tar_archive(&archive_path, "events.csv", b"event_id,title\n1,Demo\n");
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: events.tar
  format: tar
  checksum_sha256: 0000000000000000000000000000000000000000000000000000000000000000
files:
  - logical_name: events
    path: events.csv
    format: csv
"#,
        )
        .expect("manifest");

        let error = lint_archive_manifest_file(&manifest_path).expect_err("checksum mismatch");

        assert!(format!("{error:#}").contains("checksum mismatch"));
    }

    #[test]
    fn archive_manifest_rejects_duplicate_archive_entry_paths() {
        let temp = tempfile::tempdir().expect("tempdir");
        let archive_path = temp.path().join("events.tar");
        write_tar_archive_entries(
            &archive_path,
            &[
                ("events.csv", b"event_id,title\n1,Demo\n".as_slice()),
                ("events.csv", b"event_id,title\n2,Duplicate\n".as_slice()),
            ],
        );
        let checksum = checksum(&archive_path);
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: events.tar
  format: tar
  checksum_sha256: {checksum}
files:
  - logical_name: events
    path: events.csv
    format: csv
"#
            ),
        )
        .expect("manifest");

        let error = lint_archive_manifest_file(&manifest_path).expect_err("duplicate archive path");

        assert!(format!("{error:#}").contains("duplicate entry path events.csv"));
        let unpack_error = unpack_archive_manifest(&manifest_path, temp.path().join("unpacked"))
            .expect_err("duplicate archive path during unpack");
        assert!(format!("{unpack_error:#}").contains("duplicate entry path events.csv"));
    }

    #[test]
    fn archive_copy_rejects_entries_over_unpack_limit() {
        let mut input = "hello".as_bytes();
        let mut output = Vec::new();

        let error = copy_with_limit(&mut input, &mut output, 4).expect_err("copy limit");

        assert!(format!("{error:#}").contains("archive entry exceeds unpack size limits"));
        assert_eq!(output, b"hell");
    }

    #[test]
    fn archive_manifest_rejects_tar_symlink_entries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let archive_path = temp.path().join("events.tar");
        write_tar_symlink_archive(&archive_path, "events.csv", "target.csv");
        let checksum = checksum(&archive_path);
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: events.tar
  format: tar
  checksum_sha256: {checksum}
files:
  - logical_name: events
    path: events.csv
    format: csv
"#
            ),
        )
        .expect("manifest");

        let error = lint_archive_manifest_file(&manifest_path).expect_err("tar symlink");

        assert!(format!("{error:#}").contains("regular files only"));
    }

    #[test]
    fn archive_manifest_rejects_zip_symlink_entries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let archive_path = temp.path().join("events.zip");
        write_zip_symlink_archive(&archive_path, "events.csv", b"target.csv");
        let checksum = checksum(&archive_path);
        let manifest_path = temp.path().join("events.archive.yaml");
        fs::write(
            &manifest_path,
            format!(
                r#"
schema_version: 1
kind: archive_source
source_id: event-archive
source_name: Event archive
manifest_version: 1
archive:
  path: events.zip
  format: zip
  checksum_sha256: {checksum}
files:
  - logical_name: events
    path: events.csv
    format: csv
"#
            ),
        )
        .expect("manifest");

        let error = lint_archive_manifest_file(&manifest_path).expect_err("zip symlink");

        assert!(format!("{error:#}").contains("regular files only"));
    }

    fn write_tar_archive(path: &Path, entry_path: &str, bytes: &[u8]) {
        write_tar_archive_entries(path, &[(entry_path, bytes)]);
    }

    fn write_tar_archive_entries(path: &Path, entries: &[(&str, &[u8])]) {
        let file = fs::File::create(path).expect("tar file");
        let mut builder = tar::Builder::new(file);
        for (entry_path, bytes) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, *entry_path, *bytes)
                .expect("tar entry");
        }
        builder.finish().expect("tar finish");
    }

    fn write_tar_symlink_archive(path: &Path, entry_path: &str, link_target: &str) {
        let file = fs::File::create(path).expect("tar file");
        let mut builder = tar::Builder::new(file);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_mode(0o777);
        header.set_link_name(link_target).expect("link target");
        header.set_cksum();
        builder
            .append_data(&mut header, entry_path, std::io::empty())
            .expect("tar symlink entry");
        builder.finish().expect("tar finish");
    }

    fn write_zip_archive(path: &Path, entry_path: &str, bytes: &[u8]) {
        let file = fs::File::create(path).expect("zip file");
        let mut writer = zip::ZipWriter::new(file);
        writer
            .start_file(entry_path, zip::write::SimpleFileOptions::default())
            .expect("zip entry");
        writer.write_all(bytes).expect("zip bytes");
        writer.finish().expect("zip finish");
    }

    fn write_zip_symlink_archive(path: &Path, entry_path: &str, link_target: &[u8]) {
        let file = fs::File::create(path).expect("zip file");
        let mut writer = zip::ZipWriter::new(file);
        writer
            .add_symlink(
                entry_path,
                String::from_utf8_lossy(link_target),
                zip::write::SimpleFileOptions::default(),
            )
            .expect("zip symlink entry");
        writer.finish().expect("zip finish");
    }

    fn checksum(path: &Path) -> String {
        format!(
            "{:x}",
            Sha256::digest(fs::read(path).expect("checksum bytes"))
        )
    }
}
