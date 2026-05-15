use std::{
    collections::BTreeSet,
    fmt,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    is_source_id, read_raw, validate_portable_relative_path, ProfileCompatibilityLevel,
    ProfilePackManifest, SOURCE_ID_RULE_DESCRIPTION,
};

const FIELD_MAPPING_REF_RULE_DESCRIPTION: &str = "must be non-empty and trimmed, use only lowercase letters, digits, underscores, and hyphens, and must not start or end with a separator";
const RUNTIME_EXECUTABLE_FIELD_MAPPING_IDS: &[&str] = &["event_v1"];
pub const PROFILE_CONNECTOR_SCHEMA_CONTRACT_VERSION: &str =
    "local_stable_connector_manifest_schema_v1";
const PROFILE_CONNECTOR_YAML_MANIFEST_SCHEMA_VERSION: u32 = 1;
const LOCAL_REFERENCE_SAFETY: ProfileConnectorSafetyMetadata = ProfileConnectorSafetyMetadata {
    local_reference_only: true,
    dynamic_loading_enabled: false,
    live_fetch_default: false,
    allowlist_required: false,
};
const LOCAL_CRAWLER_REFERENCE_SAFETY: ProfileConnectorSafetyMetadata =
    ProfileConnectorSafetyMetadata {
        local_reference_only: true,
        dynamic_loading_enabled: false,
        live_fetch_default: false,
        allowlist_required: true,
    };

static PROFILE_CONNECTOR_SCHEMA_CONTRACTS: [ProfileConnectorSchemaContract; 5] = [
    ProfileConnectorSchemaContract {
        connector_type: ProfileConnectorType::SourceManifest,
        source_class: ProfileSourceClass::CsvImport,
        manifest_kind: "import_source",
        manifest_schema_version: Some(PROFILE_CONNECTOR_YAML_MANIFEST_SCHEMA_VERSION),
        source_id_scope: "manifest_required_profile_override_must_match",
        field_mapping_scope: "not_supported",
        runtime_execution: "profile_source_import",
        manifest_lint: "source_manifest_lint",
        safety: LOCAL_REFERENCE_SAFETY,
    },
    ProfileConnectorSchemaContract {
        connector_type: ProfileConnectorType::CsvImport,
        source_class: ProfileSourceClass::CsvImport,
        manifest_kind: "csv_file",
        manifest_schema_version: None,
        source_id_scope: "profile_required",
        field_mapping_scope: "event_v1_required_for_runtime",
        runtime_execution: "event_csv_import",
        manifest_lint: "file_reference",
        safety: LOCAL_REFERENCE_SAFETY,
    },
    ProfileConnectorSchemaContract {
        connector_type: ProfileConnectorType::NdjsonImport,
        source_class: ProfileSourceClass::NdjsonImport,
        manifest_kind: "ndjson_file",
        manifest_schema_version: None,
        source_id_scope: "profile_required",
        field_mapping_scope: "event_v1_required_for_runtime",
        runtime_execution: "event_ndjson_import",
        manifest_lint: "file_reference",
        safety: LOCAL_REFERENCE_SAFETY,
    },
    ProfileConnectorSchemaContract {
        connector_type: ProfileConnectorType::ArchiveSource,
        source_class: ProfileSourceClass::ArchiveImport,
        manifest_kind: "archive_source",
        manifest_schema_version: Some(PROFILE_CONNECTOR_YAML_MANIFEST_SCHEMA_VERSION),
        source_id_scope: "manifest_required_profile_override_must_match",
        field_mapping_scope: "event_v1_required_for_runtime",
        runtime_execution: "event_archive_import",
        manifest_lint: "archive_source_lint",
        safety: LOCAL_REFERENCE_SAFETY,
    },
    ProfileConnectorSchemaContract {
        connector_type: ProfileConnectorType::CrawlerManifest,
        source_class: ProfileSourceClass::HtmlCrawl,
        manifest_kind: "crawler_source",
        manifest_schema_version: Some(PROFILE_CONNECTOR_YAML_MANIFEST_SCHEMA_VERSION),
        source_id_scope: "manifest_required_profile_override_must_match",
        field_mapping_scope: "not_supported",
        runtime_execution: "crawler_commands_only",
        manifest_lint: "crawler_manifest_lint",
        safety: LOCAL_CRAWLER_REFERENCE_SAFETY,
    },
];

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ProfileConnectorType {
    SourceManifest,
    CsvImport,
    NdjsonImport,
    ArchiveSource,
    CrawlerManifest,
}

impl ProfileConnectorType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SourceManifest => "source_manifest",
            Self::CsvImport => "csv_import",
            Self::NdjsonImport => "ndjson_import",
            Self::ArchiveSource => "archive_source",
            Self::CrawlerManifest => "crawler_manifest",
        }
    }

    pub fn source_class(self) -> ProfileSourceClass {
        self.schema_contract().source_class
    }

    pub fn expected_manifest_kind(self) -> &'static str {
        self.schema_contract().manifest_kind
    }

    pub fn expected_manifest_schema_version(self) -> Option<u32> {
        self.schema_contract().manifest_schema_version
    }

    fn schema_contract(self) -> &'static ProfileConnectorSchemaContract {
        profile_connector_schema_contracts()
            .iter()
            .find(|contract| contract.connector_type == self)
            .expect("profile connector schema contract")
    }

    fn expected_extension(self) -> Option<&'static str> {
        match self {
            Self::CsvImport => Some("csv"),
            Self::NdjsonImport => Some("ndjson"),
            Self::SourceManifest | Self::ArchiveSource | Self::CrawlerManifest => None,
        }
    }
}

impl fmt::Display for ProfileConnectorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ProfileSourceClass {
    CsvImport,
    NdjsonImport,
    ArchiveImport,
    HtmlCrawl,
}

impl ProfileSourceClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CsvImport => "csv_import",
            Self::NdjsonImport => "ndjson_import",
            Self::ArchiveImport => "archive_import",
            Self::HtmlCrawl => "html_crawl",
        }
    }
}

impl fmt::Display for ProfileSourceClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileConnectorSafetyMetadata {
    pub local_reference_only: bool,
    pub dynamic_loading_enabled: bool,
    pub live_fetch_default: bool,
    pub allowlist_required: bool,
}

impl ProfileConnectorSafetyMetadata {
    fn for_connector_type(connector_type: ProfileConnectorType) -> Self {
        connector_type.schema_contract().safety
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct ProfileConnectorSchemaContract {
    pub connector_type: ProfileConnectorType,
    pub source_class: ProfileSourceClass,
    pub manifest_kind: &'static str,
    pub manifest_schema_version: Option<u32>,
    pub source_id_scope: &'static str,
    pub field_mapping_scope: &'static str,
    pub runtime_execution: &'static str,
    pub manifest_lint: &'static str,
    pub safety: ProfileConnectorSafetyMetadata,
}

pub fn profile_connector_schema_contracts() -> &'static [ProfileConnectorSchemaContract] {
    &PROFILE_CONNECTOR_SCHEMA_CONTRACTS
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProfileConnectorFieldMapping {
    EventV1,
    Custom(String),
}

impl ProfileConnectorFieldMapping {
    pub fn as_str(&self) -> &str {
        match self {
            Self::EventV1 => "event_v1",
            Self::Custom(value) => value.as_str(),
        }
    }

    pub fn is_runtime_executable(&self) -> bool {
        matches!(self, Self::EventV1)
    }
}

impl Serialize for ProfileConnectorFieldMapping {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProfileConnectorFieldMapping {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value == "event_v1" {
            Ok(Self::EventV1)
        } else {
            Ok(Self::Custom(value))
        }
    }
}

impl From<&str> for ProfileConnectorFieldMapping {
    fn from(value: &str) -> Self {
        if value == "event_v1" {
            Self::EventV1
        } else {
            Self::Custom(value.to_string())
        }
    }
}

impl From<String> for ProfileConnectorFieldMapping {
    fn from(value: String) -> Self {
        if value == "event_v1" {
            Self::EventV1
        } else {
            Self::Custom(value)
        }
    }
}

impl fmt::Display for ProfileConnectorFieldMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileConnectorRegistryEntry {
    pub connector_type: ProfileConnectorType,
    pub source_class: ProfileSourceClass,
    pub manifest_path: PathBuf,
    pub manifest_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_schema_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_mapping: Option<ProfileConnectorFieldMapping>,
    pub profile_compatibility: ProfileCompatibilityLevel,
    pub safety: ProfileConnectorSafetyMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProfileConnectorRef {
    #[serde(rename = "type")]
    pub connector_type: ProfileConnectorType,
    pub manifest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_mapping: Option<ProfileConnectorFieldMapping>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct ConnectorManifestHeader {
    schema_version: u32,
    kind: String,
    #[serde(default)]
    source_id: Option<String>,
}

pub(crate) fn build_profile_connector_registry(
    path: &Path,
    manifest: &ProfilePackManifest,
    manifest_dir: &Path,
) -> Result<Vec<ProfileConnectorRegistryEntry>> {
    let entries = manifest
        .connectors
        .iter()
        .map(|connector| profile_connector_registry_entry(path, manifest, manifest_dir, connector))
        .collect::<Result<Vec<_>>>()?;
    ensure_unique_connector_source_ids(path, &entries)?;
    Ok(entries)
}

pub(crate) fn validate_profile_connector_ref_contract(
    profile_path: &Path,
    connector: &ProfileConnectorRef,
) -> Result<()> {
    if let Some(field_mapping) = connector.field_mapping.as_ref() {
        ensure!(
            is_field_mapping_ref(field_mapping.as_str()),
            "profile pack {} connector {} field_mapping '{}' is invalid; {}",
            profile_path.display(),
            connector.connector_type.as_str(),
            field_mapping.as_str(),
            FIELD_MAPPING_REF_RULE_DESCRIPTION
        );
    }
    Ok(())
}

fn profile_connector_registry_entry(
    path: &Path,
    manifest: &ProfilePackManifest,
    manifest_dir: &Path,
    connector: &ProfileConnectorRef,
) -> Result<ProfileConnectorRegistryEntry> {
    let resolved = manifest_dir.join(validate_portable_relative_path(
        path,
        "connectors.manifest",
        &connector.manifest,
    )?);
    ensure!(
        resolved.is_file(),
        "profile pack {} connector {} manifest {} is missing or not a file",
        path.display(),
        connector.connector_type.as_str(),
        resolved.display()
    );

    let (manifest_kind, manifest_schema_version, source_id, field_mapping) =
        match connector.connector_type {
            ProfileConnectorType::SourceManifest => {
                ensure_no_file_field_mapping(path, connector)?;
                let header = load_connector_manifest_header(path, &resolved)?;
                ensure_connector_manifest_kind(path, &resolved, connector, &header)?;
                let source_id =
                    require_connector_source_id(path, &resolved, header.source_id.as_deref())?;
                validate_connector_source_id_override(path, &resolved, connector, &source_id)?;
                (
                    header.kind,
                    Some(header.schema_version),
                    Some(source_id),
                    None,
                )
            }
            ProfileConnectorType::CrawlerManifest => {
                ensure_no_file_field_mapping(path, connector)?;
                let header = load_connector_manifest_header(path, &resolved)?;
                ensure_connector_manifest_kind(path, &resolved, connector, &header)?;
                let source_id =
                    require_connector_source_id(path, &resolved, header.source_id.as_deref())?;
                validate_connector_source_id_override(path, &resolved, connector, &source_id)?;
                (
                    header.kind,
                    Some(header.schema_version),
                    Some(source_id),
                    None,
                )
            }
            ProfileConnectorType::ArchiveSource => {
                let header = load_connector_manifest_header(path, &resolved)?;
                ensure_connector_manifest_kind(path, &resolved, connector, &header)?;
                let source_id =
                    require_connector_source_id(path, &resolved, header.source_id.as_deref())?;
                validate_connector_source_id_override(path, &resolved, connector, &source_id)?;
                let field_mapping =
                    require_runtime_executable_import_field_mapping(path, manifest, connector)?;
                (
                    header.kind,
                    Some(header.schema_version),
                    Some(source_id),
                    Some(field_mapping),
                )
            }
            ProfileConnectorType::CsvImport | ProfileConnectorType::NdjsonImport => {
                let expected_extension = connector
                    .connector_type
                    .expected_extension()
                    .expect("file connector extension");
                let extension = resolved
                    .extension()
                    .and_then(|value| value.to_str())
                    .unwrap_or_default();
                ensure!(
                    extension.eq_ignore_ascii_case(expected_extension),
                    "profile pack {} connector {} manifest {} must point to a .{} file",
                    path.display(),
                    connector.connector_type.as_str(),
                    resolved.display(),
                    expected_extension
                );
                let field_mapping =
                    require_runtime_executable_import_field_mapping(path, manifest, connector)?;
                (
                    connector
                        .connector_type
                        .expected_manifest_kind()
                        .to_string(),
                    None,
                    Some(require_profile_connector_source_id(path, connector)?),
                    Some(field_mapping),
                )
            }
        };

    Ok(ProfileConnectorRegistryEntry {
        connector_type: connector.connector_type,
        source_class: connector.connector_type.source_class(),
        manifest_path: resolved.canonicalize().with_context(|| {
            format!(
                "failed to canonicalize profile pack {} connector {} manifest {}",
                path.display(),
                connector.connector_type.as_str(),
                resolved.display()
            )
        })?,
        manifest_kind,
        manifest_schema_version,
        source_id,
        field_mapping,
        profile_compatibility: manifest.compatibility_level,
        safety: ProfileConnectorSafetyMetadata::for_connector_type(connector.connector_type),
    })
}

fn ensure_connector_manifest_kind(
    profile_path: &Path,
    connector_manifest_path: &Path,
    connector: &ProfileConnectorRef,
    header: &ConnectorManifestHeader,
) -> Result<()> {
    if let Some(expected_schema_version) =
        connector.connector_type.expected_manifest_schema_version()
    {
        ensure!(
            header.schema_version == expected_schema_version,
            "profile pack {} connector {} manifest {} schema_version {} is invalid; expected {}",
            profile_path.display(),
            connector.connector_type.as_str(),
            connector_manifest_path.display(),
            header.schema_version,
            expected_schema_version
        );
    }
    ensure!(
        header.kind == connector.connector_type.expected_manifest_kind(),
        "profile pack {} connector {} manifest {} kind {} is invalid; expected {}",
        profile_path.display(),
        connector.connector_type.as_str(),
        connector_manifest_path.display(),
        header.kind,
        connector.connector_type.expected_manifest_kind()
    );
    Ok(())
}

fn require_connector_source_id(
    profile_path: &Path,
    connector_manifest_path: &Path,
    source_id: Option<&str>,
) -> Result<String> {
    let source_id = source_id.with_context(|| {
        format!(
            "profile pack {} connector manifest {} must declare source_id",
            profile_path.display(),
            connector_manifest_path.display()
        )
    })?;
    ensure!(
        is_source_id(source_id),
        "profile pack {} connector manifest {} source_id '{}' is invalid; {}",
        profile_path.display(),
        connector_manifest_path.display(),
        source_id,
        SOURCE_ID_RULE_DESCRIPTION
    );
    Ok(source_id.to_string())
}

fn require_profile_connector_source_id(
    profile_path: &Path,
    connector: &ProfileConnectorRef,
) -> Result<String> {
    let source_id = connector.source_id.as_deref().with_context(|| {
        format!(
            "profile pack {} connector {} must declare source_id",
            profile_path.display(),
            connector.connector_type.as_str()
        )
    })?;
    ensure_profile_connector_source_id(profile_path, connector, source_id)?;
    Ok(source_id.to_string())
}

fn validate_connector_source_id_override(
    profile_path: &Path,
    connector_manifest_path: &Path,
    connector: &ProfileConnectorRef,
    manifest_source_id: &str,
) -> Result<()> {
    if let Some(source_id) = connector.source_id.as_deref() {
        ensure_profile_connector_source_id(profile_path, connector, source_id)?;
        ensure!(
            source_id == manifest_source_id,
            "profile pack {} connector {} source_id {} does not match manifest {} source_id {}",
            profile_path.display(),
            connector.connector_type.as_str(),
            source_id,
            connector_manifest_path.display(),
            manifest_source_id
        );
    }
    Ok(())
}

fn ensure_profile_connector_source_id(
    profile_path: &Path,
    connector: &ProfileConnectorRef,
    source_id: &str,
) -> Result<()> {
    ensure!(
        is_source_id(source_id),
        "profile pack {} connector {} source_id '{}' is invalid; {}",
        profile_path.display(),
        connector.connector_type.as_str(),
        source_id,
        SOURCE_ID_RULE_DESCRIPTION
    );
    Ok(())
}

fn ensure_no_file_field_mapping(
    profile_path: &Path,
    connector: &ProfileConnectorRef,
) -> Result<()> {
    ensure!(
        connector.field_mapping.is_none(),
        "profile pack {} connector {} must not declare field_mapping; field_mapping is only supported for file or archive import connectors",
        profile_path.display(),
        connector.connector_type.as_str()
    );
    Ok(())
}

fn require_runtime_executable_import_field_mapping(
    profile_path: &Path,
    manifest: &ProfilePackManifest,
    connector: &ProfileConnectorRef,
) -> Result<ProfileConnectorFieldMapping> {
    let field_mapping = connector.field_mapping.as_ref().with_context(|| {
        format!(
            "profile pack {} connector {} must declare field_mapping",
            profile_path.display(),
            connector.connector_type.as_str()
        )
    })?;
    ensure!(
        field_mapping.is_runtime_executable(),
        "profile pack {} connector {} field_mapping {} is a valid mapping ref but is not executable by the current import runtime; current import runtime executes only {}",
        profile_path.display(),
        connector.connector_type.as_str(),
        field_mapping.as_str(),
        RUNTIME_EXECUTABLE_FIELD_MAPPING_IDS.join(",")
    );
    ensure!(
        manifest
            .supported_content_kinds
            .iter()
            .any(|kind| kind.as_str() == "event"),
        "profile pack {} connector {} field_mapping event_v1 requires supported_content_kinds to include event",
        profile_path.display(),
        connector.connector_type.as_str()
    );
    Ok(field_mapping.clone())
}

fn is_field_mapping_ref(value: &str) -> bool {
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

fn ensure_unique_connector_source_ids(
    profile_path: &Path,
    entries: &[ProfileConnectorRegistryEntry],
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for entry in entries {
        if let Some(source_id) = entry.source_id.as_deref() {
            ensure!(
                seen.insert(source_id.to_string()),
                "profile pack {} connectors contain duplicate source_id {}",
                profile_path.display(),
                source_id
            );
        }
    }
    Ok(())
}

fn load_connector_manifest_header(
    profile_path: &Path,
    connector_manifest_path: &Path,
) -> Result<ConnectorManifestHeader> {
    let raw = read_raw(connector_manifest_path).with_context(|| {
        format!(
            "failed to read profile pack {} connector manifest {}",
            profile_path.display(),
            connector_manifest_path.display()
        )
    })?;
    let header: ConnectorManifestHeader = serde_yaml::from_str(&raw).with_context(|| {
        format!(
            "failed to parse profile pack {} connector manifest {}",
            profile_path.display(),
            connector_manifest_path.display()
        )
    })?;
    ensure!(
        header.schema_version > 0,
        "profile pack {} connector manifest {} schema_version must be positive",
        profile_path.display(),
        connector_manifest_path.display()
    );
    ensure!(
        !header.kind.trim().is_empty(),
        "profile pack {} connector manifest {} kind must not be empty",
        profile_path.display(),
        connector_manifest_path.display()
    );
    Ok(header)
}
