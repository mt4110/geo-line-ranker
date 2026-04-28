use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use config::AppSettings;
use crawler::{
    format_doctor_summary, format_dry_run_summary, format_health_summary, format_scaffold_summary,
    format_summary, run_crawl_command, run_doctor_command, run_dry_run_command, run_fetch_command,
    run_health_command, run_parse_command, scaffold_domain, serve_manifest_dir,
    ScaffoldDomainRequest,
};
use crawler_core::{
    lint_manifest_dir, CrawlManifestLintSummary, ParserExpectedShape, SourceMaturity,
};
use observability::init_tracing;

const ROOT_LONG_ABOUT: &str = "\
Operate allowlist crawl sources without leaving the terminal.

The crawler CLI is designed to cover the everyday loop:
- scaffold a source
- verify policy and parser wiring
- fetch raw content
- parse and import deterministic event rows
- review recent health

If a source is already modeled in this repository, `--help` should tell you
enough to run and debug it without detouring through a website or GitHub page.";

const ROOT_AFTER_HELP: &str = "\
Common flows:
  Inspect one source before the first live fetch:
    crawler doctor --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml

  Run one source end to end:
    crawler run --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml

  Review recent health and red flags:
    crawler health --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml

  Start a new source:
    crawler scaffold-domain --help

Operational note:
  `crawler serve` auto-runs only manifests whose resolved `source_maturity` is
  `live_ready`.";

const FETCH_LONG_ABOUT: &str = "\
Fetch raw content for one manifest and stage it under raw storage.

Use this when you want to inspect live fetch behavior, robots handling, or
checksum changes before parsing. `fetch` alone does not import rows into
`events`.";

const FETCH_AFTER_HELP: &str = "\
Example:
  crawler fetch --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml

Good before:
  - the first live run of a new source
  - checking whether content changed
  - reproducing a fetch-only failure";

const PARSE_LONG_ABOUT: &str = "\
Parse the latest fetched content for one manifest, dedupe rows, and import the
result into core crawl events.

Use this after `fetch`, or after you already have staged raw content and want to
re-run parser logic deterministically.";

const PARSE_AFTER_HELP: &str = "\
Example:
  crawler parse --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml

This command reads the latest staged raw content for the manifest. If nothing
has been fetched yet, run `crawler fetch` first.";

const RUN_LONG_ABOUT: &str = "\
Fetch and parse one manifest in a single command.

Use `run` for manual smoke checks once `doctor` is clean and the manifest is in
a state you would trust to execute end to end.";

const RUN_AFTER_HELP: &str = "\
Example:
  crawler run --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml

If you want more control over fetch and parse independently, use those
subcommands directly.";

const HEALTH_LONG_ABOUT: &str = "\
Summarize recent crawler runs for one manifest.

`health` is the quickest way to answer:
- is this source succeeding lately?
- which fetch / parse reasons are repeating?
- which logical_name currently looks unhealthy?";

const HEALTH_AFTER_HELP: &str = "\
Example:
  crawler health --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml

Increase `--limit` when you want a longer trend window.";

const DOCTOR_LONG_ABOUT: &str = "\
Check robots, terms, parser registration, school existence, and target
`expected_shape` before you chase a live failure.

Use `doctor` first when onboarding a source or when a manifest starts behaving
strangely after a site change.";

const DOCTOR_AFTER_HELP: &str = "\
Example:
  crawler doctor --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml

This command is intentionally read-only. It is the safest first look at a new or
recently changed source.";

const DRY_RUN_LONG_ABOUT: &str = "\
Re-parse the latest fetched raw content and show predicted parse / dedupe /
import effects without mutating core events.

Use `dry-run` when you want to understand the blast radius of parser changes
before writing anything new to `events`.";

const DRY_RUN_AFTER_HELP: &str = "\
Example:
  crawler dry-run --manifest configs/crawler/sources/aoyama-junior-school-tour.yaml

This is especially useful while tuning parser logic against already staged raw
HTML or JSON.";

const SCAFFOLD_DOMAIN_LONG_ABOUT: &str = "\
Start a new crawl source without the usual manifest/fixture/doc busywork.

This command writes three files:
- a crawl manifest
- a parser fixture
- a shape-aware guide for the next edits

It also infers sensible defaults from `expected_shape` and `target_url`, so the
first draft already nudges you toward a clean logical_name, realistic defaults,
and a fixture that is small enough to test comfortably.";

const SCAFFOLD_DOMAIN_AFTER_HELP: &str = "\
Typical flow:
  1. point the scaffold at the real page or feed
  2. trim the fixture to the smallest parser-shaped snippet
  3. wire parser tests
  4. run doctor / dry-run / health before promotion

Example:
  cargo run -p crawler -- scaffold-domain \\
    --source-id aoyama-junior-school-tour \\
    --source-name \"Aoyama Gakuin Junior High admissions school tours\" \\
    --school-id school_aoyama_gakuin_junior \\
    --parser-key aoyama_junior_school_tour_v1 \\
    --source-maturity parser_only \\
    --expected-shape html_school_tour_blocks \\
    --target-url https://www.jh.aoyama.ed.jp/admission/explanation.html";

const SERVE_LONG_ABOUT: &str = "\
Continuously poll a manifest directory and execute live-ready sources on an
interval.

This is the background mode for the crawler. It intentionally skips manifests
that resolve to `parser_only` or `policy_blocked` so operator review stays
explicit.";

const SERVE_AFTER_HELP: &str = "\
Example:
  crawler serve --manifest-dir configs/crawler/sources --poll-interval-secs 300

Use `doctor`, `dry-run`, and `health` on individual manifests before you trust a
source enough to let `serve` poll it automatically.";

#[derive(Debug, Parser)]
#[command(
    name = "geo-line-ranker-crawler",
    about = "Allowlist crawler operations for deterministic event sources.",
    long_about = ROOT_LONG_ABOUT,
    after_long_help = ROOT_AFTER_HELP,
    next_line_help = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(
        about = "Fetch raw content for one manifest.",
        long_about = FETCH_LONG_ABOUT,
        after_long_help = FETCH_AFTER_HELP
    )]
    Fetch {
        #[arg(
            long,
            help_heading = "Required Inputs",
            help = "Path to the crawl manifest yaml to fetch."
        )]
        manifest: PathBuf,
    },
    #[command(
        about = "Parse the latest fetched content for one manifest.",
        long_about = PARSE_LONG_ABOUT,
        after_long_help = PARSE_AFTER_HELP
    )]
    Parse {
        #[arg(
            long,
            help_heading = "Required Inputs",
            help = "Path to the crawl manifest yaml whose latest staged content should be parsed."
        )]
        manifest: PathBuf,
    },
    #[command(
        about = "Fetch then parse one manifest in a single step.",
        long_about = RUN_LONG_ABOUT,
        after_long_help = RUN_AFTER_HELP
    )]
    Run {
        #[arg(
            long,
            help_heading = "Required Inputs",
            help = "Path to the crawl manifest yaml to run end to end."
        )]
        manifest: PathBuf,
    },
    #[command(
        about = "Summarize recent health for one manifest.",
        long_about = HEALTH_LONG_ABOUT,
        after_long_help = HEALTH_AFTER_HELP
    )]
    Health {
        #[arg(
            long,
            help_heading = "Required Inputs",
            help = "Path to the crawl manifest yaml whose recent run history should be summarized."
        )]
        manifest: PathBuf,
        #[arg(
            long,
            default_value_t = 10,
            help_heading = "Behavior",
            help = "How many recent runs to include in the health summary."
        )]
        limit: usize,
    },
    #[command(
        about = "Check robots, policy, parser wiring, and shape before a live run.",
        long_about = DOCTOR_LONG_ABOUT,
        after_long_help = DOCTOR_AFTER_HELP
    )]
    Doctor {
        #[arg(
            long,
            help_heading = "Required Inputs",
            help = "Path to the crawl manifest yaml to inspect."
        )]
        manifest: PathBuf,
    },
    #[command(
        about = "Preview parse/import effects without mutating events.",
        long_about = DRY_RUN_LONG_ABOUT,
        after_long_help = DRY_RUN_AFTER_HELP
    )]
    DryRun {
        #[arg(
            long,
            help_heading = "Required Inputs",
            help = "Path to the crawl manifest yaml whose latest staged content should be re-parsed."
        )]
        manifest: PathBuf,
    },
    #[command(
        about = "Scaffold a new crawl source with a manifest, fixture, and guide.",
        long_about = SCAFFOLD_DOMAIN_LONG_ABOUT,
        after_long_help = SCAFFOLD_DOMAIN_AFTER_HELP
    )]
    ScaffoldDomain(Box<ScaffoldDomainArgs>),
    #[command(
        about = "Poll a manifest directory and auto-run live-ready sources.",
        long_about = SERVE_LONG_ABOUT,
        after_long_help = SERVE_AFTER_HELP
    )]
    Serve {
        #[arg(
            long,
            default_value = "configs/crawler/sources",
            help_heading = "Required Inputs",
            help = "Directory containing crawler manifests to watch."
        )]
        manifest_dir: PathBuf,
        #[arg(
            long,
            default_value_t = 300,
            help_heading = "Behavior",
            help = "Polling interval in seconds between manifest scans."
        )]
        poll_interval_secs: u64,
    },
    #[command(about = "Inspect crawler manifests without fetching live content")]
    Manifest {
        #[command(subcommand)]
        target: ManifestCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ManifestCommand {
    #[command(about = "Lint crawler manifest files")]
    Lint {
        #[arg(
            long,
            default_value = "configs/crawler/sources",
            help_heading = "Required Inputs",
            help = "Directory or YAML file containing crawler manifests."
        )]
        manifest_dir: PathBuf,
    },
}

#[derive(Debug, Args)]
#[command(next_line_help = true)]
struct ScaffoldDomainArgs {
    #[arg(
        long,
        help_heading = "Required Inputs",
        help = "Stable source slug used for the manifest file name and fixture stub."
    )]
    source_id: String,
    #[arg(
        long,
        help_heading = "Required Inputs",
        help = "Human-readable source name used in the manifest, guide, and fixture copy."
    )]
    source_name: String,
    #[arg(
        long,
        help_heading = "Required Inputs",
        help = "Target school id that the scaffold should wire into defaults and tests."
    )]
    school_id: String,
    #[arg(
        long,
        help_heading = "Required Inputs",
        help = "Parser registry key to bind this source to, or a temporary placeholder while you implement one."
    )]
    parser_key: String,
    #[arg(
        long,
        default_value = "parser_only",
        value_parser = ["live_ready", "policy_blocked", "parser_only"],
        help_heading = "Behavior",
        help = "Operational state for the new source."
    )]
    source_maturity: String,
    #[arg(
        long,
        value_parser = [
            "html_heading_page",
            "html_card_listing",
            "html_keio_event_cards",
            "html_school_tour_blocks",
            "json_feed",
            "html_qua_sections",
            "html_session_tables",
            "html_monthly_dl_pairs"
        ],
        help_heading = "Required Inputs",
        help = "Parser/doctor contract for the target page or feed. This also drives inferred defaults."
    )]
    expected_shape: String,
    #[arg(
        long,
        help_heading = "Required Inputs",
        help = "Public page or feed URL to model. Its host seeds allowlist and robots placeholders."
    )]
    target_url: String,
    #[arg(
        long,
        help_heading = "Behavior",
        help = "Optional logical_name override. When omitted, the scaffold infers one from shape and path."
    )]
    logical_name: Option<String>,
    #[arg(
        long,
        default_value = "configs/crawler/sources",
        help_heading = "Output Paths",
        help = "Directory where the manifest yaml should be written."
    )]
    manifest_dir: PathBuf,
    #[arg(
        long,
        default_value = "storage/fixtures/crawler",
        help_heading = "Output Paths",
        help = "Directory where the starter parser fixture should be written."
    )]
    fixture_dir: PathBuf,
    #[arg(
        long,
        default_value = "docs/crawler_scaffolds",
        help_heading = "Output Paths",
        help = "Directory where the next-step guide should be written."
    )]
    guide_dir: PathBuf,
    #[arg(
        long,
        default_value_t = false,
        help_heading = "Safety",
        help = "Overwrite existing scaffold files instead of refusing to replace them."
    )]
    force: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing("info");

    let settings = AppSettings::from_env()?;
    let cli = Cli::parse();

    match cli.command.unwrap_or(Command::Serve {
        manifest_dir: PathBuf::from("configs/crawler/sources"),
        poll_interval_secs: 300,
    }) {
        Command::Fetch { manifest } => {
            let summary = run_fetch_command(&settings, manifest).await?;
            println!("{}", format_summary(&summary));
        }
        Command::Parse { manifest } => {
            let summary = run_parse_command(&settings, manifest).await?;
            println!("{}", format_summary(&summary));
        }
        Command::Run { manifest } => {
            let summary = run_crawl_command(&settings, manifest).await?;
            println!("{}", format_summary(&summary));
        }
        Command::Health { manifest, limit } => {
            let summary = run_health_command(&settings, manifest, limit).await?;
            println!("{}", format_health_summary(&summary));
        }
        Command::Doctor { manifest } => {
            let summary = run_doctor_command(&settings, manifest).await?;
            println!("{}", format_doctor_summary(&summary));
        }
        Command::DryRun { manifest } => {
            let summary = run_dry_run_command(&settings, manifest).await?;
            println!("{}", format_dry_run_summary(&summary));
        }
        Command::ScaffoldDomain(args) => {
            let summary = scaffold_domain(ScaffoldDomainRequest {
                source_id: args.source_id.clone(),
                source_name: args.source_name.clone(),
                school_id: args.school_id.clone(),
                parser_key: args.parser_key.clone(),
                source_maturity: args.source_maturity.parse::<SourceMaturity>()?,
                expected_shape: args.expected_shape.parse::<ParserExpectedShape>()?,
                target_url: args.target_url.clone(),
                logical_name: args.logical_name.clone(),
                manifest_dir: args.manifest_dir.clone(),
                fixture_dir: args.fixture_dir.clone(),
                guide_dir: args.guide_dir.clone(),
                force: args.force,
            })?;
            println!("{}", format_scaffold_summary(&summary));
        }
        Command::Serve {
            manifest_dir,
            poll_interval_secs,
        } => {
            serve_manifest_dir(&settings, manifest_dir, poll_interval_secs).await?;
        }
        Command::Manifest { target } => match target {
            ManifestCommand::Lint { manifest_dir } => {
                let summary = lint_manifest_dir(manifest_dir)?;
                println!("{}", format_manifest_lint_summary(&summary));
            }
        },
    }

    Ok(())
}

fn format_manifest_lint_summary(summary: &CrawlManifestLintSummary) -> String {
    let mut lines = vec![format!(
        "crawler manifest lint ok: files={}",
        summary.files.len()
    )];
    lines.extend(summary.files.iter().map(|file| {
        format!(
            "- {} schema_version={} kind={} manifest_version={} source_id={} parser_key={} targets={}",
            file.path.display(),
            file.schema_version,
            file.kind.as_str(),
            file.manifest_version,
            file.source_id,
            file.parser_key,
            file.target_count
        )
    }));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn scaffold_domain_help_is_grouped_and_actionable() {
        let mut command = Cli::command();
        let scaffold = command
            .find_subcommand_mut("scaffold-domain")
            .expect("scaffold-domain subcommand");
        let mut buffer = Vec::new();
        scaffold.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help
            .contains("Start a new crawl source without the usual manifest/fixture/doc busywork."));
        assert!(help.contains("Required Inputs:"));
        assert!(help.contains("Behavior:"));
        assert!(help.contains("Output Paths:"));
        assert!(help.contains("Typical flow:"));
        assert!(help.contains("html_school_tour_blocks"));
        assert!(help.contains("policy_blocked"));
        assert!(help.contains("When omitted, the scaffold infers one"));
    }

    #[test]
    fn root_help_maps_the_crawler_workflow() {
        let mut command = Cli::command();
        let mut buffer = Vec::new();
        command.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(help.contains("Operate allowlist crawl sources without leaving the terminal."));
        assert!(help.contains("Fetch raw content for one manifest."));
        assert!(help.contains("Check robots, policy, parser wiring, and shape before a live run."));
        assert!(help.contains("Poll a manifest directory and auto-run live-ready sources."));
        assert!(help.contains("Common flows:"));
        assert!(help.contains("crawler scaffold-domain --help"));
        assert!(help.contains("`live_ready`"));
    }

    #[test]
    fn doctor_help_is_self_sufficient() {
        let mut command = Cli::command();
        let doctor = command
            .find_subcommand_mut("doctor")
            .expect("doctor subcommand");
        let mut buffer = Vec::new();
        doctor.write_long_help(&mut buffer).expect("write help");
        let help = String::from_utf8(buffer).expect("utf8 help");

        assert!(
            help.contains("Check robots, terms, parser registration, school existence, and target")
        );
        assert!(help.contains("Path to the crawl manifest yaml to inspect."));
        assert!(help.contains("This command is intentionally read-only."));
    }
}
