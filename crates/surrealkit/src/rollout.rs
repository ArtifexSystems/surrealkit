use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_types::SurrealValue;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use crate::core::{exec_surql, sha256_hex};
use crate::schema_state::{
	CATALOG_SNAPSHOT_PATH, CatalogChange, CatalogDiff, CatalogEntity, CatalogSnapshot, EntityKey,
	FileDiff, ROLLOUTS_DIR, SchemaFile, SchemaSnapshot, build_catalog_snapshot,
	collect_schema_files, diff_catalog, diff_schema, ensure_local_state_dirs, ensure_overwrite,
	hash_schema_snapshot, load_catalog_snapshot, load_schema_snapshot, render_remove_sql,
	save_catalog_snapshot, save_schema_snapshot, snapshot_from_files,
};
use crate::setup::run_setup;
use crate::variables::TemplateVars;

#[derive(Debug, Clone)]
pub struct RolloutPlanOpts {
	pub name: Option<String>,
	pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct RolloutExecutionOpts {
	pub selector: Option<String>,
}

const PROJECT_CONFIG_PATHS: [&str; 2] = ["database/surrealkit.toml", "surrealkit.toml"];

#[derive(Debug, Default, Deserialize)]
struct RolloutProjectConfig {
	#[serde(default)]
	rollouts: RolloutConfig,
}

#[derive(Debug, Default, Deserialize)]
struct RolloutConfig {
	/// Project-root-relative schema-directory prefixes that name the declared
	/// extension transition: omitted from source/target hash computation, and
	/// used as the autoplan partition boundary when set. The complete
	/// schema tree is still parsed, applied, and retained in the desired catalog.
	#[serde(default)]
	target_hash_exclude_prefixes: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct RolloutHashScope {
	exclude_prefixes: Vec<String>,
}

impl RolloutHashScope {
	fn hash_files(&self, files: &[SchemaFile]) -> Result<String> {
		self.hash_snapshot(&snapshot_from_files(files))
	}

	fn hash_snapshot(&self, snapshot: &SchemaSnapshot) -> Result<String> {
		let mut filtered = snapshot.clone();
		filtered.files.retain(|entry| {
			!self.exclude_prefixes.iter().any(|prefix| entry.path.starts_with(prefix))
		});
		hash_schema_snapshot(&filtered)
	}

	fn path_is_declared_transition(&self, path: &str) -> bool {
		self.exclude_prefixes.iter().any(|prefix| path.starts_with(prefix))
	}

	fn path_is_declared_catalog_change(&self, change: &CatalogChange) -> bool {
		self.path_is_declared_transition(&change.old.source_path)
			|| self.path_is_declared_transition(&change.new.source_path)
	}

	fn partition_paths(&self, paths: Vec<String>) -> (Vec<String>, Vec<String>) {
		if self.exclude_prefixes.is_empty() {
			return (Vec::new(), paths);
		}
		let mut declared = Vec::new();
		let mut unrelated = Vec::new();
		for path in paths {
			if self.path_is_declared_transition(&path) {
				declared.push(path);
			} else {
				unrelated.push(path);
			}
		}
		(declared, unrelated)
	}

	fn partition_entities(
		&self,
		entities: Vec<CatalogEntity>,
	) -> (Vec<CatalogEntity>, Vec<CatalogEntity>) {
		if self.exclude_prefixes.is_empty() {
			return (Vec::new(), entities);
		}
		let mut declared = Vec::new();
		let mut unrelated = Vec::new();
		for entity in entities {
			if self.path_is_declared_transition(&entity.source_path) {
				declared.push(entity);
			} else {
				unrelated.push(entity);
			}
		}
		(declared, unrelated)
	}

	fn partition_changes(
		&self,
		changes: Vec<CatalogChange>,
	) -> (Vec<CatalogChange>, Vec<CatalogChange>) {
		if self.exclude_prefixes.is_empty() {
			return (Vec::new(), changes);
		}
		let mut declared = Vec::new();
		let mut unrelated = Vec::new();
		for change in changes {
			if self.path_is_declared_catalog_change(&change) {
				declared.push(change);
			} else {
				unrelated.push(change);
			}
		}
		(declared, unrelated)
	}

	/// Split a file diff into the declared extension transition and the
	/// unrelated remainder. With no prefixes, everything is unrelated so
	/// stale-baseline reconciliation still sees the whole catalog.
	fn partition_file_diff(&self, diff: FileDiff) -> (FileDiff, FileDiff) {
		let (added_declared, added_unrelated) = self.partition_paths(diff.added);
		let (modified_declared, modified_unrelated) = self.partition_paths(diff.modified);
		let (removed_declared, removed_unrelated) = self.partition_paths(diff.removed);
		(
			FileDiff {
				added: added_declared,
				modified: modified_declared,
				removed: removed_declared,
			},
			FileDiff {
				added: added_unrelated,
				modified: modified_unrelated,
				removed: removed_unrelated,
			},
		)
	}

	fn partition_catalog_diff(&self, diff: CatalogDiff) -> (CatalogDiff, CatalogDiff) {
		let (added_declared, added_unrelated) = self.partition_entities(diff.added);
		let (removed_declared, removed_unrelated) = self.partition_entities(diff.removed);
		let (modified_declared, modified_unrelated) = self.partition_changes(diff.modified);
		(
			CatalogDiff {
				added: added_declared,
				removed: removed_declared,
				modified: modified_declared,
			},
			CatalogDiff {
				added: added_unrelated,
				removed: removed_unrelated,
				modified: modified_unrelated,
			},
		)
	}

	/// When exclusion prefixes are configured, autoplan classifies only the
	/// declared extension transition (paths under those prefixes). Unrelated
	/// managed-core snapshot drift is ignored for classification; fingerprints
	/// still hash the managed-core complement, and the full catalog is retained.
	fn scope_file_diff_for_autoplan(&self, diff: FileDiff) -> FileDiff {
		if self.exclude_prefixes.is_empty() {
			return diff;
		}
		self.partition_file_diff(diff).0
	}

	fn scope_catalog_diff_for_autoplan(&self, diff: CatalogDiff) -> CatalogDiff {
		if self.exclude_prefixes.is_empty() {
			return diff;
		}
		self.partition_catalog_diff(diff).0
	}
}

fn merge_file_diff(declared: FileDiff, unrelated: FileDiff) -> FileDiff {
	let mut added = declared.added;
	added.extend(unrelated.added);
	let mut modified = declared.modified;
	modified.extend(unrelated.modified);
	let mut removed = declared.removed;
	removed.extend(unrelated.removed);
	FileDiff {
		added,
		modified,
		removed,
	}
}

fn merge_catalog_diff(declared: CatalogDiff, unrelated: CatalogDiff) -> CatalogDiff {
	let mut added = declared.added;
	added.extend(unrelated.added);
	let mut removed = declared.removed;
	removed.extend(unrelated.removed);
	let mut modified = declared.modified;
	modified.extend(unrelated.modified);
	CatalogDiff {
		added,
		removed,
		modified,
	}
}

/// Public autoplan path: partition the declared transition *before* stale
/// baseline cleanup, then reconcile only the unrelated remainder.
fn prepare_autoplan_diffs(
	hash_scope: &RolloutHashScope,
	file_diff: FileDiff,
	catalog_diff: CatalogDiff,
) -> (FileDiff, CatalogDiff, bool) {
	let (declared_files, unrelated_files) = hash_scope.partition_file_diff(file_diff);
	let (declared_catalog, unrelated_catalog) = hash_scope.partition_catalog_diff(catalog_diff);
	let (cleaned_files, cleaned_catalog, stale) =
		reconcile_stale_baseline_for_autoplan(unrelated_files, unrelated_catalog);
	(
		merge_file_diff(declared_files, cleaned_files),
		merge_catalog_diff(declared_catalog, cleaned_catalog),
		stale,
	)
}

fn load_rollout_hash_scope() -> Result<RolloutHashScope> {
	let paths: Vec<&Path> =
		PROJECT_CONFIG_PATHS.iter().map(Path::new).filter(|path| path.is_file()).collect();
	match paths.as_slice() {
		[] => Ok(RolloutHashScope::default()),
		[path] => {
			let raw =
				fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
			parse_rollout_hash_scope(&raw, path)
		}
		_ => bail!(
			"multiple SurrealKit project manifests found ({}); keep one canonical manifest",
			paths.iter().map(|path| path.display().to_string()).collect::<Vec<_>>().join(", ")
		),
	}
}

fn parse_rollout_hash_scope(raw: &str, path: &Path) -> Result<RolloutHashScope> {
	let config: RolloutProjectConfig =
		toml::from_str(raw).with_context(|| format!("parsing {}", path.display()))?;
	let schema_prefix = format!("{}/", crate::schema_state::SCHEMA_DIR);
	let mut prefixes = Vec::new();
	for prefix in config.rollouts.target_hash_exclude_prefixes {
		if prefix.trim() != prefix
			|| prefix.contains('\\')
			|| !prefix.ends_with('/')
			|| prefix[..prefix.len().saturating_sub(1)].split('/').any(str::is_empty)
		{
			bail!(
				"invalid rollouts.target_hash_exclude_prefixes entry '{}' in {}: use a normalized project-relative directory prefix ending in '/'",
				prefix,
				path.display()
			);
		}
		let candidate = Path::new(&prefix);
		if candidate.is_absolute()
			|| candidate.components().any(|component| !matches!(component, Component::Normal(_)))
			|| !prefix.starts_with(&schema_prefix)
			|| prefix == schema_prefix
		{
			bail!(
				"invalid rollouts.target_hash_exclude_prefixes entry '{}' in {}: prefix must name a directory strictly below {}/",
				prefix,
				path.display(),
				crate::schema_state::SCHEMA_DIR
			);
		}
		prefixes.push(prefix);
	}
	prefixes.sort();
	prefixes.dedup();
	Ok(RolloutHashScope {
		exclude_prefixes: prefixes,
	})
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutPhase {
	Start,
	Complete,
	Rollback,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStepKind {
	ApplySchema,
	RunSql,
	AssertSql,
	RemoveEntities,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStatus {
	Planned,
	RunningStart,
	ReadyToComplete,
	RunningComplete,
	Completed,
	RunningRollback,
	RolledBack,
	Failed,
}

impl RolloutStatus {
	fn as_str(&self) -> &'static str {
		match self {
			Self::Planned => "planned",
			Self::RunningStart => "running_start",
			Self::ReadyToComplete => "ready_to_complete",
			Self::RunningComplete => "running_complete",
			Self::Completed => "completed",
			Self::RunningRollback => "running_rollback",
			Self::RolledBack => "rolled_back",
			Self::Failed => "failed",
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutSpec {
	pub id: String,
	pub name: String,
	pub source_schema_hash: String,
	pub target_schema_hash: String,
	pub compatibility: String,
	#[serde(default)]
	pub renames: Vec<RolloutRename>,
	#[serde(default)]
	pub steps: Vec<RolloutStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutRename {
	pub kind: String,
	pub scope: Option<String>,
	pub from: String,
	pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RolloutStep {
	pub id: String,
	pub phase: RolloutPhase,
	pub kind: RolloutStepKind,
	#[serde(default)]
	pub files: Vec<String>,
	pub sql: Option<String>,
	pub expect: Option<String>,
	#[serde(default)]
	pub entities: Vec<EntityKey>,
	pub idempotent: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct LoadedRolloutSpec {
	pub path: PathBuf,
	pub checksum: String,
	pub spec: RolloutSpec,
}

#[expect(dead_code)]
#[derive(Debug, Clone)]
pub struct ManagedEntityRecord {
	pub entity: CatalogEntity,
	pub active_rollout_id: Option<String>,
	pub state: String,
}

pub async fn run_baseline(db: &Surreal<Any>) -> Result<()> {
	run_setup(db).await?;
	ensure_local_state_dirs()?;
	if rollout_rows_exist(db).await? {
		bail!("rollout state already exists; baseline can only be run once");
	}

	let files = collect_schema_files()?;
	let schema_snapshot = snapshot_from_files(&files);
	let catalog_snapshot = build_catalog_snapshot(&files)?;

	replace_managed_entities(db, &catalog_snapshot.entities, None, "active").await?;
	replace_sync_hashes(db, &files).await?;
	save_schema_snapshot(&schema_snapshot)?;
	save_catalog_snapshot(&catalog_snapshot)?;

	println!(
		"Seeded managed entity baseline with {} schema file(s) and {} managed object(s).",
		files.len(),
		catalog_snapshot.entities.len()
	);
	Ok(())
}

pub async fn run_plan(opts: RolloutPlanOpts) -> Result<()> {
	ensure_local_state_dirs()?;
	let hash_scope = load_rollout_hash_scope()?;
	let files = collect_schema_files()?;
	let old_schema = load_schema_snapshot()?;
	let old_catalog = load_catalog_snapshot()?;
	let new_schema = snapshot_from_files(&files);
	let new_catalog = build_catalog_snapshot(&files)?;
	let file_diff = diff_schema(&old_schema, &new_schema);
	let catalog_diff =
		filter_normalization_false_positives(diff_catalog(&old_catalog, &new_catalog));

	// Partition the declared extension transition first. Stale local snapshots
	// (often left behind when schema was applied via sync outside plan) show
	// hundreds of unrelated "modified" entities; catch up that remainder and
	// keep every modified/removed entity in the declared transition so
	// validate_autoplan can still refuse them. Full parse/apply/catalog
	// retention is unchanged.
	let (file_diff, catalog_diff, stale_caught_up) =
		prepare_autoplan_diffs(&hash_scope, file_diff, catalog_diff);

	validate_autoplan(&catalog_diff)?;

	let name = opts.name.unwrap_or_else(|| "schema_rollout".to_string());
	let slug = slugify(&name);
	let ts = OffsetDateTime::now_utc()
		.format(&format_description!("[year][month][day][hour][minute][second]"))?;
	let rollout_id = format!("{ts}__{slug}");
	let path = Path::new(ROLLOUTS_DIR).join(format!("{rollout_id}.toml"));

	let spec = build_rollout_spec(
		&rollout_id,
		&name,
		&files,
		&file_diff,
		&catalog_diff,
		&old_schema,
		&new_schema,
		&hash_scope,
	)?;
	let raw = toml::to_string_pretty(&spec).context("serializing rollout spec")?;

	if stale_caught_up {
		eprintln!(
			"note: database/snapshots were stale relative to database/schema; \
caught up the rollout baseline and planning additive entities only \
(+{} file(s), +{} object(s)). Real TYPE/ASSERT edits on a fresh baseline \
still refuse automatic planning.",
			file_diff.added.len(),
			catalog_diff.added.len()
		);
	}

	if opts.dry_run {
		println!("Pending rollout plan:");
		println!(
			"  files: +{} ~{} -{}",
			file_diff.added.len(),
			file_diff.modified.len(),
			file_diff.removed.len()
		);
		println!(
			"  entities: +{} ~{} -{}",
			catalog_diff.added.len(),
			catalog_diff.modified.len(),
			catalog_diff.removed.len()
		);
		println!("  would create: {}", path.display());
		return Ok(());
	}

	fs::write(&path, raw).with_context(|| format!("writing rollout file {}", path.display()))?;
	save_schema_snapshot(&new_schema)?;
	save_catalog_snapshot(&new_catalog)?;

	println!("Generated rollout manifest {}", path.display());
	println!("Updated {}", CATALOG_SNAPSHOT_PATH);
	Ok(())
}

pub async fn run_lint(opts: RolloutExecutionOpts) -> Result<()> {
	ensure_local_state_dirs()?;
	let rollout = load_rollout_spec(resolve_rollout_path(opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	let files = collect_schema_files()?;
	let current_hash = load_rollout_hash_scope()?.hash_files(&files)?;
	if current_hash != rollout.spec.target_schema_hash {
		bail!(
			"target schema hash mismatch for '{}': manifest={}, current={}",
			rollout.spec.id,
			rollout.spec.target_schema_hash,
			current_hash
		);
	}
	println!("Rollout {} is valid (checksum {}).", rollout.spec.id, rollout.checksum);
	Ok(())
}

pub async fn run_status(db: &Surreal<Any>, selector: Option<String>) -> Result<()> {
	run_setup(db).await?;
	let mut query =
		"SELECT id, name, status, started_at, completed_at, last_error, steps FROM __rollout"
			.to_string();
	if selector.is_some() {
		query.push_str(" WHERE record::id(id) = $id");
	}
	query.push_str(" ORDER BY started_at DESC;");

	let mut req = db.query(query);
	if let Some(id) = selector {
		req = req.bind(("id", id));
	}
	let mut resp = req.await?;
	let raw_rows: Vec<surrealdb_types::Value> = resp.take(0)?;
	let rows: Vec<Value> =
		raw_rows.into_iter().map(|v| Value::from_value(v).unwrap_or(Value::Null)).collect();
	if rows.is_empty() {
		println!("No rollout records found.");
		return Ok(());
	}

	for row in rows {
		let id = string_field(&row, "id").unwrap_or_else(|| "<unknown>".to_string());
		let name = string_field(&row, "name").unwrap_or_else(|| "<unnamed>".to_string());
		let status = string_field(&row, "status").unwrap_or_else(|| "<unknown>".to_string());
		println!("{} [{}] {}", id, status, name);
		if let Some(started_at) = string_field(&row, "started_at") {
			println!("  started_at: {}", started_at);
		}
		if let Some(completed_at) = string_field(&row, "completed_at") {
			println!("  completed_at: {}", completed_at);
		}
		if let Some(last_error) = string_field(&row, "last_error") {
			println!("  last_error: {}", last_error);
		}

		let steps = row.get("steps").and_then(|v| v.as_array()).cloned().unwrap_or_default();
		for step in steps {
			let step_id = string_field(&step, "step_id").unwrap_or_else(|| "<step>".to_string());
			let phase = string_field(&step, "phase").unwrap_or_else(|| "?".to_string());
			let kind = string_field(&step, "kind").unwrap_or_else(|| "?".to_string());
			let status = string_field(&step, "status").unwrap_or_else(|| "?".to_string());
			println!("  - {} [{}:{}] {}", step_id, phase, kind, status);
			if let Some(err) = string_field(&step, "error") {
				println!("    error: {}", err);
			}
		}
	}
	Ok(())
}

pub async fn run_start(
	db: &Surreal<Any>,
	opts: RolloutExecutionOpts,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db).await?;
	ensure_local_state_dirs()?;
	let rollout = load_rollout_spec(resolve_rollout_path(opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	let files = collect_schema_files()?;
	let target_hash = load_rollout_hash_scope()?.hash_files(&files)?;
	if target_hash != rollout.spec.target_schema_hash {
		bail!(
			"target schema hash mismatch for '{}': manifest={}, current={}",
			rollout.spec.id,
			rollout.spec.target_schema_hash,
			target_hash
		);
	}
	let target_catalog = build_catalog_snapshot(&files)?;
	let source_entities = load_managed_entities(db).await?;
	let source_catalog = CatalogSnapshot {
		version: 2,
		entities: source_entities.into_iter().map(|r| r.entity).collect(),
	};
	start_inner(db, &rollout, &source_catalog, &target_catalog, vars).await
}

/// Runs the start phase of a rollout defined entirely in code.
///
/// `spec` describes the rollout steps. `target_files` is the desired schema state;
/// it is used to build the entity catalog and verify `spec.target_schema_hash`
/// when that field is non-empty.
///
/// `ApplySchema` steps in `spec` should carry their SurrealQL in the `sql` field
/// rather than in `files`, since no filesystem is read during execution.
///
/// `vars` is applied to step SQL (`ApplySchema`, `RunSql`, `AssertSql`) before execution.
/// Pass `&TemplateVars::default()` if no substitution is needed.
pub async fn run_start_with_spec(
	db: &Surreal<Any>,
	spec: &RolloutSpec,
	target_files: &[crate::sync::EmbeddedSchemaFile],
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db).await?;
	validate_rollout_spec(spec)?;
	let schema_files = embedded_to_schema_files(target_files);
	if !spec.target_schema_hash.is_empty() {
		let hash = hash_schema_snapshot(&snapshot_from_files(&schema_files))?;
		if hash != spec.target_schema_hash {
			bail!(
				"target schema hash mismatch for '{}': spec={}, files={}",
				spec.id,
				spec.target_schema_hash,
				hash
			);
		}
	}
	let target_catalog = build_catalog_snapshot(&schema_files)?;
	let source_entities = load_managed_entities(db).await?;
	let source_catalog = CatalogSnapshot {
		version: 2,
		entities: source_entities.into_iter().map(|r| r.entity).collect(),
	};
	start_inner(db, &make_loaded_spec(spec), &source_catalog, &target_catalog, vars).await
}

async fn start_inner(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	source_catalog: &CatalogSnapshot,
	target_catalog: &CatalogSnapshot,
	vars: &TemplateVars,
) -> Result<()> {
	acquire_lock(db, "global").await?;
	let result = async {
		ensure_no_conflicting_active_rollout(db, &rollout.spec.id).await?;
		let record = load_rollout_record(db, &rollout.spec.id).await?;
		match record.as_ref().and_then(|row| string_field(row, "status")).as_deref() {
			Some("completed") => bail!("rollout '{}' is already completed", rollout.spec.id),
			Some("rolled_back") => {
				bail!("rollout '{}' has already been rolled back", rollout.spec.id)
			}
			_ => {}
		}
		if let Some(ref row) = record {
			verify_rollout_record_matches(row, rollout)?;
		} else {
			create_rollout_record(
				db,
				rollout,
				&source_catalog.entities,
				&target_catalog.entities,
				RolloutStatus::Planned,
			)
			.await?;
		}
		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningStart, None, None).await?;
		if let Err(err) = execute_phase(db, rollout, RolloutPhase::Start, vars).await {
			set_rollout_status(
				db,
				&rollout.spec.id,
				RolloutStatus::Failed,
				Some(&format!("{err:#}")),
				None,
			)
			.await?;
			return Err(err);
		}
		set_rollout_status(db, &rollout.spec.id, RolloutStatus::ReadyToComplete, None, None)
			.await?;
		println!("Rollout {} is ready to complete.", rollout.spec.id);
		Ok(())
	}
	.await;
	let release = release_lock(db, "global").await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

pub async fn run_complete(
	db: &Surreal<Any>,
	opts: RolloutExecutionOpts,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db).await?;
	let rollout = load_rollout_spec(resolve_rollout_path(opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	complete_inner(db, &rollout, vars).await
}

/// Runs the complete phase of a rollout defined entirely in code.
///
/// The `spec` must be identical to the one passed to [`run_start_with_spec`].
/// `vars` is applied to step SQL before execution; pass `&TemplateVars::default()`
/// if no substitution is needed.
pub async fn run_complete_with_spec(
	db: &Surreal<Any>,
	spec: &RolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db).await?;
	validate_rollout_spec(spec)?;
	complete_inner(db, &make_loaded_spec(spec), vars).await
}

async fn complete_inner(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	acquire_lock(db, "global").await?;
	let result = async {
		let row = load_rollout_record(db, &rollout.spec.id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has not been started", rollout.spec.id))?;
		verify_rollout_record_matches(&row, rollout)?;
		match string_field(&row, "status").as_deref() {
			Some("ready_to_complete") | Some("running_complete") | Some("failed") => {}
			Some(other) => {
				bail!("rollout '{}' is not ready to complete (status={})", rollout.spec.id, other)
			}
			None => bail!("rollout '{}' has no status", rollout.spec.id),
		}
		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningComplete, None, None)
			.await?;
		if let Err(err) = execute_phase(db, rollout, RolloutPhase::Complete, vars).await {
			set_rollout_status(
				db,
				&rollout.spec.id,
				RolloutStatus::Failed,
				Some(&format!("{err:#}")),
				None,
			)
			.await?;
			return Err(err);
		}
		let target_entities = deserialize_entities_field(&row, "target_entities")?;
		replace_managed_entities(db, &target_entities, None, "active").await?;
		set_rollout_status(
			db,
			&rollout.spec.id,
			RolloutStatus::Completed,
			None,
			Some(OffsetDateTime::now_utc().format(&Rfc3339)?),
		)
		.await?;
		println!("Completed rollout {}.", rollout.spec.id);
		Ok(())
	}
	.await;
	let release = release_lock(db, "global").await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

pub async fn run_rollback(
	db: &Surreal<Any>,
	opts: RolloutExecutionOpts,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db).await?;
	let rollout = load_rollout_spec(resolve_rollout_path(opts.selector.as_deref())?)?;
	validate_rollout_spec(&rollout.spec)?;
	rollback_inner(db, &rollout, vars).await
}

/// Runs the rollback phase of a rollout defined entirely in code.
///
/// The `spec` must be identical to the one passed to [`run_start_with_spec`].
/// `vars` is applied to step SQL before execution; pass `&TemplateVars::default()`
/// if no substitution is needed.
pub async fn run_rollback_with_spec(
	db: &Surreal<Any>,
	spec: &RolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	run_setup(db).await?;
	validate_rollout_spec(spec)?;
	rollback_inner(db, &make_loaded_spec(spec), vars).await
}

async fn rollback_inner(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	vars: &TemplateVars,
) -> Result<()> {
	acquire_lock(db, "global").await?;
	let result = async {
		let row = load_rollout_record(db, &rollout.spec.id)
			.await?
			.ok_or_else(|| anyhow!("rollout '{}' has not been started", rollout.spec.id))?;
		verify_rollout_record_matches(&row, rollout)?;
		match string_field(&row, "status").as_deref() {
			Some("completed") => bail!("rollout '{}' is already completed", rollout.spec.id),
			Some("rolled_back") => {
				println!("Rollout {} is already rolled back.", rollout.spec.id);
				return Ok(());
			}
			_ => {}
		}
		set_rollout_status(db, &rollout.spec.id, RolloutStatus::RunningRollback, None, None)
			.await?;
		if let Err(err) = execute_phase(db, rollout, RolloutPhase::Rollback, vars).await {
			set_rollout_status(
				db,
				&rollout.spec.id,
				RolloutStatus::Failed,
				Some(&format!("{err:#}")),
				None,
			)
			.await?;
			return Err(err);
		}
		let source_entities = deserialize_entities_field(&row, "source_entities")?;
		replace_managed_entities(db, &source_entities, None, "active").await?;
		set_rollout_status(
			db,
			&rollout.spec.id,
			RolloutStatus::RolledBack,
			None,
			Some(OffsetDateTime::now_utc().format(&Rfc3339)?),
		)
		.await?;
		println!("Rolled back rollout {}.", rollout.spec.id);
		Ok(())
	}
	.await;
	let release = release_lock(db, "global").await;
	match (result, release) {
		(Err(err), _) => Err(err),
		(Ok(_), Err(err)) => Err(err),
		(Ok(value), Ok(())) => Ok(value),
	}
}

pub async fn load_active_rollout_id(db: &Surreal<Any>) -> Result<Option<String>> {
	let mut resp = db
		.query(
			"SELECT record::id(id) AS rollout_id, started_at FROM __rollout \
			 WHERE status INSIDE ['planned', 'running_start', 'ready_to_complete', 'running_complete', 'running_rollback', 'failed'] \
			 ORDER BY started_at DESC LIMIT 1;",
		)
		.await?;
	let raw: Option<surrealdb_types::Value> = resp.take(0)?;
	let row = raw.map(|v| Value::from_value(v).unwrap_or(Value::Null));
	Ok(row.and_then(|value| string_field(&value, "rollout_id")))
}

pub async fn load_managed_entities(db: &Surreal<Any>) -> Result<Vec<ManagedEntityRecord>> {
	let mut resp = db.query("SELECT key, val FROM __entity WHERE ns = 'schema';").await?;
	let rows: Vec<Value> = resp.take(0)?;
	let mut out = Vec::with_capacity(rows.len());
	for row in rows {
		let key = string_field_req(&row, "key")?;
		let val = row.get("val").cloned().unwrap_or(Value::Null);

		// key format: "kind:scope:name" (scope may be empty)
		let parts: Vec<&str> = key.splitn(3, ':').collect();
		if parts.len() < 3 {
			continue;
		}
		let kind = parts[0].to_string();
		let scope = if parts[1].is_empty() {
			None
		} else {
			Some(parts[1].to_string())
		};
		let name = parts[2].to_string();

		let source_path =
			val.get("source_path").and_then(|v| v.as_str()).unwrap_or_default().to_string();
		let statement_hash =
			val.get("statement_hash").and_then(|v| v.as_str()).unwrap_or_default().to_string();
		let file_hash =
			val.get("file_hash").and_then(|v| v.as_str()).unwrap_or_default().to_string();
		let active_rollout_id =
			val.get("active_rollout_id").and_then(|v| v.as_str()).map(str::to_string);
		let state = val.get("state").and_then(|v| v.as_str()).unwrap_or("active").to_string();

		out.push(ManagedEntityRecord {
			entity: CatalogEntity {
				kind,
				scope,
				name,
				source_path,
				statement_hash,
				file_hash,
			},
			active_rollout_id,
			state,
		});
	}
	out.sort_by(|a, b| a.entity.cmp(&b.entity));
	Ok(out)
}

pub async fn upsert_managed_entities(
	db: &Surreal<Any>,
	entities: &[CatalogEntity],
	active_rollout_id: Option<&str>,
	state: &str,
) -> Result<()> {
	for entity in entities {
		let entity_key = entity_key_string(&entity.kind, entity.scope.as_deref(), &entity.name);
		// UPSERT … WHERE is one statement against the unique (ns,key) index — no
		// DELETE+CREATE gap for a concurrent writer to observe a stale unique hit.
		db.query(
			"UPSERT __entity SET \
			 	ns = 'schema', \
			 	key = $key, \
			 	val = { \
			 		source_path: $source_path, \
			 		statement_hash: $statement_hash, \
			 		file_hash: $file_hash, \
			 		active_rollout_id: $active_rollout_id, \
			 		state: $state \
			 	}, \
			 	updated_at = time::now() \
			 WHERE ns = 'schema' AND key = $key;",
		)
		.bind(("key", entity_key))
		.bind(("source_path", entity.source_path.clone()))
		.bind(("statement_hash", entity.statement_hash.clone()))
		.bind(("file_hash", entity.file_hash.clone()))
		.bind(("active_rollout_id", active_rollout_id.map(str::to_string)))
		.bind(("state", state.to_string()))
		.await?
		.check()?;
	}
	Ok(())
}

fn entity_key_string(kind: &str, scope: Option<&str>, name: &str) -> String {
	format!("{}:{}:{}", kind, scope.unwrap_or(""), name)
}

pub async fn delete_managed_entities(db: &Surreal<Any>, entities: &[EntityKey]) -> Result<()> {
	for entity in entities {
		let key = entity_key_string(&entity.kind, entity.scope.as_deref(), &entity.name);
		db.query("DELETE __entity WHERE ns = 'schema' AND key = $key;")
			.bind(("key", key))
			.await?
			.check()?;
	}
	Ok(())
}

pub async fn replace_managed_entities(
	db: &Surreal<Any>,
	entities: &[CatalogEntity],
	active_rollout_id: Option<&str>,
	state: &str,
) -> Result<()> {
	db.query("DELETE __entity WHERE ns = 'schema';").await?.check()?;
	upsert_managed_entities(db, entities, active_rollout_id, state).await
}

pub async fn replace_sync_hashes(db: &Surreal<Any>, files: &[SchemaFile]) -> Result<()> {
	db.query("DELETE __entity WHERE ns = 'sync';").await?.check()?;
	for file in files {
		db.query(
			"CREATE __entity CONTENT { ns: 'sync', key: $path, val: { hash: $hash }, updated_at: time::now() };",
		)
		.bind(("path", file.path.clone()))
		.bind(("hash", file.hash.clone()))
		.await?
		.check()?;
	}
	Ok(())
}

pub async fn delete_sync_hashes(db: &Surreal<Any>, paths: &[String]) -> Result<()> {
	for path in paths {
		db.query("DELETE __entity WHERE ns = 'sync' AND key = $path;")
			.bind(("path", path.clone()))
			.await?
			.check()?;
	}
	Ok(())
}

fn build_rollout_spec(
	rollout_id: &str,
	name: &str,
	files: &[SchemaFile],
	file_diff: &FileDiff,
	catalog_diff: &CatalogDiff,
	old_schema: &crate::schema_state::SchemaSnapshot,
	new_schema: &crate::schema_state::SchemaSnapshot,
	hash_scope: &RolloutHashScope,
) -> Result<RolloutSpec> {
	let changed_paths = changed_files(files, file_diff);
	let mut steps = Vec::new();
	if !changed_paths.is_empty() {
		steps.push(RolloutStep {
			id: "apply_expand_schema".to_string(),
			phase: RolloutPhase::Start,
			kind: RolloutStepKind::ApplySchema,
			files: changed_paths,
			sql: None,
			expect: None,
			entities: Vec::new(),
			idempotent: None,
		});
	}

	let added_entities: Vec<EntityKey> =
		catalog_diff.added.iter().map(CatalogEntity::key).collect();
	if !added_entities.is_empty() {
		steps.push(RolloutStep {
			id: "rollback_expand_schema".to_string(),
			phase: RolloutPhase::Rollback,
			kind: RolloutStepKind::RemoveEntities,
			files: Vec::new(),
			sql: None,
			expect: None,
			entities: added_entities,
			idempotent: None,
		});
	}

	let removed_entities: Vec<EntityKey> =
		catalog_diff.removed.iter().map(CatalogEntity::key).collect();
	if !removed_entities.is_empty() {
		steps.push(RolloutStep {
			id: "remove_legacy_entities".to_string(),
			phase: RolloutPhase::Complete,
			kind: RolloutStepKind::RemoveEntities,
			files: Vec::new(),
			sql: None,
			expect: None,
			entities: removed_entities,
			idempotent: None,
		});
	}

	Ok(RolloutSpec {
		id: rollout_id.to_string(),
		name: name.to_string(),
		source_schema_hash: hash_scope.hash_snapshot(old_schema)?,
		target_schema_hash: hash_scope.hash_snapshot(new_schema)?,
		compatibility: "phased".to_string(),
		renames: Vec::new(),
		steps,
	})
}

/// Drop catalog "modifications" whose defining file bytes did not change.
///
/// `statement_hash` can drift from normalization alone while `file_hash` stays
/// equal — that is not a real schema edit and must not trip autoplan.
fn filter_normalization_false_positives(diff: CatalogDiff) -> CatalogDiff {
	CatalogDiff {
		added: diff.added,
		removed: diff.removed,
		modified: diff
			.modified
			.into_iter()
			.filter(|change| change.old.file_hash != change.new.file_hash)
			.collect(),
	}
}

/// Wholesale snapshot drift vs `database/schema` (Tamera / T001127 shape).
///
/// Small intentional edits stay on the precise refusal path; large modified
/// counts alongside many changed files mean the local snapshots lagged, not
/// that the caller's additive files are non-additive.
fn looks_like_stale_baseline(file_diff: &FileDiff, catalog_diff: &CatalogDiff) -> bool {
	const MIN_MODIFIED_ENTITIES: usize = 10;
	const MIN_MODIFIED_FILES: usize = 5;
	catalog_diff.modified.len() >= MIN_MODIFIED_ENTITIES
		&& file_diff.modified.len() >= MIN_MODIFIED_FILES
}

/// When snapshots are stale, plan only additive work and let snapshot save
/// catch up; keep real in-file modifications refused on a fresh baseline.
fn reconcile_stale_baseline_for_autoplan(
	file_diff: FileDiff,
	catalog_diff: CatalogDiff,
) -> (FileDiff, CatalogDiff, bool) {
	if !looks_like_stale_baseline(&file_diff, &catalog_diff) {
		return (file_diff, catalog_diff, false);
	}
	(
		FileDiff {
			added: file_diff.added,
			modified: Vec::new(),
			removed: Vec::new(),
		},
		CatalogDiff {
			added: catalog_diff.added,
			modified: Vec::new(),
			removed: Vec::new(),
		},
		true,
	)
}

fn entity_label(kind: &str, scope: &Option<String>, name: &str) -> String {
	match scope {
		Some(scope) => format!("{kind}:{scope}.{name}"),
		None => format!("{kind}:{name}"),
	}
}

fn validate_autoplan(diff: &CatalogDiff) -> Result<()> {
	let modified_names = if diff.modified.is_empty() {
		None
	} else {
		Some(
			diff.modified
				.iter()
				.map(|change| entity_label(&change.old.kind, &change.old.scope, &change.old.name))
				.collect::<Vec<_>>()
				.join(", "),
		)
	};
	let removed_names = if diff.removed.is_empty() {
		None
	} else {
		Some(
			diff.removed
				.iter()
				.map(|entity| entity_label(&entity.kind, &entity.scope, &entity.name))
				.collect::<Vec<_>>()
				.join(", "),
		)
	};
	match (modified_names, removed_names) {
		(None, None) => Ok(()),
		(Some(modified), None) => bail!(
			"automatic rollout planning refuses modified managed entities: {}. \
Author a manual rollout manifest for non-additive changes.",
			modified
		),
		(None, Some(removed)) => bail!(
			"automatic rollout planning refuses removed managed entities: {}. \
Author a manual rollout manifest for non-additive changes.",
			removed
		),
		(Some(modified), Some(removed)) => bail!(
			"automatic rollout planning refuses modified managed entities: {}; \
refuses removed managed entities: {}. \
Author a manual rollout manifest for non-additive changes.",
			modified,
			removed
		),
	}
}

fn changed_files(files: &[SchemaFile], diff: &FileDiff) -> Vec<String> {
	let changed: BTreeSet<&str> =
		diff.added.iter().chain(diff.modified.iter()).map(String::as_str).collect();
	let mut out: Vec<String> = files
		.iter()
		.filter(|file| changed.contains(file.path.as_str()))
		.map(|file| file.path.clone())
		.collect();
	out.sort();
	out
}

fn make_loaded_spec(spec: &RolloutSpec) -> LoadedRolloutSpec {
	let checksum = sha256_hex(toml::to_string_pretty(spec).unwrap_or_default().as_bytes());
	LoadedRolloutSpec {
		path: PathBuf::from(format!("embedded:{}", spec.id)),
		checksum,
		spec: spec.clone(),
	}
}

fn embedded_to_schema_files(files: &[crate::sync::EmbeddedSchemaFile]) -> Vec<SchemaFile> {
	files
		.iter()
		.map(|f| SchemaFile {
			path: f.path.to_string(),
			sql: f.sql.to_string(),
			hash: sha256_hex(f.sql.as_bytes()),
		})
		.collect()
}

fn load_rollout_spec(path: PathBuf) -> Result<LoadedRolloutSpec> {
	let raw = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
	let spec: RolloutSpec =
		toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
	Ok(LoadedRolloutSpec {
		path,
		checksum: sha256_hex(raw.as_bytes()),
		spec,
	})
}

fn resolve_rollout_path(selector: Option<&str>) -> Result<PathBuf> {
	let selector = selector.ok_or_else(|| anyhow!("rollout id or path is required"))?;
	let path = Path::new(selector);
	if path.exists() {
		return Ok(path.to_path_buf());
	}
	let direct = Path::new(ROLLOUTS_DIR).join(selector);
	if direct.exists() {
		return Ok(direct);
	}
	let with_ext = Path::new(ROLLOUTS_DIR).join(format!("{selector}.toml"));
	if with_ext.exists() {
		return Ok(with_ext);
	}
	bail!("unable to find rollout '{}'", selector)
}

fn validate_rollout_spec(spec: &RolloutSpec) -> Result<()> {
	if spec.id.trim().is_empty() {
		bail!("rollout id is required");
	}
	if spec.name.trim().is_empty() {
		bail!("rollout name is required");
	}
	if spec.compatibility.trim().is_empty() {
		bail!("compatibility is required");
	}

	let mut step_ids = BTreeSet::new();
	for step in &spec.steps {
		if !step_ids.insert(step.id.clone()) {
			bail!("duplicate rollout step id '{}'", step.id);
		}
		match step.kind {
			RolloutStepKind::ApplySchema => {
				let has_files = !step.files.is_empty();
				let has_sql = step.sql.as_deref().is_some_and(|s| !s.trim().is_empty());
				if !has_files && !has_sql {
					bail!("apply_schema step '{}' requires either files or sql", step.id);
				}
			}
			RolloutStepKind::RunSql => {
				if step.sql.as_deref().unwrap_or("").trim().is_empty() {
					bail!("run_sql step '{}' requires sql", step.id);
				}
				if step.idempotent != Some(true) {
					bail!("run_sql step '{}' must declare idempotent = true", step.id);
				}
			}
			RolloutStepKind::AssertSql => {
				if step.sql.as_deref().unwrap_or("").trim().is_empty() {
					bail!("assert_sql step '{}' requires sql", step.id);
				}
				if step.expect.as_deref().unwrap_or("").trim().is_empty() {
					bail!("assert_sql step '{}' requires expect", step.id);
				}
			}
			RolloutStepKind::RemoveEntities => {
				if step.entities.is_empty() {
					bail!("remove_entities step '{}' requires entities", step.id);
				}
			}
		}
	}
	Ok(())
}

async fn execute_phase(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	phase: RolloutPhase,
	vars: &TemplateVars,
) -> Result<()> {
	for step in rollout.spec.steps.iter().filter(|step| step.phase == phase) {
		if step_already_completed(db, &rollout.spec.id, &step.id).await? {
			continue;
		}

		record_step_start(db, &rollout.spec.id, step).await?;
		let result = execute_step(db, step, vars).await;
		match result {
			Ok(()) => record_step_complete(db, &rollout.spec.id, step).await?,
			Err(err) => {
				record_step_failure(db, &rollout.spec.id, step, &format!("{err:#}")).await?;
				return Err(err);
			}
		}
	}
	Ok(())
}

async fn execute_step(db: &Surreal<Any>, step: &RolloutStep, vars: &TemplateVars) -> Result<()> {
	match step.kind {
		RolloutStepKind::ApplySchema => {
			if !step.files.is_empty() {
				for file in &step.files {
					let raw =
						fs::read_to_string(file).with_context(|| format!("reading {}", file))?;
					let substituted = vars
						.apply(&raw)
						.with_context(|| format!("applying template variables in {}", file))?;
					exec_surql(db, &ensure_overwrite(&substituted)).await?;
				}
			} else if let Some(ref sql) = step.sql {
				let substituted = vars.apply(sql)?;
				exec_surql(db, &ensure_overwrite(&substituted)).await?;
			}
			Ok(())
		}
		RolloutStepKind::RunSql => {
			let sql = step.sql.as_deref().ok_or_else(|| anyhow!("missing sql"))?;
			let substituted = vars.apply(sql)?;
			exec_surql(db, &substituted).await
		}
		RolloutStepKind::AssertSql => {
			let sql = step.sql.as_deref().ok_or_else(|| anyhow!("missing sql"))?;
			let expect = step.expect.as_deref().ok_or_else(|| anyhow!("missing expect"))?;
			let substituted = vars.apply(sql)?;
			let actual = execute_sql_value(db, &substituted).await?;
			if value_to_expect_string(&actual) != expect.trim() {
				bail!(
					"assert step '{}' failed: expected {}, got {}",
					step.id,
					expect,
					value_to_expect_string(&actual)
				);
			}
			Ok(())
		}
		RolloutStepKind::RemoveEntities => {
			let sql = render_remove_sql(&step.entities, true)?.join("\n");
			if sql.trim().is_empty() {
				return Ok(());
			}
			exec_surql(db, &sql).await
		}
	}
}

async fn execute_sql_value(db: &Surreal<Any>, sql: &str) -> Result<Value> {
	let mut response = db.query(sql).await?.check()?;
	let raw: surrealdb_types::Value = response.take(0)?;
	Ok(Value::from_value(raw).unwrap_or(Value::Null))
}

fn value_to_expect_string(value: &Value) -> String {
	match value {
		Value::Null => "null".to_string(),
		Value::Bool(v) => v.to_string(),
		Value::Number(v) => v.to_string(),
		Value::String(v) => v.clone(),
		other => other.to_string(),
	}
}

async fn rollout_rows_exist(db: &Surreal<Any>) -> Result<bool> {
	let mut resp = db.query("SELECT id FROM __rollout LIMIT 1;").await?;
	let row: Option<Value> = resp.take(0)?;
	Ok(row.is_some())
}

async fn ensure_no_conflicting_active_rollout(db: &Surreal<Any>, rollout_id: &str) -> Result<()> {
	if let Some(active_id) = load_active_rollout_id(db).await?
		&& active_id != rollout_id
	{
		bail!("rollout '{}' cannot start while rollout '{}' is active", rollout_id, active_id);
	}
	Ok(())
}

async fn create_rollout_record(
	db: &Surreal<Any>,
	rollout: &LoadedRolloutSpec,
	source_entities: &[CatalogEntity],
	target_entities: &[CatalogEntity],
	status: RolloutStatus,
) -> Result<()> {
	let started_at = OffsetDateTime::now_utc().format(&Rfc3339)?;
	db.query(
		"DELETE __rollout WHERE record::id(id) = $id; \
		 CREATE __rollout CONTENT { \
		 	id: $id, \
		 	name: $name, \
		 	manifest_path: $manifest_path, \
		 	manifest_checksum: $manifest_checksum, \
		 	source_schema_hash: $source_schema_hash, \
		 	target_schema_hash: $target_schema_hash, \
		 	status: $status, \
		 	source_entities: $source_entities, \
		 	target_entities: $target_entities, \
		 	started_at: <datetime> $started_at, \
		 	updated_at: time::now(), \
		 	last_error: NONE \
		 };",
	)
	.bind(("id", rollout.spec.id.clone()))
	.bind(("name", rollout.spec.name.clone()))
	.bind(("manifest_path", rollout.path.to_string_lossy().to_string()))
	.bind(("manifest_checksum", rollout.checksum.clone()))
	.bind(("source_schema_hash", rollout.spec.source_schema_hash.clone()))
	.bind(("target_schema_hash", rollout.spec.target_schema_hash.clone()))
	.bind(("status", status.as_str().to_string()))
	.bind(("source_entities", serde_json::to_value(source_entities)?))
	.bind(("target_entities", serde_json::to_value(target_entities)?))
	.bind(("started_at", started_at))
	.await?
	.check()?;
	Ok(())
}

async fn load_rollout_record(db: &Surreal<Any>, rollout_id: &str) -> Result<Option<Value>> {
	let mut resp = db
		.query("SELECT * FROM __rollout WHERE record::id(id) = $id LIMIT 1;")
		.bind(("id", rollout_id.to_string()))
		.await?;
	let raw: Option<surrealdb_types::Value> = resp.take(0)?;
	Ok(raw.map(|v| Value::from_value(v).unwrap_or(Value::Null)))
}

fn verify_rollout_record_matches(row: &Value, rollout: &LoadedRolloutSpec) -> Result<()> {
	let checksum = string_field_req(row, "manifest_checksum")?;
	if checksum != rollout.checksum {
		bail!(
			"manifest checksum mismatch for '{}': db={}, file={}",
			rollout.spec.id,
			checksum,
			rollout.checksum
		);
	}
	let source = string_field_req(row, "source_schema_hash")?;
	let target = string_field_req(row, "target_schema_hash")?;
	if source != rollout.spec.source_schema_hash || target != rollout.spec.target_schema_hash {
		bail!("schema hash mismatch for rollout '{}'", rollout.spec.id);
	}
	Ok(())
}

fn deserialize_entities_field(row: &Value, key: &str) -> Result<Vec<CatalogEntity>> {
	let value =
		row.get(key).cloned().ok_or_else(|| anyhow!("missing '{}' on rollout record", key))?;
	serde_json::from_value(value).with_context(|| format!("parsing {}", key))
}

async fn set_rollout_status(
	db: &Surreal<Any>,
	rollout_id: &str,
	status: RolloutStatus,
	last_error: Option<&str>,
	completed_at: Option<String>,
) -> Result<()> {
	db.query(
		"UPDATE __rollout SET \
		 	status = $status, \
		 	last_error = $last_error, \
		 	completed_at = IF $completed_at THEN <datetime> $completed_at ELSE NONE END, \
		 	updated_at = time::now() \
		 WHERE record::id(id) = $id;",
	)
	.bind(("id", rollout_id.to_string()))
	.bind(("status", status.as_str().to_string()))
	.bind(("last_error", last_error.map(str::to_string)))
	.bind(("completed_at", completed_at))
	.await?
	.check()?;
	Ok(())
}

async fn step_already_completed(
	db: &Surreal<Any>,
	rollout_id: &str,
	step_id: &str,
) -> Result<bool> {
	let row = load_rollout_record(db, rollout_id).await?;
	let Some(row) = row else {
		return Ok(false);
	};
	let steps = row.get("steps").and_then(|v| v.as_array());
	Ok(steps
		.map(|arr| {
			arr.iter().any(|s| {
				s.get("step_id").and_then(|v| v.as_str()) == Some(step_id)
					&& s.get("status").and_then(|v| v.as_str()) == Some("completed")
			})
		})
		.unwrap_or(false))
}

async fn record_step_start(db: &Surreal<Any>, rollout_id: &str, step: &RolloutStep) -> Result<()> {
	let new_step = serde_json::json!({
		"step_id": step.id,
		"phase": format!("{:?}", step.phase).to_ascii_lowercase(),
		"kind": format!("{:?}", step.kind).to_ascii_lowercase(),
		"checksum": step_checksum(step)?,
		"status": "running",
		"error": null
	});
	// Load, remove any existing entry for this step, append, write back
	let row = load_rollout_record(db, rollout_id)
		.await?
		.ok_or_else(|| anyhow!("rollout '{}' not found", rollout_id))?;
	let mut steps: Vec<Value> =
		row.get("steps").and_then(|v| v.as_array()).cloned().unwrap_or_default();
	steps.retain(|s| s.get("step_id").and_then(|v| v.as_str()) != Some(&step.id));
	steps.push(new_step);
	db.query(
		"UPDATE __rollout SET steps = $steps, updated_at = time::now() \
		 WHERE record::id(id) = $id;",
	)
	.bind(("id", rollout_id.to_string()))
	.bind(("steps", steps))
	.await?
	.check()?;
	Ok(())
}

async fn record_step_complete(
	db: &Surreal<Any>,
	rollout_id: &str,
	step: &RolloutStep,
) -> Result<()> {
	update_step_status(db, rollout_id, &step.id, "completed", None).await
}

async fn record_step_failure(
	db: &Surreal<Any>,
	rollout_id: &str,
	step: &RolloutStep,
	error: &str,
) -> Result<()> {
	update_step_status(db, rollout_id, &step.id, "failed", Some(error)).await
}

async fn update_step_status(
	db: &Surreal<Any>,
	rollout_id: &str,
	step_id: &str,
	status: &str,
	error: Option<&str>,
) -> Result<()> {
	// Load, patch in Rust, write back — avoids complex inline array mutation
	let row = load_rollout_record(db, rollout_id)
		.await?
		.ok_or_else(|| anyhow!("rollout '{}' not found", rollout_id))?;
	let mut steps: Vec<Value> =
		row.get("steps").and_then(|v| v.as_array()).cloned().unwrap_or_default();
	for s in &mut steps {
		if s.get("step_id").and_then(|v| v.as_str()) == Some(step_id)
			&& let Some(obj) = s.as_object_mut()
		{
			obj.insert("status".into(), Value::String(status.to_string()));
			obj.insert(
				"error".into(),
				error.map(|e| Value::String(e.to_string())).unwrap_or(Value::Null),
			);
		}
	}
	db.query(
		"UPDATE __rollout SET steps = $steps, updated_at = time::now() \
		 WHERE record::id(id) = $id;",
	)
	.bind(("id", rollout_id.to_string()))
	.bind(("steps", steps))
	.await?
	.check()?;
	Ok(())
}

fn step_checksum(step: &RolloutStep) -> Result<String> {
	let raw = serde_json::to_vec(step).context("serializing rollout step")?;
	Ok(sha256_hex(&raw))
}

/// Default stale lock TTL: a crashed holder older than this can be reclaimed.
/// Override with `SURREALKIT_LOCK_STALE_SECS` (tests use a short TTL).
pub const LOCK_STALE: std::time::Duration = std::time::Duration::from_secs(300);
/// Bounded wait when another live apply holds the lock.
pub const LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
const LOCK_POLL: std::time::Duration = std::time::Duration::from_millis(100);

static LOCK_OWNER_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

struct HeldLock {
	owner: String,
	renew_abort: tokio::task::AbortHandle,
}

fn held_locks() -> &'static Mutex<HashMap<String, HeldLock>> {
	static HELD: OnceLock<Mutex<HashMap<String, HeldLock>>> = OnceLock::new();
	HELD.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_stale_duration() -> std::time::Duration {
	match std::env::var("SURREALKIT_LOCK_STALE_SECS") {
		Ok(raw) => match raw.parse::<u64>() {
			Ok(secs) if secs > 0 => std::time::Duration::from_secs(secs),
			_ => LOCK_STALE,
		},
		Err(_) => LOCK_STALE,
	}
}

fn lock_record_key(lock_key: &str) -> String {
	let mut out = String::from("lock_");
	for ch in lock_key.chars() {
		if ch.is_ascii_alphanumeric() {
			out.push(ch.to_ascii_lowercase());
		} else {
			out.push('_');
		}
	}
	out
}

fn lock_owner_id() -> String {
	let base = std::env::var("SURREALKIT_OWNER").unwrap_or_else(|_| "surrealkit".to_string());
	let seq = LOCK_OWNER_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	format!("{base}:{}:{seq}", std::process::id())
}

/// Compare-and-set schema-apply lock with an owner-bound renewable lease.
///
/// A live holder renews `updated_at` so a long apply cannot be reaped at the
/// stale TTL. A stale holder (`updated_at` older than the TTL) is reclaimed —
/// including pre-deterministic random `__entity` record ids that share the
/// same `ns`/`key`. Release only deletes rows owned by this acquire.
pub async fn acquire_lock(db: &Surreal<Any>, lock_key: &str) -> Result<()> {
	let owner = lock_owner_id();
	let record_key = lock_record_key(lock_key);
	let stale = lock_stale_duration();
	let deadline = tokio::time::Instant::now() + LOCK_WAIT;
	loop {
		// Reclaim expired holders by ns/key (covers legacy random record ids)
		// and by the deterministic id used since aieus.10.
		db.query(&format!(
			"DELETE __entity WHERE ns = 'lock' AND key = $key AND updated_at < time::now() - {}s; \
			 DELETE type::record('__entity', $id) WHERE updated_at < time::now() - {}s;",
			stale.as_secs(),
			stale.as_secs()
		))
		.bind(("key", lock_key.to_string()))
		.bind(("id", record_key.clone()))
		.await?
		.check()?;

		let create = db
			.query(
				"CREATE ONLY type::record('__entity', $id) CONTENT { \
				 	ns: 'lock', \
				 	key: $key, \
				 	val: { owner: $owner }, \
				 	updated_at: time::now() \
				 };",
			)
			.bind(("id", record_key.clone()))
			.bind(("key", lock_key.to_string()))
			.bind(("owner", owner.clone()))
			.await;

		match create {
			Ok(resp) => match resp.check() {
				Ok(_) => {
					start_lock_renewal(db.clone(), lock_key, &record_key, &owner, stale);
					return Ok(());
				}
				Err(err) if is_lock_contention(&err.to_string()) => {
					// Client may have lost the success response while the row
					// landed — only treat as ours when owner matches THIS acquire.
					if lock_held_by_us(db, &record_key, &owner).await? {
						start_lock_renewal(db.clone(), lock_key, &record_key, &owner, stale);
						return Ok(());
					}
					if tokio::time::Instant::now() >= deadline {
						bail!(
							"could not acquire SurrealKit lock '{lock_key}' within {}s (held or contended)",
							LOCK_WAIT.as_secs()
						);
					}
					tokio::time::sleep(LOCK_POLL).await;
				}
				Err(err) => return Err(err.into()),
			},
			Err(err) if is_lock_contention(&err.to_string()) => {
				if lock_held_by_us(db, &record_key, &owner).await? {
					start_lock_renewal(db.clone(), lock_key, &record_key, &owner, stale);
					return Ok(());
				}
				if tokio::time::Instant::now() >= deadline {
					bail!(
						"could not acquire SurrealKit lock '{lock_key}' within {}s (held or contended)",
						LOCK_WAIT.as_secs()
					);
				}
				tokio::time::sleep(LOCK_POLL).await;
			}
			Err(err) => return Err(err.into()),
		}
	}
}

fn start_lock_renewal(
	db: Surreal<Any>,
	lock_key: &str,
	record_key: &str,
	owner: &str,
	stale: std::time::Duration,
) {
	let renew_every = (stale / 3).max(std::time::Duration::from_millis(50));
	let record_key = record_key.to_string();
	let owner_for_task = owner.to_string();
	let owner_for_map = owner.to_string();
	let handle = tokio::spawn(async move {
		loop {
			tokio::time::sleep(renew_every).await;
			let updated = db
				.query(
					"UPDATE type::record('__entity', $id) SET updated_at = time::now() \
					 WHERE val.owner = $owner RETURN id;",
				)
				.bind(("id", record_key.clone()))
				.bind(("owner", owner_for_task.clone()))
				.await;
			let Ok(mut resp) = updated else {
				break;
			};
			let Ok(rows): Result<Vec<serde_json::Value>, _> = resp.take(0) else {
				break;
			};
			if rows.is_empty() {
				// Lost the lease (reaped or overwritten) — stop renewing.
				break;
			}
		}
	});
	let mut map = held_locks().lock().expect("held lock map");
	if let Some(prev) = map.insert(
		lock_key.to_string(),
		HeldLock {
			owner: owner_for_map,
			renew_abort: handle.abort_handle(),
		},
	) {
		prev.renew_abort.abort();
	}
}

async fn lock_held_by_us(db: &Surreal<Any>, record_key: &str, owner: &str) -> Result<bool> {
	let mut resp = db
		.query("SELECT val FROM type::record('__entity', $id);")
		.bind(("id", record_key.to_string()))
		.await?;
	let rows: Vec<serde_json::Value> = resp.take(0)?;
	Ok(rows
		.first()
		.and_then(|row| row.get("val"))
		.and_then(|val| val.get("owner"))
		.and_then(|v| v.as_str())
		== Some(owner))
}

fn is_lock_contention(message: &str) -> bool {
	message.contains("already contains")
		|| message.contains("Resource busy")
		|| message.contains("Transaction conflict")
		|| message.contains("already exists")
}

/// Release only the lock owned by this process's matching acquire. A former
/// holder cannot delete a successor's lease after stale reclaim.
pub async fn release_lock(db: &Surreal<Any>, lock_key: &str) -> Result<()> {
	let held = {
		let mut map = held_locks().lock().expect("held lock map");
		map.remove(lock_key)
	};
	let Some(held) = held else {
		return Ok(());
	};
	held.renew_abort.abort();
	let record_key = lock_record_key(lock_key);
	db.query(
		"DELETE type::record('__entity', $id) WHERE val.owner = $owner; \
		 DELETE __entity WHERE ns = 'lock' AND key = $key AND val.owner = $owner;",
	)
	.bind(("id", record_key))
	.bind(("key", lock_key.to_string()))
	.bind(("owner", held.owner))
	.await?
	.check()?;
	Ok(())
}

fn slugify(input: &str) -> String {
	let mut out = String::new();
	let mut prev_dash = false;
	for ch in input.chars() {
		let c = ch.to_ascii_lowercase();
		if c.is_ascii_alphanumeric() {
			out.push(c);
			prev_dash = false;
		} else if !prev_dash {
			out.push('_');
			prev_dash = true;
		}
	}
	let trimmed = out.trim_matches('_');
	if trimmed.is_empty() {
		"schema_rollout".to_string()
	} else {
		trimmed.to_string()
	}
}

fn string_field(row: &Value, key: &str) -> Option<String> {
	row.get(key).and_then(|value| value.as_str()).map(str::to_string)
}

fn string_field_req(row: &Value, key: &str) -> Result<String> {
	string_field(row, key).ok_or_else(|| anyhow!("missing '{}' in database row", key))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema_state::{CatalogChange, SchemaSnapshot, SchemaSnapshotEntry};

	#[test]
	fn plan_rejects_modified_entities() {
		let diff = CatalogDiff {
			added: Vec::new(),
			removed: Vec::new(),
			modified: vec![CatalogChange {
				old: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "name".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "a".to_string(),
					file_hash: "fa".to_string(),
				},
				new: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "name".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "b".to_string(),
					file_hash: "fb".to_string(),
				},
			}],
		};

		let err = validate_autoplan(&diff).expect_err("should reject modified entities");
		assert!(err.to_string().contains("refuses modified"));
	}

	#[test]
	fn normalization_false_positive_same_file_hash_is_dropped() {
		let diff = filter_normalization_false_positives(CatalogDiff {
			added: Vec::new(),
			removed: Vec::new(),
			modified: vec![CatalogChange {
				old: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "name".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "a".to_string(),
					file_hash: "same".to_string(),
				},
				new: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "name".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "b".to_string(),
					file_hash: "same".to_string(),
				},
			}],
		});
		assert!(diff.modified.is_empty());
		validate_autoplan(&diff).expect("normalization-only drift must not refuse");
	}

	#[test]
	fn stale_baseline_reconcile_keeps_additive_and_clears_false_modified() {
		let mut modified_entities = Vec::new();
		for i in 0..12 {
			modified_entities.push(CatalogChange {
				old: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: format!("f{i}"),
					source_path: format!("database/schema/old_{i}.surql"),
					statement_hash: "a".to_string(),
					file_hash: format!("old-{i}"),
				},
				new: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: format!("f{i}"),
					source_path: format!("database/schema/old_{i}.surql"),
					statement_hash: "b".to_string(),
					file_hash: format!("new-{i}"),
				},
			});
		}
		let file_diff = FileDiff {
			added: vec!["database/schema/new_additive.surql".to_string()],
			modified: (0..6).map(|i| format!("database/schema/old_{i}.surql")).collect(),
			removed: Vec::new(),
		};
		let catalog_diff = CatalogDiff {
			added: vec![CatalogEntity {
				kind: "table".to_string(),
				scope: None,
				name: "new_additive".to_string(),
				source_path: "database/schema/new_additive.surql".to_string(),
				statement_hash: "n".to_string(),
				file_hash: "n".to_string(),
			}],
			removed: Vec::new(),
			modified: modified_entities,
		};
		let (files, entities, stale) =
			reconcile_stale_baseline_for_autoplan(file_diff, catalog_diff);
		assert!(stale);
		assert_eq!(files.added.len(), 1);
		assert!(files.modified.is_empty());
		assert_eq!(entities.added.len(), 1);
		assert!(entities.modified.is_empty());
		validate_autoplan(&entities).expect("stale+additive must plan clean");
	}

	#[test]
	fn small_real_modification_is_not_treated_as_stale() {
		let file_diff = FileDiff {
			added: Vec::new(),
			modified: vec!["database/schema/person.surql".to_string()],
			removed: Vec::new(),
		};
		let catalog_diff = CatalogDiff {
			added: Vec::new(),
			removed: Vec::new(),
			modified: vec![CatalogChange {
				old: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "name".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "a".to_string(),
					file_hash: "fa".to_string(),
				},
				new: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "name".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "b".to_string(),
					file_hash: "fb".to_string(),
				},
			}],
		};
		let (files, entities, stale) =
			reconcile_stale_baseline_for_autoplan(file_diff, catalog_diff);
		assert!(!stale);
		assert_eq!(files.modified.len(), 1);
		assert_eq!(entities.modified.len(), 1);
		let err = validate_autoplan(&entities).expect_err("real edits still refuse");
		assert!(err.to_string().contains("refuses modified"));
	}

	#[test]
	fn build_plan_creates_add_and_remove_steps() {
		let files = vec![SchemaFile {
			path: "database/schema/customer.surql".to_string(),
			sql: "DEFINE TABLE customer SCHEMAFULL;".to_string(),
			hash: "file-a".to_string(),
		}];
		let spec = build_rollout_spec(
			"20260302153045__customer",
			"customer",
			&files,
			&FileDiff {
				added: vec!["database/schema/customer.surql".to_string()],
				modified: Vec::new(),
				removed: Vec::new(),
			},
			&CatalogDiff {
				added: vec![CatalogEntity {
					kind: "table".to_string(),
					scope: None,
					name: "customer".to_string(),
					source_path: "database/schema/customer.surql".to_string(),
					statement_hash: "stmt".to_string(),
					file_hash: "file-a".to_string(),
				}],
				removed: vec![CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: "nickname".to_string(),
					source_path: "database/schema/person.surql".to_string(),
					statement_hash: "old".to_string(),
					file_hash: "file-old".to_string(),
				}],
				modified: Vec::new(),
			},
			&SchemaSnapshot {
				version: 1,
				files: vec![SchemaSnapshotEntry {
					path: "database/schema/person.surql".to_string(),
					hash: "old".to_string(),
				}],
			},
			&SchemaSnapshot {
				version: 1,
				files: vec![SchemaSnapshotEntry {
					path: "database/schema/customer.surql".to_string(),
					hash: "new".to_string(),
				}],
			},
			&RolloutHashScope::default(),
		)
		.expect("build rollout");

		assert_eq!(spec.steps.len(), 3);
		assert!(
			spec.steps.iter().any(|step| step.phase == RolloutPhase::Start
				&& step.kind == RolloutStepKind::ApplySchema)
		);
		assert!(spec.steps.iter().any(|step| {
			step.phase == RolloutPhase::Rollback && step.kind == RolloutStepKind::RemoveEntities
		}));
		assert!(spec.steps.iter().any(|step| {
			step.phase == RolloutPhase::Complete && step.kind == RolloutStepKind::RemoveEntities
		}));
	}

	#[test]
	fn rollout_hash_scope_excludes_only_configured_schema_prefixes() {
		let scope = parse_rollout_hash_scope(
			r#"
[rollouts]
target_hash_exclude_prefixes = ["database/schema/900_extension/"]
"#,
			Path::new("database/surrealkit.toml"),
		)
		.expect("valid rollout hash scope");
		let core = SchemaFile {
			path: "database/schema/020_tables/core.surql".to_string(),
			sql: "DEFINE TABLE core SCHEMAFULL;".to_string(),
			hash: "core".to_string(),
		};
		let extension = SchemaFile {
			path: "database/schema/900_extension/tenant.surql".to_string(),
			sql: "DEFINE TABLE tenant_extension SCHEMAFULL;".to_string(),
			hash: "tenant".to_string(),
		};

		assert_eq!(
			scope.hash_files(&[core.clone(), extension.clone()]).unwrap(),
			RolloutHashScope::default().hash_files(std::slice::from_ref(&core)).unwrap()
		);
		let catalog = build_catalog_snapshot(&[core, extension]).expect("full desired catalog");
		assert_eq!(catalog.entities.len(), 2, "hash exclusion must not filter the catalog");
	}

	fn core_field_change(file_hash_old: &str, file_hash_new: &str) -> CatalogChange {
		CatalogChange {
			old: CatalogEntity {
				kind: "field".to_string(),
				scope: Some("person".to_string()),
				name: "name".to_string(),
				source_path: "database/schema/020_tables/person.surql".to_string(),
				statement_hash: "a".to_string(),
				file_hash: file_hash_old.to_string(),
			},
			new: CatalogEntity {
				kind: "field".to_string(),
				scope: Some("person".to_string()),
				name: "name".to_string(),
				source_path: "database/schema/020_tables/person.surql".to_string(),
				statement_hash: "b".to_string(),
				file_hash: file_hash_new.to_string(),
			},
		}
	}

	fn extension_table(name: &str) -> CatalogEntity {
		CatalogEntity {
			kind: "table".to_string(),
			scope: None,
			name: name.to_string(),
			source_path: "database/schema/900_extension/tenant.surql".to_string(),
			statement_hash: "ext".to_string(),
			file_hash: "ext".to_string(),
		}
	}

	fn extension_scope() -> RolloutHashScope {
		parse_rollout_hash_scope(
			r#"
[rollouts]
target_hash_exclude_prefixes = ["database/schema/900_extension/"]
"#,
			Path::new("database/surrealkit.toml"),
		)
		.expect("valid rollout hash scope")
	}

	fn threshold_core_file_drift() -> FileDiff {
		FileDiff {
			added: Vec::new(),
			modified: (0..6).map(|i| format!("database/schema/020_tables/old_{i}.surql")).collect(),
			removed: Vec::new(),
		}
	}

	fn threshold_core_catalog_drift() -> Vec<CatalogChange> {
		(0..12)
			.map(|i| CatalogChange {
				old: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: format!("f{i}"),
					source_path: format!("database/schema/020_tables/old_{i}.surql"),
					statement_hash: "a".to_string(),
					file_hash: format!("old-{i}"),
				},
				new: CatalogEntity {
					kind: "field".to_string(),
					scope: Some("person".to_string()),
					name: format!("f{i}"),
					source_path: format!("database/schema/020_tables/old_{i}.surql"),
					statement_hash: "b".to_string(),
					file_hash: format!("new-{i}"),
				},
			})
			.collect()
	}

	fn extension_field_change() -> CatalogChange {
		CatalogChange {
			old: CatalogEntity {
				kind: "field".to_string(),
				scope: Some("tenant_quote".to_string()),
				name: "body".to_string(),
				source_path: "database/schema/900_extension/tenant.surql".to_string(),
				statement_hash: "a".to_string(),
				file_hash: "fa".to_string(),
			},
			new: CatalogEntity {
				kind: "field".to_string(),
				scope: Some("tenant_quote".to_string()),
				name: "body".to_string(),
				source_path: "database/schema/900_extension/tenant.surql".to_string(),
				statement_hash: "b".to_string(),
				file_hash: "fb".to_string(),
			},
		}
	}

	fn extension_table_removed() -> CatalogEntity {
		CatalogEntity {
			kind: "table".to_string(),
			scope: None,
			name: "tenant_quote".to_string(),
			source_path: "database/schema/900_extension/tenant.surql".to_string(),
			statement_hash: "old".to_string(),
			file_hash: "old".to_string(),
		}
	}

	fn aieus12_reconcile_then_scope(
		scope: &RolloutHashScope,
		file_diff: FileDiff,
		catalog_diff: CatalogDiff,
	) -> (FileDiff, CatalogDiff, bool) {
		let (files, entities, stale) =
			reconcile_stale_baseline_for_autoplan(file_diff, catalog_diff);
		(
			scope.scope_file_diff_for_autoplan(files),
			scope.scope_catalog_diff_for_autoplan(entities),
			stale,
		)
	}

	#[test]
	fn aieus12_reconcile_then_scope_erases_declared_extension_transition() {
		let scope = extension_scope();
		let mut files = threshold_core_file_drift();
		files.modified.push("database/schema/900_extension/tenant.surql".to_string());
		files.removed.push("database/schema/900_extension/gone.surql".to_string());
		let catalog_diff = CatalogDiff {
			added: Vec::new(),
			removed: vec![extension_table_removed()],
			modified: {
				let mut modified = threshold_core_catalog_drift();
				modified.push(extension_field_change());
				modified
			},
		};
		let (_files, entities, stale) = aieus12_reconcile_then_scope(&scope, files, catalog_diff);
		assert!(stale);
		assert!(
			entities.modified.is_empty() && entities.removed.is_empty(),
			"aieus.12 order wiped the declared transition before validate_autoplan"
		);
		validate_autoplan(&entities).expect("bug: erased non-additive extension changes pass");
	}

	#[test]
	fn autoplan_additive_extension_plus_threshold_core_drift_plans() {
		let scope = extension_scope();
		let mut files = threshold_core_file_drift();
		files.added.push("database/schema/900_extension/tenant.surql".to_string());
		let catalog_diff = CatalogDiff {
			added: vec![extension_table("tenant_quote")],
			removed: Vec::new(),
			modified: threshold_core_catalog_drift(),
		};
		let (files, entities, stale) = prepare_autoplan_diffs(&scope, files, catalog_diff);
		assert!(stale, "unrelated core drift is threshold-scale");
		assert_eq!(files.added, vec!["database/schema/900_extension/tenant.surql".to_string()]);
		assert!(files.modified.is_empty(), "unrelated stale file drift is caught up");
		assert_eq!(entities.added.len(), 1);
		assert!(entities.modified.is_empty());
		assert!(entities.removed.is_empty());
		validate_autoplan(&entities).expect("additive extension plus stale core must plan");

		let old = SchemaSnapshot {
			version: 1,
			files: vec![SchemaSnapshotEntry {
				path: "database/schema/020_tables/person.surql".to_string(),
				hash: "old-core".to_string(),
			}],
		};
		let new_core_only = SchemaSnapshot {
			version: 1,
			files: vec![SchemaSnapshotEntry {
				path: "database/schema/020_tables/person.surql".to_string(),
				hash: "new-core".to_string(),
			}],
		};
		let new_with_extension = SchemaSnapshot {
			version: 1,
			files: vec![
				SchemaSnapshotEntry {
					path: "database/schema/020_tables/person.surql".to_string(),
					hash: "new-core".to_string(),
				},
				SchemaSnapshotEntry {
					path: "database/schema/900_extension/tenant.surql".to_string(),
					hash: "ext".to_string(),
				},
			],
		};
		let source = scope.hash_snapshot(&old).unwrap();
		let target = scope.hash_snapshot(&new_with_extension).unwrap();
		assert_eq!(source.len(), 64);
		assert_eq!(target.len(), 64);
		assert_ne!(source, target, "core drift is visible in managed-core fingerprints");
		assert_eq!(
			target,
			scope.hash_snapshot(&new_core_only).unwrap(),
			"extension files must not enter source/target hashes"
		);
	}

	#[test]
	fn autoplan_preserves_modified_and_removed_extension_through_stale_cleanup() {
		let scope = extension_scope();
		let mut files = threshold_core_file_drift();
		files.modified.push("database/schema/900_extension/tenant.surql".to_string());
		files.removed.push("database/schema/900_extension/gone.surql".to_string());
		let catalog_diff = CatalogDiff {
			added: Vec::new(),
			removed: vec![extension_table_removed()],
			modified: {
				let mut modified = threshold_core_catalog_drift();
				modified.push(extension_field_change());
				modified
			},
		};
		let (files, entities, stale) = prepare_autoplan_diffs(&scope, files, catalog_diff);
		assert!(stale, "unrelated core drift is threshold-scale and caught up");
		assert_eq!(files.modified, vec!["database/schema/900_extension/tenant.surql".to_string()]);
		assert_eq!(files.removed, vec!["database/schema/900_extension/gone.surql".to_string()]);
		assert_eq!(entities.modified.len(), 1, "declared modified entity must survive cleanup");
		assert_eq!(entities.removed.len(), 1, "declared removed entity must survive cleanup");
		assert!(
			entities
				.modified
				.iter()
				.all(|change| change.old.source_path.starts_with("database/schema/900_extension/")),
			"unrelated stale core entities must not remain"
		);
		let err = validate_autoplan(&entities)
			.expect_err("preserved non-additive extension changes must refuse");
		let message = err.to_string();
		assert!(message.contains("field:tenant_quote.body"), "{message}");
		assert!(message.contains("table:tenant_quote"), "{message}");
	}

	#[test]
	fn autoplan_refuses_modified_entity_in_declared_extension_transition() {
		let scope = extension_scope();
		let catalog_diff = CatalogDiff {
			added: Vec::new(),
			removed: Vec::new(),
			modified: vec![extension_field_change()],
		};
		let (_files, entities, stale) =
			prepare_autoplan_diffs(&scope, FileDiff::default(), catalog_diff);
		assert!(!stale);
		let err = validate_autoplan(&entities).expect_err("declared extension edits still refuse");
		let message = err.to_string();
		assert!(message.contains("refuses modified"));
		assert!(message.contains("field:tenant_quote.body"), "{message}");
	}

	#[test]
	fn autoplan_refuses_removed_entity_in_declared_extension_transition() {
		let scope = extension_scope();
		let catalog_diff = CatalogDiff {
			added: Vec::new(),
			modified: Vec::new(),
			removed: vec![extension_table_removed()],
		};
		let (_files, entities, stale) =
			prepare_autoplan_diffs(&scope, FileDiff::default(), catalog_diff);
		assert!(!stale);
		let err =
			validate_autoplan(&entities).expect_err("declared extension removals still refuse");
		let message = err.to_string();
		assert!(message.contains("refuses removed"));
		assert!(message.contains("table:tenant_quote"), "{message}");
	}

	#[test]
	fn autoplan_core_only_stale_baseline_still_plans_additive() {
		let file_diff = FileDiff {
			added: vec!["database/schema/new_additive.surql".to_string()],
			modified: (0..6).map(|i| format!("database/schema/old_{i}.surql")).collect(),
			removed: Vec::new(),
		};
		let catalog_diff = CatalogDiff {
			added: vec![CatalogEntity {
				kind: "table".to_string(),
				scope: None,
				name: "new_additive".to_string(),
				source_path: "database/schema/new_additive.surql".to_string(),
				statement_hash: "n".to_string(),
				file_hash: "n".to_string(),
			}],
			removed: Vec::new(),
			modified: (0..12)
				.map(|i| CatalogChange {
					old: CatalogEntity {
						kind: "field".to_string(),
						scope: Some("person".to_string()),
						name: format!("f{i}"),
						source_path: format!("database/schema/old_{i}.surql"),
						statement_hash: "a".to_string(),
						file_hash: format!("old-{i}"),
					},
					new: CatalogEntity {
						kind: "field".to_string(),
						scope: Some("person".to_string()),
						name: format!("f{i}"),
						source_path: format!("database/schema/old_{i}.surql"),
						statement_hash: "b".to_string(),
						file_hash: format!("new-{i}"),
					},
				})
				.collect(),
		};
		let (files, entities, stale) =
			prepare_autoplan_diffs(&RolloutHashScope::default(), file_diff, catalog_diff);
		assert!(stale);
		assert_eq!(files.added.len(), 1);
		assert!(files.modified.is_empty());
		assert_eq!(entities.added.len(), 1);
		assert!(entities.modified.is_empty());
		validate_autoplan(&entities).expect("core-only stale+additive must plan clean");
	}

	#[test]
	fn autoplan_without_exclude_prefixes_still_refuses_core_edits() {
		let catalog_diff = CatalogDiff {
			added: vec![extension_table("tenant_quote")],
			removed: Vec::new(),
			modified: vec![core_field_change("old-core", "new-core")],
		};
		let (_files, entities, stale) =
			prepare_autoplan_diffs(&RolloutHashScope::default(), FileDiff::default(), catalog_diff);
		assert!(!stale);
		let err =
			validate_autoplan(&entities).expect_err("unscoped autoplan still refuses core edits");
		assert!(err.to_string().contains("field:person.name"));
	}

	#[test]
	fn rollout_hash_scope_deduplicates_prefixes() {
		let scope = parse_rollout_hash_scope(
			r#"
[rollouts]
target_hash_exclude_prefixes = [
  "database/schema/900_extension/",
  "database/schema/900_extension/",
]
"#,
			Path::new("surrealkit.toml"),
		)
		.expect("valid rollout hash scope");
		assert_eq!(scope.exclude_prefixes, vec!["database/schema/900_extension/"]);
	}

	#[test]
	fn rollout_hash_scope_rejects_unsafe_prefixes() {
		for prefix in [
			"database/schema/",
			"database/schema/../seed/",
			"database/schema//900_extension/",
			"database/schema/900_extension",
			"/database/schema/900_extension/",
			"database\\schema\\900_extension\\",
		] {
			let raw = format!("[rollouts]\ntarget_hash_exclude_prefixes = ['{prefix}']\n");
			let err = parse_rollout_hash_scope(&raw, Path::new("surrealkit.toml"))
				.expect_err("unsafe prefix must fail closed");
			assert!(
				err.to_string().contains("target_hash_exclude_prefixes"),
				"unexpected error for {prefix}: {err:#}"
			);
		}
	}

	#[test]
	fn rollout_lint_rejects_non_idempotent_run_sql() {
		let spec = RolloutSpec {
			id: "a".to_string(),
			name: "a".to_string(),
			source_schema_hash: "1".to_string(),
			target_schema_hash: "2".to_string(),
			compatibility: "phased".to_string(),
			renames: Vec::new(),
			steps: vec![RolloutStep {
				id: "step".to_string(),
				phase: RolloutPhase::Start,
				kind: RolloutStepKind::RunSql,
				files: Vec::new(),
				sql: Some("UPDATE person SET name = 'a';".to_string()),
				expect: None,
				entities: Vec::new(),
				idempotent: Some(false),
			}],
		};

		let err = validate_rollout_spec(&spec).expect_err("must reject non-idempotent run_sql");
		assert!(err.to_string().contains("idempotent = true"));
	}

	async fn connect_mem_db() -> Surreal<Any> {
		use surrealdb::engine::any::connect;
		use surrealdb::opt::Config;
		use surrealdb::opt::capabilities::Capabilities;

		let config = Config::new().capabilities(Capabilities::all());
		let db = connect(("mem://", config)).await.expect("connect mem://");
		db.use_ns("surrealkit_test").use_db("rollout_test").await.expect("use_ns/use_db");
		db.query(crate::scaffold::DEFAULT_SETUP)
			.await
			.expect("setup schema")
			.check()
			.expect("setup schema check");
		db
	}

	fn sample_loaded_spec(id: &str) -> LoadedRolloutSpec {
		LoadedRolloutSpec {
			path: PathBuf::from(format!("database/rollouts/{id}.toml")),
			checksum: "sum".to_string(),
			spec: RolloutSpec {
				id: id.to_string(),
				name: "test".to_string(),
				source_schema_hash: "src".to_string(),
				target_schema_hash: "tgt".to_string(),
				compatibility: "phased".to_string(),
				renames: Vec::new(),
				steps: Vec::new(),
			},
		}
	}

	async fn load_single_row(db: &Surreal<Any>) -> Value {
		let mut resp =
			db.query("SELECT * FROM __rollout LIMIT 1;").await.expect("select __rollout");
		let rows: Vec<Value> = resp.take(0).expect("take rows");
		rows.into_iter().next().expect("one row exists")
	}

	// Regression: CREATE __rollout used to bind started_at as a plain RFC3339 string,
	// which the SCHEMAFULL `datetime` field rejected. Keep the SQL-side `<datetime>`
	// cast so string bindings coerce server-side.
	#[tokio::test]
	async fn create_rollout_record_accepts_rfc3339_started_at() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260417181055__initial_schema");

		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned)
			.await
			.expect("create_rollout_record should coerce started_at string to datetime");

		let row = load_single_row(&db).await;
		let started = row
			.get("started_at")
			.and_then(|v| v.as_str())
			.expect("started_at is serialized as a datetime string");
		time::OffsetDateTime::parse(started, &Rfc3339)
			.expect("started_at should round-trip through RFC3339");
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("planned"));
	}

	// Regression: set_rollout_status bound completed_at as Option<String>, which the
	// SCHEMAFULL `option<datetime>` field rejected for the Some(rfc3339) case. The
	// SQL-side `IF $completed_at THEN <datetime> $completed_at ELSE NONE END`
	// pattern must accept both Some(rfc3339) and None.
	#[tokio::test]
	async fn set_rollout_status_accepts_rfc3339_completed_at() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260417181055__complete_path");
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::RunningComplete)
			.await
			.expect("seed rollout record");

		let completed_at = OffsetDateTime::now_utc().format(&Rfc3339).expect("format rfc3339");
		set_rollout_status(
			&db,
			&loaded.spec.id,
			RolloutStatus::Completed,
			None,
			Some(completed_at),
		)
		.await
		.expect("set_rollout_status should coerce completed_at string to datetime");

		let row = load_single_row(&db).await;
		let completed = row
			.get("completed_at")
			.and_then(|v| v.as_str())
			.expect("completed_at is serialized as a datetime string");
		time::OffsetDateTime::parse(completed, &Rfc3339)
			.expect("completed_at should round-trip through RFC3339");
	}

	// Regression: `completed_at = None` must clear the field to NONE rather than
	// failing the `option<datetime>` coercion with an empty/null placeholder. Also
	// verifies the UPDATE's WHERE clause matched (status transitions planned →
	// running_start), which would silently no-op if the id lookup is broken.
	#[tokio::test]
	async fn set_rollout_status_accepts_none_completed_at() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260417181055__running_path");
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned)
			.await
			.expect("seed rollout record");

		set_rollout_status(&db, &loaded.spec.id, RolloutStatus::RunningStart, None, None)
			.await
			.expect("set_rollout_status with None completed_at should succeed");

		let row = load_single_row(&db).await;
		assert!(
			row.get("completed_at").is_none_or(Value::is_null),
			"completed_at should be NONE/null, got {:?}",
			row.get("completed_at")
		);
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("running_start"));
	}

	// Regression: `WHERE id = $id` against __rollout silently matched zero rows
	// because the record id is a Thing (`__rollout:…`) and the bound string isn't
	// auto-coerced. `load_rollout_record` must find the row it just created.
	#[tokio::test]
	async fn load_rollout_record_finds_created_row() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260417181055__lookup");
		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned)
			.await
			.expect("seed rollout record");

		let row = load_rollout_record(&db, &loaded.spec.id)
			.await
			.expect("load_rollout_record query")
			.expect("row must be found by rollout id");
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("planned"));
	}

	// Regression: run_status used to call resp.take::<Vec<serde_json::Value>>(0) which
	// panics over HTTP/CBOR when rows contain SurrealDB datetime values (started_at,
	// completed_at). The fix deserialises via surrealdb_types::Value first and then
	// converts, matching the pattern already used in execute_sql_value.
	//
	// This test exercises the exact SELECT used by run_status against a completed rollout
	// record that has both started_at and completed_at populated.
	#[tokio::test]
	async fn run_status_select_does_not_panic_with_datetime_fields() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260420101627__initial_schema");

		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::Planned)
			.await
			.expect("create rollout record");

		let completed_at = OffsetDateTime::now_utc().format(&Rfc3339).expect("format rfc3339");
		set_rollout_status(
			&db,
			&loaded.spec.id,
			RolloutStatus::Completed,
			None,
			Some(completed_at),
		)
		.await
		.expect("set completed status");

		// Replicate the exact query run_status issues, including the datetime fields.
		let mut resp = db
			.query(
				"SELECT id, name, status, started_at, completed_at, last_error, steps \
				 FROM __rollout WHERE record::id(id) = $id ORDER BY started_at DESC;",
			)
			.bind(("id", loaded.spec.id.clone()))
			.await
			.expect("query");

		// Fixed deserialization path (must not panic).
		let raw_rows: Vec<surrealdb_types::Value> = resp.take(0).expect("take raw rows");
		let rows: Vec<Value> =
			raw_rows.into_iter().map(|v| Value::from_value(v).unwrap_or(Value::Null)).collect();

		assert_eq!(rows.len(), 1, "expected one rollout row");
		let row = &rows[0];
		assert_eq!(row.get("status").and_then(|v| v.as_str()), Some("completed"));
		assert!(
			row.get("started_at").is_some(),
			"started_at must survive the surrealdb_types→serde_json conversion"
		);
		assert!(
			row.get("completed_at").is_some(),
			"completed_at must survive the surrealdb_types→serde_json conversion"
		);
	}

	// Regression: selecting `id` returns the full SurrealDB record id
	// (`__rollout:<id>`), so a retry of the same failed rollout was mistaken for a
	// conflicting rollout. Select the raw record key and verify both same-rollout
	// resumption and different-rollout exclusion.
	#[tokio::test]
	async fn active_rollout_id_allows_same_rollout_resume() {
		let db = connect_mem_db().await;
		let loaded = sample_loaded_spec("20260420101627__active_id_test");

		create_rollout_record(&db, &loaded, &[], &[], RolloutStatus::RunningStart)
			.await
			.expect("create rollout record");

		let active =
			load_active_rollout_id(&db).await.expect("load_active_rollout_id must not fail");
		assert_eq!(active.as_deref(), Some(loaded.spec.id.as_str()));
		ensure_no_conflicting_active_rollout(&db, &loaded.spec.id)
			.await
			.expect("the same active rollout must be resumable");
		let err = ensure_no_conflicting_active_rollout(&db, "different_rollout")
			.await
			.expect_err("a different rollout must remain blocked");
		assert!(err.to_string().contains(&loaded.spec.id));
	}
}
