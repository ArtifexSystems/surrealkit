use std::path::Path;
use std::sync::Mutex;

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use surrealkit::schema_state::EntityKey;
use surrealkit::{
	EmbeddedSchemaFile, RolloutExecutionOpts, RolloutPhase, RolloutPlanOpts, RolloutSpec,
	RolloutStep, RolloutStepKind, SyncOpts, TemplateVars, run_baseline, run_complete,
	run_complete_with_spec, run_plan, run_rollback, run_rollback_with_spec, run_setup, run_start,
	run_start_with_spec, run_status, run_sync_embedded, run_sync_embedded_with_opts, seed_from_dir,
};

async fn mem_db() -> Surreal<Any> {
	let cfg = Config::new().capabilities(Capabilities::all());
	let db = connect(("mem://", cfg)).await.expect("connect mem://");
	db.use_ns("surrealkit_test").use_db("library_api_test").await.expect("use_ns/use_db");
	db
}

// Tests that change cwd must hold this lock to avoid races.
static FS_LOCK: Mutex<()> = Mutex::new(());

struct RestoreCwd(std::path::PathBuf);

impl Drop for RestoreCwd {
	fn drop(&mut self) {
		let _ = std::env::set_current_dir(&self.0);
	}
}

fn enter_tempdir() -> (tempfile::TempDir, RestoreCwd) {
	let original = std::env::current_dir().expect("get cwd");
	let tmp = tempfile::TempDir::new().expect("create temp dir");
	std::env::set_current_dir(tmp.path()).expect("set cwd");
	(tmp, RestoreCwd(original))
}

#[tokio::test]
async fn setup_initialises_metadata_tables() {
	let _lock = FS_LOCK.lock().unwrap();
	let (_tmp, _cwd) = enter_tempdir();
	let db = mem_db().await;
	run_setup(&db).await.expect("run_setup");

	db.query("SELECT * FROM __entity LIMIT 1;")
		.await
		.expect("query __entity")
		.check()
		.expect("__entity must exist");

	db.query("SELECT * FROM __rollout LIMIT 1;")
		.await
		.expect("query __rollout")
		.check()
		.expect("__rollout must exist");
}

#[tokio::test]
async fn sync_embedded_applies_schema_and_tracks_file() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/person.surql",
		sql: "DEFINE TABLE person SCHEMALESS;",
	}];

	run_sync_embedded(&db, FILES).await.expect("run_sync_embedded");

	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get("key").and_then(|v| v.as_str()), Some("database/schema/person.surql"));
}

#[tokio::test]
async fn sync_embedded_is_idempotent() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/idempotent.surql",
		sql: "DEFINE TABLE idempotent_test SCHEMALESS;",
	}];

	run_sync_embedded(&db, FILES).await.expect("first sync");
	run_sync_embedded(&db, FILES).await.expect("second sync");

	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1, "no duplicate tracking records");
}

#[tokio::test]
async fn sync_embedded_prunes_removed_files() {
	let db = mem_db().await;

	static TWO_FILES: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/alpha.surql",
			sql: "DEFINE TABLE alpha SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/beta.surql",
			sql: "DEFINE TABLE beta SCHEMALESS;",
		},
	];
	run_sync_embedded(&db, TWO_FILES).await.expect("initial sync");

	let mut resp =
		db.query("SELECT key FROM __entity WHERE ns = 'sync' ORDER BY key;").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 2);

	static ONE_FILE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/alpha.surql",
		sql: "DEFINE TABLE alpha SCHEMALESS;",
	}];
	run_sync_embedded_with_opts(
		&db,
		ONE_FILE,
		&SyncOpts {
			watch: false,
			debounce_ms: 0,
			dry_run: false,
			fail_fast: true,
			prune: true,
			allow_shared_prune: false,
			..Default::default()
		},
	)
	.await
	.expect("pruning sync");

	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1, "beta must be pruned");
	assert_eq!(rows[0].get("key").and_then(|v| v.as_str()), Some("database/schema/alpha.surql"));
}

#[tokio::test]
async fn sync_embedded_refuses_to_prune_beyond_max_prune_limit() {
	// Safety rail: a non-zero max_prune caps how many stale managed entities
	// a single sync may drop. Triggered by real-world "ran from the wrong
	// cwd" mistakes that turn an entire project into "stale" in one go.
	let db = mem_db().await;

	static THREE_FILES: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/a.surql",
			sql: "DEFINE TABLE table_a SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/b.surql",
			sql: "DEFINE TABLE table_b SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/c.surql",
			sql: "DEFINE TABLE table_c SCHEMALESS;",
		},
	];
	run_sync_embedded(&db, THREE_FILES).await.expect("initial sync");

	// Empty schema set → all three entities become stale. Cap at 2.
	static EMPTY: &[EmbeddedSchemaFile] = &[];
	let err = run_sync_embedded_with_opts(
		&db,
		EMPTY,
		&SyncOpts {
			fail_fast: true,
			prune: true,
			max_prune: 2,
			..Default::default()
		},
	)
	.await
	.expect_err("3 > max_prune=2 must abort");
	let msg = format!("{err:#}");
	assert!(msg.contains("refusing to prune"), "unexpected error: {msg}");
	assert!(msg.contains("--max-prune"), "error should reference flag: {msg}");

	// Cap at 5 (above stale count) → succeeds.
	run_sync_embedded_with_opts(
		&db,
		EMPTY,
		&SyncOpts {
			fail_fast: true,
			prune: true,
			max_prune: 5,
			..Default::default()
		},
	)
	.await
	.expect("3 <= max_prune=5 must succeed");

	let mut resp = db.query("SELECT key FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(rows.is_empty(), "all three entities must be pruned when limit allows");
}

#[tokio::test]
async fn sync_embedded_self_heals_catalog_drift() {
	// Catalog drift: an entity tracked in __entity is already missing from the
	// live DB (e.g., a `run_sql REMOVE …` rollout step dropped it but didn't
	// update the catalog). On the next sync, the pruner emits REMOVE for that
	// entity. Without `IF EXISTS` the REMOVE fails with "X does not exist" and
	// halts the whole prune batch (sync.rs::prune_managed_entities joins all
	// statements). With `IF EXISTS` the prune succeeds and the catalog row is
	// reaped, so drift self-heals on the next sync.

	let db = mem_db().await;

	static WITH_FIELD: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/drift.surql",
		sql: "DEFINE TABLE drift_target SCHEMAFULL;\n\
		      DEFINE FIELD nickname ON drift_target TYPE string;",
	}];
	run_sync_embedded(&db, WITH_FIELD).await.expect("initial sync");

	// Simulate the rollout step that dropped the field by raw `run_sql`:
	// the live DB no longer has it, but __entity still tracks it.
	db.query("REMOVE FIELD nickname ON drift_target;")
		.await
		.expect("manual remove query")
		.check()
		.expect("manual remove succeeded");

	// Drop the file entirely so the pruner is asked to remove BOTH the table
	// (still present) AND the field (already gone). Pre-fix, the field prune
	// errors and the table is never reached.
	static EMPTY: &[EmbeddedSchemaFile] = &[];
	run_sync_embedded_with_opts(
		&db,
		EMPTY,
		&SyncOpts {
			watch: false,
			debounce_ms: 0,
			dry_run: false,
			fail_fast: true,
			prune: true,
			allow_shared_prune: false,
			..Default::default()
		},
	)
	.await
	.expect("pruning sync should self-heal drift, not error on missing field");

	let mut resp = db.query("SELECT * FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(rows.is_empty(), "catalog must be fully reaped: {rows:?}");
}

#[tokio::test]
async fn sync_embedded_dry_run_makes_no_changes() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/dry.surql",
		sql: "DEFINE TABLE dry_run_test SCHEMALESS;",
	}];

	run_sync_embedded_with_opts(
		&db,
		FILES,
		&SyncOpts {
			watch: false,
			debounce_ms: 0,
			dry_run: true,
			fail_fast: true,
			prune: true,
			allow_shared_prune: false,
			..Default::default()
		},
	)
	.await
	.expect("dry run");

	let mut resp = db.query("SELECT * FROM __entity WHERE ns = 'sync';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(rows.is_empty(), "dry run must not write tracking records");
}

#[tokio::test]
async fn rollout_status_is_empty_when_no_rollouts_exist() {
	let db = mem_db().await;
	run_status(&db, None).await.expect("run_status on empty DB");
}

// Regression: run_status crashed (SIGABRT via panic=abort) when called after a rollout
// had been completed, because resp.take::<Vec<serde_json::Value>> panics over HTTP/CBOR
// when rows include SurrealDB datetime fields (started_at, completed_at).
// Both the targeted selector and the list-all form must work without panicking.
#[tokio::test]
async fn rollout_status_does_not_crash_after_completed_rollout() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;

	write_schema_file(tmp.path(), "person.surql", "DEFINE TABLE person SCHEMALESS;");
	run_baseline(&db).await.expect("baseline");

	write_schema_file(tmp.path(), "account.surql", "DEFINE TABLE account SCHEMALESS;");
	run_plan(RolloutPlanOpts {
		name: Some("add_account_status_test".to_string()),
		dry_run: false,
	})
	.await
	.expect("plan");

	let rollout_id = find_latest_rollout_id(tmp.path()).expect("rollout TOML not found");

	run_start(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("start");

	run_complete(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("complete");

	// Targeted lookup — this is what the customer's CI script calls.
	run_status(&db, Some(rollout_id.clone()))
		.await
		.expect("run_status with selector must not crash on completed rollout");

	// List-all form.
	run_status(&db, None)
		.await
		.expect("run_status without selector must not crash on completed rollout");
}

fn write_schema_file(dir: &Path, name: &str, sql: &str) {
	let schema_dir = dir.join("database/schema");
	std::fs::create_dir_all(&schema_dir).expect("create schema dir");
	std::fs::write(schema_dir.join(name), sql).expect("write schema file");
}

#[tokio::test]
async fn rollout_full_lifecycle_via_library() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;

	write_schema_file(tmp.path(), "person.surql", "DEFINE TABLE person SCHEMALESS;");
	run_baseline(&db).await.expect("baseline");

	// ns = 'schema' is the internal key used by the managed-entity tracker.
	let mut resp = db.query("SELECT * FROM __entity WHERE ns = 'schema';").await.expect("query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert!(!rows.is_empty(), "baseline must track managed entities");

	write_schema_file(tmp.path(), "account.surql", "DEFINE TABLE account SCHEMALESS;");
	run_plan(RolloutPlanOpts {
		name: Some("add_account".to_string()),
		dry_run: false,
	})
	.await
	.expect("plan");

	let rollout_id = find_latest_rollout_id(tmp.path()).expect("rollout TOML not found");

	run_start(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("start");

	assert_eq!(query_rollout_status(&db, &rollout_id).await.as_deref(), Some("ready_to_complete"));

	run_complete(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("complete");

	assert_eq!(query_rollout_status(&db, &rollout_id).await.as_deref(), Some("completed"));
}

#[tokio::test]
async fn rollout_rollback_after_start_via_library() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;

	write_schema_file(tmp.path(), "order.surql", "DEFINE TABLE order SCHEMALESS;");
	run_baseline(&db).await.expect("baseline");

	write_schema_file(tmp.path(), "invoice.surql", "DEFINE TABLE invoice SCHEMALESS;");
	run_plan(RolloutPlanOpts {
		name: Some("add_invoice".to_string()),
		dry_run: false,
	})
	.await
	.expect("plan");

	let rollout_id = find_latest_rollout_id(tmp.path()).expect("rollout TOML not found");

	run_start(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("start");

	run_rollback(
		&db,
		RolloutExecutionOpts {
			selector: Some(rollout_id.clone()),
		},
		&TemplateVars::default(),
	)
	.await
	.expect("rollback");

	assert_eq!(query_rollout_status(&db, &rollout_id).await.as_deref(), Some("rolled_back"));
}

#[tokio::test]
async fn seed_from_dir_is_accessible_via_library() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();

	let db = mem_db().await;

	let seed_dir = tmp.path().join("custom_seed");
	std::fs::create_dir_all(&seed_dir).expect("create seed dir");
	std::fs::write(seed_dir.join("01_data.surql"), "CREATE person:alice SET name = 'Alice';")
		.expect("write seed file");

	seed_from_dir(&db, &seed_dir, &TemplateVars::default()).await.expect("seed_from_dir");

	let mut resp = db.query("SELECT name FROM person WHERE id = person:alice;").await.expect("q");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Alice"));
}

fn find_latest_rollout_id(base: &Path) -> Option<String> {
	let rollouts_dir = base.join("database/rollouts");
	let mut entries: Vec<_> = std::fs::read_dir(&rollouts_dir)
		.ok()?
		.filter_map(|e| e.ok())
		.filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("toml"))
		.collect();
	entries.sort_by_key(|e| e.file_name());
	let stem = entries.last()?.path().file_stem()?.to_str()?.to_string();
	Some(stem)
}

async fn query_rollout_status(db: &Surreal<Any>, rollout_id: &str) -> Option<String> {
	let mut resp = db
		.query("SELECT status FROM __rollout WHERE record::id(id) = $id LIMIT 1;")
		.bind(("id", rollout_id.to_string()))
		.await
		.ok()?;
	let rows: Vec<serde_json::Value> = resp.take(0).ok()?;
	rows.into_iter().next()?.get("status")?.as_str().map(str::to_string)
}

// Builds a minimal RolloutSpec that adds one table via inline SQL (no filesystem).
fn add_table_spec(id: &str, table: &str) -> RolloutSpec {
	RolloutSpec {
		id: id.to_string(),
		name: id.to_string(),
		source_schema_hash: String::new(),
		target_schema_hash: String::new(),
		compatibility: "phased".to_string(),
		renames: vec![],
		steps: vec![
			RolloutStep {
				id: "apply".to_string(),
				phase: RolloutPhase::Start,
				kind: RolloutStepKind::ApplySchema,
				files: vec![],
				sql: Some(format!("DEFINE TABLE {table} SCHEMALESS;")),
				expect: None,
				entities: vec![],
				idempotent: None,
			},
			RolloutStep {
				id: "rollback".to_string(),
				phase: RolloutPhase::Rollback,
				kind: RolloutStepKind::RemoveEntities,
				files: vec![],
				sql: None,
				expect: None,
				entities: vec![EntityKey {
					kind: "table".to_string(),
					scope: None,
					name: table.to_string(),
				}],
				idempotent: None,
			},
		],
	}
}

#[tokio::test]
async fn rollout_with_spec_full_lifecycle() {
	let db = mem_db().await;

	static SOURCE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/order.surql",
		sql: "DEFINE TABLE order SCHEMALESS;",
	}];
	static TARGET: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/order.surql",
			sql: "DEFINE TABLE order SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/invoice.surql",
			sql: "DEFINE TABLE invoice SCHEMALESS;",
		},
	];

	// Establish baseline using the source schema.
	run_sync_embedded(&db, SOURCE).await.expect("baseline sync");

	let spec = add_table_spec("add_invoice", "invoice");

	run_start_with_spec(&db, &spec, TARGET, &TemplateVars::default())
		.await
		.expect("start_with_spec");
	assert_eq!(query_rollout_status(&db, &spec.id).await.as_deref(), Some("ready_to_complete"));

	run_complete_with_spec(&db, &spec, &TemplateVars::default()).await.expect("complete_with_spec");
	assert_eq!(query_rollout_status(&db, &spec.id).await.as_deref(), Some("completed"));
}

#[tokio::test]
async fn rollout_with_spec_rollback() {
	let db = mem_db().await;

	static SOURCE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/product.surql",
		sql: "DEFINE TABLE product SCHEMALESS;",
	}];
	static TARGET: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/product.surql",
			sql: "DEFINE TABLE product SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/variant.surql",
			sql: "DEFINE TABLE variant SCHEMALESS;",
		},
	];

	run_sync_embedded(&db, SOURCE).await.expect("baseline sync");

	let spec = add_table_spec("add_variant", "variant");

	run_start_with_spec(&db, &spec, TARGET, &TemplateVars::default())
		.await
		.expect("start_with_spec");
	run_rollback_with_spec(&db, &spec, &TemplateVars::default()).await.expect("rollback_with_spec");
	assert_eq!(query_rollout_status(&db, &spec.id).await.as_deref(), Some("rolled_back"));
}

#[tokio::test]
async fn rollout_with_spec_blocks_concurrent_rollout() {
	let db = mem_db().await;

	static SOURCE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/user.surql",
		sql: "DEFINE TABLE user SCHEMALESS;",
	}];
	static TARGET_A: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/user.surql",
			sql: "DEFINE TABLE user SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/session.surql",
			sql: "DEFINE TABLE session SCHEMALESS;",
		},
	];
	static TARGET_B: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/user.surql",
			sql: "DEFINE TABLE user SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/token.surql",
			sql: "DEFINE TABLE token SCHEMALESS;",
		},
	];

	run_sync_embedded(&db, SOURCE).await.expect("baseline sync");

	let spec_a = add_table_spec("add_session_x", "session");
	run_start_with_spec(&db, &spec_a, TARGET_A, &TemplateVars::default())
		.await
		.expect("first rollout starts");

	// A second, different rollout must be rejected while the first is active.
	let spec_b = add_table_spec("add_token_x", "token");
	let err = run_start_with_spec(&db, &spec_b, TARGET_B, &TemplateVars::default())
		.await
		.expect_err("concurrent rollout must be rejected");
	assert!(err.to_string().contains("active"), "error should mention active rollout: {err}");
}

// Template variable tests

#[tokio::test]
async fn sync_embedded_with_vars_substitutes_table_name() {
	let db = mem_db().await;

	let mut vars = std::collections::HashMap::new();
	vars.insert("PREFIX".to_string(), "acme".to_string());
	let template_vars = TemplateVars {
		vars,
	};

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/prefixed.surql",
		sql: "DEFINE TABLE ${prefix}_users SCHEMALESS;",
	}];

	run_sync_embedded_with_opts(
		&db,
		FILES,
		&SyncOpts {
			fail_fast: true,
			prune: true,
			vars: template_vars,
			..Default::default()
		},
	)
	.await
	.expect("sync with vars");

	let mut resp = db.query("INFO FOR DB;").await.expect("INFO FOR DB");
	let info: Option<serde_json::Value> = resp.take(0).expect("take");
	let tables = info
		.as_ref()
		.and_then(|v| v.get("tables"))
		.and_then(|v| v.as_object())
		.map(|m| m.keys().cloned().collect::<Vec<_>>())
		.unwrap_or_default();
	assert!(
		tables.iter().any(|t| t == "acme_users"),
		"expected table 'acme_users' but got: {tables:?}"
	);
}

#[tokio::test]
async fn sync_embedded_with_undefined_var_returns_error() {
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/bad.surql",
		sql: "DEFINE TABLE ${undefined_var} SCHEMALESS;",
	}];

	let err = run_sync_embedded_with_opts(
		&db,
		FILES,
		&SyncOpts {
			fail_fast: true,
			prune: true,
			..Default::default()
		},
	)
	.await
	.expect_err("undefined var must error");

	// Variable name lives in the cause chain (wrapped by file-path context); {:#} prints full
	// chain.
	let chain = format!("{err:#}");
	assert!(
		chain.contains("UNDEFINED_VAR"),
		"error chain should name the missing variable: {chain}"
	);
}

#[tokio::test]
async fn seed_with_vars_substitutes_in_seed_file() {
	let _lock = FS_LOCK.lock().unwrap();
	let (tmp, _cwd) = enter_tempdir();

	let db = mem_db().await;

	let seed_dir = tmp.path().join("custom_seed");
	std::fs::create_dir_all(&seed_dir).expect("create seed dir");
	std::fs::write(seed_dir.join("01_data.surql"), "CREATE person:1 SET role = '${role}';")
		.expect("write seed file");

	let mut vars = std::collections::HashMap::new();
	vars.insert("ROLE".to_string(), "admin".to_string());
	let template_vars = TemplateVars {
		vars,
	};

	seed_from_dir(&db, &seed_dir, &template_vars).await.expect("seed_from_dir with vars");

	let mut resp = db.query("SELECT role FROM person WHERE id = person:1;").await.expect("q");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get("role").and_then(|v| v.as_str()), Some("admin"));
}

#[tokio::test]
async fn seed_with_undefined_var_returns_error() {
	let _lock = FS_LOCK.lock().unwrap();
	let (tmp, _cwd) = enter_tempdir();

	let db = mem_db().await;

	let seed_dir = tmp.path().join("seed");
	std::fs::create_dir_all(&seed_dir).expect("create seed dir");
	std::fs::write(seed_dir.join("01_bad.surql"), "CREATE x:1 SET y = '${NO_SUCH_VAR}';")
		.expect("write seed file");

	let err = seed_from_dir(&db, &seed_dir, &TemplateVars::default())
		.await
		.expect_err("undefined var must error");
	assert!(
		format!("{err:#}").contains("NO_SUCH_VAR"),
		"error should name the missing variable: {err:#}"
	);
}

#[tokio::test]
async fn rollout_apply_schema_step_with_files_substitutes_vars() {
	// ApplySchema with files reads from disk; rollout_run_sql_step_with_vars covers inline sql.
	let _lock = FS_LOCK.lock().unwrap();
	let (tmp, _cwd) = enter_tempdir();

	let db = mem_db().await;

	let schema_path = tmp.path().join("schema_for_apply.surql");
	std::fs::write(&schema_path, "DEFINE TABLE ${tbl_name} SCHEMALESS;")
		.expect("write schema file");

	let spec = RolloutSpec {
		id: "apply_schema_with_vars".to_string(),
		name: "apply_schema_with_vars".to_string(),
		source_schema_hash: String::new(),
		target_schema_hash: String::new(),
		compatibility: "phased".to_string(),
		renames: vec![],
		steps: vec![RolloutStep {
			id: "apply".to_string(),
			phase: RolloutPhase::Start,
			kind: RolloutStepKind::ApplySchema,
			files: vec![schema_path.to_string_lossy().into_owned()],
			sql: None,
			expect: None,
			entities: vec![],
			idempotent: None,
		}],
	};

	let mut vars = std::collections::HashMap::new();
	vars.insert("TBL_NAME".to_string(), "applied_table".to_string());
	let template_vars = TemplateVars {
		vars,
	};

	run_start_with_spec(&db, &spec, &[], &template_vars).await.expect("apply_schema with vars");

	let mut resp = db.query("INFO FOR DB;").await.expect("INFO FOR DB");
	let info: Option<serde_json::Value> = resp.take(0).expect("take");
	let tables = info
		.as_ref()
		.and_then(|v| v.get("tables"))
		.and_then(|v| v.as_object())
		.map(|m| m.keys().cloned().collect::<Vec<_>>())
		.unwrap_or_default();
	assert!(
		tables.iter().any(|t| t == "applied_table"),
		"expected substituted table name in DB: {tables:?}"
	);
}

#[tokio::test]
async fn sync_error_context_includes_offending_file_path() {
	// Error chain must include the file path so operators can locate the bad file.
	let db = mem_db().await;

	static FILES: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/needs_var.surql",
		sql: "DEFINE TABLE ${ABSENT_VAR}_table SCHEMALESS;",
	}];

	let err = run_sync_embedded_with_opts(
		&db,
		FILES,
		&SyncOpts {
			fail_fast: true,
			prune: true,
			..Default::default()
		},
	)
	.await
	.expect_err("undefined var must error");

	let chain = format!("{err:#}");
	assert!(chain.contains("ABSENT_VAR"), "error chain must name the variable: {chain}");
	assert!(
		chain.contains("database/schema/needs_var.surql"),
		"error chain must include the file path: {chain}"
	);
}

#[tokio::test]
async fn rollout_run_sql_step_with_vars() {
	let db = mem_db().await;

	// A run_sql step that references a template variable.
	// SurrealDB creates schemaless tables on demand, so no pre-setup needed.
	let spec = RolloutSpec {
		id: "rollout_with_var".to_string(),
		name: "rollout_with_var".to_string(),
		source_schema_hash: String::new(),
		target_schema_hash: String::new(),
		compatibility: "phased".to_string(),
		renames: vec![],
		steps: vec![RolloutStep {
			id: "insert_record".to_string(),
			phase: RolloutPhase::Start,
			kind: RolloutStepKind::RunSql,
			files: vec![],
			sql: Some("CREATE vartest:1 SET marker = '${marker_value}';".to_string()),
			expect: None,
			entities: vec![],
			idempotent: Some(true),
		}],
	};

	let mut vars = std::collections::HashMap::new();
	vars.insert("MARKER_VALUE".to_string(), "hello_from_var".to_string());
	let template_vars = TemplateVars {
		vars,
	};

	run_start_with_spec(&db, &spec, &[], &template_vars).await.expect("start with var");

	let mut resp = db.query("SELECT marker FROM vartest WHERE id = vartest:1;").await.expect("q");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1);
	assert_eq!(
		rows[0].get("marker").and_then(|v| v.as_str()),
		Some("hello_from_var"),
		"template variable should have been substituted in run_sql step"
	);
}

#[tokio::test]
async fn concurrent_managed_entity_upserts_do_not_trip_unique_index() {
	use surrealkit::schema_state::CatalogEntity;
	use surrealkit::{acquire_lock, release_lock, upsert_managed_entities};

	let db = mem_db().await;
	run_setup(&db).await.expect("setup");

	let entity = CatalogEntity {
		kind: "table".into(),
		scope: None,
		name: "race_product".into(),
		source_path: "database/schema/race_product.surql".into(),
		statement_hash: "hash-a".into(),
		file_hash: "file-a".into(),
	};
	let entities = vec![entity];

	let db_a = db.clone();
	let db_b = db.clone();
	let entities_a = entities.clone();
	let entities_b = entities.clone();

	let (left, right) = tokio::join!(
		async move {
			for i in 0..40 {
				let mut batch = entities_a.clone();
				batch[0].statement_hash = format!("hash-a-{i}");
				upsert_managed_entities(&db_a, &batch, None, "active").await?;
			}
			anyhow::Ok(())
		},
		async move {
			for i in 0..40 {
				let mut batch = entities_b.clone();
				batch[0].statement_hash = format!("hash-b-{i}");
				upsert_managed_entities(&db_b, &batch, None, "active").await?;
			}
			anyhow::Ok(())
		}
	);

	left.expect("left upsert stream");
	right.expect("right upsert stream");

	let mut resp = db
		.query(
			"SELECT key, count() AS n FROM __entity WHERE ns = 'schema' \
			 GROUP BY key;",
		)
		.await
		.expect("count query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(rows.len(), 1, "exactly one schema entity key: {rows:?}");
	assert_eq!(rows[0].get("n").and_then(|v| v.as_i64()), Some(1));

	acquire_lock(&db, "global").await.expect("first lock");
	let steal = db
		.query(
			"CREATE ONLY type::record('__entity', 'lock_global') CONTENT { \
			 	ns: 'lock', \
			 	key: 'global', \
			 	val: { owner: 'thief' }, \
			 	updated_at: time::now() \
			 };",
		)
		.await;
	let steal_err = match steal {
		Ok(resp) => resp.check().expect_err("CREATE must fail while lock held").to_string(),
		Err(err) => err.to_string(),
	};
	assert!(
		steal_err.contains("already contains")
			|| steal_err.contains("already exists")
			|| steal_err.contains("Resource busy")
			|| steal_err.contains("Transaction conflict"),
		"expected unique-index / busy rejection, got: {steal_err}"
	);
	release_lock(&db, "global").await.expect("release");

	// Stale lock (older than LOCK_STALE) is reclaimed.
	db.query(
		"CREATE type::record('__entity', 'lock_stale') CONTENT { \
		 	ns: 'lock', \
		 	key: 'stale', \
		 	val: { owner: 'crashed' }, \
		 	updated_at: time::now() - 10m \
		 };",
	)
	.await
	.expect("insert stale")
	.check()
	.expect("check stale");
	acquire_lock(&db, "stale").await.expect("reclaim stale lock");
	release_lock(&db, "stale").await.expect("release stale");
}

#[tokio::test]
async fn live_lock_renewal_prevents_stale_steal() {
	use surrealkit::{acquire_lock, release_lock};

	// Short TTL so the test finishes quickly; renewer keeps the lease live.
	unsafe { std::env::set_var("SURREALKIT_LOCK_STALE_SECS", "1") };

	let db = mem_db().await;
	run_setup(&db).await.expect("setup");

	acquire_lock(&db, "renew").await.expect("acquire");
	tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

	let second = tokio::time::timeout(
		std::time::Duration::from_secs(2),
		acquire_lock(&db, "renew"),
	)
	.await;
	assert!(
		matches!(second, Ok(Err(_)) | Err(_)),
		"second acquire must not steal a live renewed lease: {second:?}"
	);

	release_lock(&db, "renew").await.expect("release");
	acquire_lock(&db, "renew").await.expect("acquire after release");
	release_lock(&db, "renew").await.expect("final release");

	unsafe { std::env::remove_var("SURREALKIT_LOCK_STALE_SECS") };
}

#[tokio::test]
async fn legacy_random_id_stale_lock_is_reclaimed() {
	use surrealkit::{acquire_lock, release_lock};

	unsafe { std::env::set_var("SURREALKIT_LOCK_STALE_SECS", "1") };

	let db = mem_db().await;
	run_setup(&db).await.expect("setup");

	// Pre-aieus.10 style: random record id, same ns/key — UNIQUE by_ns_key still
	// blocks CREATE ONLY on the deterministic id until the stale row is cleared.
	db.query(
		"CREATE __entity CONTENT { \
		 	ns: 'lock', \
		 	key: 'legacy', \
		 	val: { owner: 'old-format' }, \
		 	updated_at: time::now() - 10m \
		 };",
	)
	.await
	.expect("insert legacy")
	.check()
	.expect("check legacy");

	acquire_lock(&db, "legacy")
		.await
		.expect("reclaim legacy random-id stale lock");
	release_lock(&db, "legacy").await.expect("release");

	unsafe { std::env::remove_var("SURREALKIT_LOCK_STALE_SECS") };
}

#[tokio::test]
async fn former_owner_cannot_release_successor_lock() {
	use surrealkit::{acquire_lock, release_lock};

	let db = mem_db().await;
	run_setup(&db).await.expect("setup");

	acquire_lock(&db, "hand-off").await.expect("first acquire");
	// Simulate a successor lease after reclaim: same deterministic id, new owner.
	db.query(
		"UPDATE type::record('__entity', 'lock_hand_off') SET \
		 	val = { owner: 'successor' }, \
		 	updated_at = time::now();",
	)
	.await
	.expect("install successor")
	.check()
	.expect("check successor");

	release_lock(&db, "hand-off")
		.await
		.expect("former holder release is owner-bound");

	let mut resp = db
		.query("SELECT val.owner AS owner FROM type::record('__entity', 'lock_hand_off');")
		.await
		.expect("select");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	assert_eq!(
		rows.first().and_then(|r| r.get("owner")).and_then(|v| v.as_str()),
		Some("successor"),
		"successor lease must survive former owner's release: {rows:?}"
	);

	// Clean up without going through release_lock (we no longer hold the map entry).
	db.query("DELETE type::record('__entity', 'lock_hand_off');")
		.await
		.expect("cleanup")
		.check()
		.expect("cleanup check");
}

#[tokio::test]
async fn concurrent_sync_embedded_serializes_without_unique_index_death() {
	let db = mem_db().await;
	run_setup(&db).await.expect("setup once before concurrent syncs");

	static BASELINE: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
		path: "database/schema/user.surql",
		sql: "DEFINE TABLE user SCHEMALESS;",
	}];
	static TARGET: &[EmbeddedSchemaFile] = &[
		EmbeddedSchemaFile {
			path: "database/schema/user.surql",
			sql: "DEFINE TABLE user SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/session.surql",
			sql: "DEFINE TABLE session SCHEMALESS;",
		},
		EmbeddedSchemaFile {
			path: "database/schema/token.surql",
			sql: "DEFINE TABLE token SCHEMALESS;",
		},
	];

	run_sync_embedded(&db, BASELINE).await.expect("baseline");

	async fn sync_with_bounded_retry(
		db: Surreal<Any>,
		files: &'static [EmbeddedSchemaFile],
	) -> anyhow::Result<()> {
		// Mirror AIEUS apply wrappers: lock serializes the common path; residual
		// mem-engine transaction conflicts still retry to completion.
		const DELAYS_MS: [u64; 5] = [20, 50, 100, 200, 400];
		for (i, delay) in DELAYS_MS.iter().enumerate() {
			match run_sync_embedded(&db, files).await {
				Ok(()) => return Ok(()),
				Err(err) => {
					let msg = err.to_string();
					let retryable = msg.contains("Transaction conflict")
						|| msg.contains("Resource busy")
						|| msg.contains("by_ns_key already contains")
						|| msg.contains("could not acquire SurrealKit lock");
					if !retryable || i + 1 == DELAYS_MS.len() {
						return Err(err);
					}
					tokio::time::sleep(std::time::Duration::from_millis(*delay)).await;
				}
			}
		}
		unreachable!()
	}

	let db_a = db.clone();
	let db_b = db.clone();
	let (left, right) = tokio::join!(
		sync_with_bounded_retry(db_a, TARGET),
		sync_with_bounded_retry(db_b, TARGET)
	);
	left.expect("sync A");
	right.expect("sync B");

	let mut resp = db
		.query("SELECT key FROM __entity WHERE ns = 'schema';")
		.await
		.expect("key query");
	let rows: Vec<serde_json::Value> = resp.take(0).expect("take");
	let mut seen = std::collections::BTreeSet::new();
	for row in &rows {
		let key = row.get("key").and_then(|v| v.as_str()).unwrap_or("").to_string();
		assert!(seen.insert(key.clone()), "duplicate schema key: {key}");
	}

	let key_set = seen;
	assert!(
		key_set.iter().any(|k| k.contains("user")),
		"user missing from {key_set:?}"
	);
	assert!(
		key_set.iter().any(|k| k.contains("session")),
		"session missing from {key_set:?}"
	);
	assert!(
		key_set.iter().any(|k| k.contains("token")),
		"token missing from {key_set:?}"
	);
	assert_eq!(key_set.len(), 3, "no spurious prune; keys={key_set:?}");
}

fn write_rel(dir: &Path, rel: &str, body: &str) {
	let path = dir.join(rel);
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent).expect("create parent");
	}
	std::fs::write(&path, body).expect("write file");
}

fn threshold_core_and_extension_fixture(dir: &Path) {
	for i in 0..6 {
		write_rel(
			dir,
			&format!("database/schema/020_tables/old_{i}.surql"),
			&format!(
				"DEFINE TABLE person SCHEMAFULL;\n\
DEFINE FIELD f{} ON person TYPE string;\n\
DEFINE FIELD f{} ON person TYPE string;\n",
				2 * i,
				2 * i + 1
			),
		);
	}
	write_rel(
		dir,
		"database/schema/900_extension/tenant.surql",
		"DEFINE TABLE tenant_quote SCHEMAFULL;\n\
DEFINE FIELD body ON tenant_quote TYPE string;\n",
	);
	write_rel(
		dir,
		"database/surrealkit.toml",
		"[rollouts]\ntarget_hash_exclude_prefixes = [\"database/schema/900_extension/\"]\n",
	);
}

fn mutate_threshold_core(dir: &Path) {
	for i in 0..6 {
		write_rel(
			dir,
			&format!("database/schema/020_tables/old_{i}.surql"),
			&format!(
				"DEFINE TABLE person SCHEMAFULL;\n\
DEFINE FIELD f{} ON person TYPE int;\n\
DEFINE FIELD f{} ON person TYPE int;\n",
				2 * i,
				2 * i + 1
			),
		);
	}
}

#[tokio::test]
async fn run_plan_refuses_modified_extension_with_threshold_core_drift() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();
	let db = mem_db().await;
	threshold_core_and_extension_fixture(tmp.path());
	run_baseline(&db).await.expect("baseline");
	mutate_threshold_core(tmp.path());
	write_rel(
		tmp.path(),
		"database/schema/900_extension/tenant.surql",
		"DEFINE TABLE tenant_quote SCHEMAFULL;\n\
DEFINE FIELD body ON tenant_quote TYPE int;\n",
	);
	let err = run_plan(RolloutPlanOpts {
		name: Some("threshold_modified_extension".to_string()),
		dry_run: true,
	})
	.await
	.expect_err("run_plan must refuse declared extension modification");
	let message = err.to_string();
	assert!(message.contains("refuses modified"), "{message}");
	assert!(message.contains("field:tenant_quote.body"), "{message}");
	assert!(!message.contains("field:person.f0"), "{message}");
}

#[tokio::test]
async fn run_plan_refuses_removed_extension_with_threshold_core_drift() {
	let _guard = FS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
	let (tmp, _restore) = enter_tempdir();
	let db = mem_db().await;
	threshold_core_and_extension_fixture(tmp.path());
	run_baseline(&db).await.expect("baseline");
	mutate_threshold_core(tmp.path());
	std::fs::remove_file(tmp.path().join("database/schema/900_extension/tenant.surql"))
		.expect("remove declared extension file");
	let err = run_plan(RolloutPlanOpts {
		name: Some("threshold_removed_extension".to_string()),
		dry_run: true,
	})
	.await
	.expect_err("run_plan must refuse declared extension removal");
	let message = err.to_string();
	assert!(message.contains("refuses removed"), "{message}");
	assert!(message.contains("table:tenant_quote"), "{message}");
	assert!(!message.contains("field:person.f0"), "{message}");
}
