//! Empty tables need no blocking index scan. Keep the file as one request, then
//! wait for its index builds before sync records the source file's hash.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::core::exec_surql;
use crate::schema_state::{EntityKind, parse_define_entity};
use crate::surql_scan::scan;

pub(crate) fn only_defines(sql: &str) -> Result<bool> {
	Ok(scan(sql)?.iter().all(|stmt| stmt.is_word(sql, 0, "DEFINE")))
}

fn prepare(sql: &str) -> Result<(String, Vec<(String, String)>)> {
	let statements = scan(sql)?;
	// Arbitrary SQL may write through an index later in this same request.
	// Retain its synchronous barriers and transaction behavior.
	if statements.iter().any(|stmt| !stmt.is_word(sql, 0, "DEFINE")) {
		return Ok((sql.to_owned(), Vec::new()));
	}
	let mut rewritten = String::with_capacity(sql.len());
	let mut indexes = Vec::new();
	let mut at = 0;
	for stmt in statements {
		let Some(entity) = parse_define_entity(&stmt, sql) else {
			continue;
		};
		if entity.kind != EntityKind::Index
			|| stmt
				.tokens
				.iter()
				.enumerate()
				.any(|(i, tok)| tok.depth == 0 && stmt.is_word(sql, i, "CONCURRENTLY"))
		{
			continue;
		}
		let Some(table) = entity.scope else {
			continue;
		};
		let definition = stmt.text(sql);
		rewritten.push_str(&sql[at..stmt.span.start]);
		// Evaluate here, rather than before the file: an earlier statement may
		// create the table or populate it. Identifiers retain their SQL quoting.
		// DEFINE INDEX can implicitly create its table; SELECT cannot. Preserve
		// that behavior without overwriting any existing table's definition.
		rewritten.push_str(&format!(
			"DEFINE TABLE IF NOT EXISTS {table};\n\
			IF array::len((SELECT VALUE id FROM {table} LIMIT 1)) = 0 {{\n\
			 {definition} CONCURRENTLY;\n}} ELSE {{\n{definition};\n}}"
		));
		at = stmt.span.end;
		indexes.push((table, entity.name));
	}
	rewritten.push_str(&sql[at..]);
	Ok((rewritten, indexes))
}

pub(crate) async fn apply(db: &Surreal<Any>, sql: &str) -> Result<Vec<(String, String)>> {
	let (sql, indexes) = prepare(sql)?;
	exec_surql(db, &sql).await?;
	Ok(indexes)
}

pub(crate) async fn wait(
	db: &Surreal<Any>,
	indexes: &[(String, String)],
	deadline: Instant,
) -> Result<()> {
	// Only newly concurrent empty-table builds need time here. A populated
	// table takes the original synchronous path and already reports ready.
	let mut pending: Vec<_> = indexes.iter().collect();
	while !pending.is_empty() {
		// One request per poll, including files with several indexes.
		let sql: String = pending
			.iter()
			.map(|(table, index)| format!("INFO FOR INDEX {index} ON {table};\n"))
			.collect();
		let mut response = db.query(sql).await?.check()?;
		let mut remaining = Vec::new();
		for (slot, pair @ (table, index)) in pending.into_iter().enumerate() {
			let info = response
				.take::<Option<serde_json::Value>>(slot)?
				.with_context(|| format!("index {index} on {table} returned no build state"))?;
			let status = info["building"]["status"].as_str();
			if status == Some("ready") {
				continue;
			}
			if matches!(status, Some("error" | "aborted")) {
				bail!("index {index} on {table} failed to build: {info}");
			}
			if Instant::now() >= deadline {
				bail!("index {index} on {table} did not become ready within 60s: {info}");
			}
			remaining.push(pair);
		}
		pending = remaining;
		if !pending.is_empty() {
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{EmbeddedSchemaFile, Sync};

	#[tokio::test]
	async fn sync_tracks_original_sources_only_after_indexes_are_ready() {
		let db = crate::test_db::fresh("index_sync").await;
		let files = [
			EmbeddedSchemaFile {
				path: "schema/a.surql",
				sql: "DEFINE TABLE a; DEFINE INDEX by_value ON a FIELDS value;",
			},
			EmbeddedSchemaFile {
				path: "schema/b.surql",
				sql: "DEFINE TABLE b; DEFINE INDEX by_value ON b FIELDS value UNIQUE;",
			},
		];
		Sync::embedded(&files).run(&db).await.unwrap();
		let hashes =
			crate::sync::load_sync_hashes(&db, &crate::Module::default_module()).await.unwrap();
		for file in &files {
			assert_eq!(hashes[file.path], crate::core::sha256_hex(file.sql.as_bytes()));
		}
		let mut response = db
			.query("INFO FOR INDEX by_value ON a; INFO FOR INDEX by_value ON b;")
			.await
			.unwrap()
			.check()
			.unwrap();
		for i in 0..2 {
			let info = response.take::<Option<serde_json::Value>>(i).unwrap().unwrap();
			assert_eq!(info["building"]["status"], "ready");
		}
		// A no-edit sync must not rebuild indexes or rewrite the ledger.
		let mut response =
			db.query("SELECT * FROM __entity WHERE ns = 'sync' ORDER BY key;").await.unwrap();
		let before: Vec<serde_json::Value> = response.take(0).unwrap();
		Sync::embedded(&files).run(&db).await.unwrap();
		let mut response =
			db.query("SELECT * FROM __entity WHERE ns = 'sync' ORDER BY key;").await.unwrap();
		let after: Vec<serde_json::Value> = response.take(0).unwrap();
		assert_eq!(before, after);
	}

	#[tokio::test]
	async fn failed_populated_build_is_not_tracked_and_can_be_retried() {
		let db = crate::test_db::fresh("failed_index_sync").await;
		exec_surql(
			&db,
			"DEFINE TABLE thing; CREATE thing:a SET value=1; CREATE thing:b SET value=1;",
		)
		.await
		.unwrap();
		let files = [EmbeddedSchemaFile {
			path: "schema/index.surql",
			sql: "DEFINE INDEX unique_value ON thing FIELDS value UNIQUE;",
		}];
		assert!(Sync::embedded(&files).run(&db).await.is_err());
		let hashes =
			crate::sync::load_sync_hashes(&db, &crate::Module::default_module()).await.unwrap();
		assert!(hashes.is_empty());
		exec_surql(&db, "DELETE thing:b;").await.unwrap();
		Sync::embedded(&files).run(&db).await.expect("retry after correcting duplicate data");
		assert_eq!(
			crate::sync::load_sync_hashes(&db, &crate::Module::default_module())
				.await
				.unwrap()
				.len(),
			1
		);
	}

	#[tokio::test]
	async fn empty_indexes_are_ready_with_quoted_names_and_same_file_tables() {
		let db = crate::test_db::fresh("empty_indexes").await;
		let indexes = apply(
			&db,
			"DEFINE TABLE `odd table`; DEFINE INDEX `odd index` ON `odd table` FIELDS value;\n\
			DEFINE INDEX second ON `odd table` FIELDS other COMMENT 'CONCURRENTLY';",
		)
		.await
		.expect("apply empty-table indexes");
		wait(&db, &indexes, Instant::now() + Duration::from_secs(60)).await.expect("ready");
		let mut response = db
			.query(
				"INFO FOR INDEX `odd index` ON `odd table`; INFO FOR INDEX second ON `odd table`;",
			)
			.await
			.unwrap()
			.check()
			.unwrap();
		for i in 0..2 {
			let info = response.take::<Option<serde_json::Value>>(i).unwrap().unwrap();
			assert_eq!(info["building"]["status"], "ready");
		}
	}

	#[tokio::test]
	async fn index_can_still_create_its_table_implicitly() {
		let db = crate::test_db::fresh("implicit_index_table").await;
		let indexes = apply(&db, "DEFINE INDEX by_value ON implicit_table FIELDS value;")
			.await
			.expect("implicit table creation");
		wait(&db, &indexes, Instant::now() + Duration::from_secs(60)).await.expect("ready");
	}

	#[tokio::test]
	async fn admitted_sql_keeps_unique_enforcement_between_files() {
		let db = crate::test_db::fresh("ordered_index_writes").await;
		let files = [
			EmbeddedSchemaFile {
				path: "schema/a.surql",
				sql: "DEFINE TABLE thing; DEFINE INDEX by_value ON thing FIELDS value UNIQUE;",
			},
			EmbeddedSchemaFile {
				path: "schema/b.surql",
				sql: "CREATE thing:a SET value=1; CREATE thing:b SET value=1;",
			},
		];
		Sync::embedded(&files)
			.allow_all_statements(true)
			.run(&db)
			.await
			.expect_err("second insert must refuse");
		let mut response =
			db.query("SELECT VALUE value FROM thing;").await.unwrap().check().unwrap();
		let values: Vec<i64> = response.take(0).unwrap();
		assert_eq!(values, vec![1]);
	}

	#[test]
	fn explicit_transactions_keep_their_original_sql() {
		let sql = "BEGIN TRANSACTION; DEFINE TABLE thing; DEFINE INDEX by_value ON thing FIELDS value; COMMIT TRANSACTION;";
		let (prepared, indexes) = prepare(sql).unwrap();
		assert_eq!(prepared, sql);
		assert!(indexes.is_empty());
	}

	#[tokio::test]
	async fn populated_unique_index_still_refuses_duplicates() {
		let db = crate::test_db::fresh("populated_indexes").await;
		let err = apply(
			&db,
			"DEFINE TABLE thing; CREATE thing:a SET value=1; CREATE thing:b SET value=1;\n\
			DEFINE INDEX unique_value ON thing FIELDS value UNIQUE;",
		)
		.await
		.expect_err("duplicate values must refuse the synchronous build");
		assert!(err.to_string().contains("already contains"), "{err:#}");
	}

	#[test]
	fn rewriting_leaves_literals_and_explicit_concurrent_definitions_alone() {
		let sql = "DEFINE PARAM $text VALUE 'DEFINE INDEX fake ON absent FIELDS a';\n\
			DEFINE INDEX existing ON thing FIELDS a CONCURRENTLY;";
		let (prepared, indexes) = prepare(sql).unwrap();
		assert_eq!(prepared, sql);
		assert!(indexes.is_empty());
	}
}
