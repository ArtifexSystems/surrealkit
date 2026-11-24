# AIEUS SurrealDB 4 SurrealKit build

Base: upstream `v1.0.0-beta.7` (`9e7fc4f`).

Changes for AIEUS SurrealDB 4 schema apply:

- `surrealdb` / `surrealdb-types` crates pinned to **4.0.0** (SDK range `>=3.0.0-alpha.1, <5.0.0`).
- Dropped Cargo feature `kv-rocksdb` (removed from SurrealDB 4 crates).
- `--version` prints `surrealkit <CARGO_PKG_VERSION>` so AIEUS `deploy/docker/surrealkit.tag` matching stays exact.
- Package version: `1.0.0-beta.7+aieus.sd4.1`.

Proved `surrealkit sync --no-prune --fail-fast` against SurrealDB **3.3.0** and **4.0.0** (table apply + `__entity` ledger).

## sd4.3 (JANUS, 2026-11-19)

- `sync` applies changed declarative files in batches of up to 200 statements: each batch's table definitions in one
  transaction, then the rest in another, instead of one commit per statement. File hashes are written in one
  transaction. A fresh AIEUS Core schema (122 files, 1,038 definitions) went from 7.3-7.4 s to 2.25-2.66 s against
  SurrealDB 4.0 on RocksDB; inside a full AIEUS create the sync went from 10.4 s to 1.8 s.
- Tables commit before their fields because SurrealDB 4 marks fields defined in the table's own transaction as
  guaranteed; with the split the stored schema and the `__entity` ledger equal file-by-file apply.
- A failing batch is applied again file by file, so errors name their file and the same files are tracked.
- Package version: `1.0.0-beta.7+aieus.sd4.3`.

## sd4.4 (CURIE, 2026-11-24)

- An existing `__entity` row is updated by its record id. The partition is not scanned once per changed key.
- A file hash already stored is updated in place. A new path is created. The hash is still written only after that file's defines and indexes succeed.
- On SurrealDB 4.0.0 RocksDB, a define-only Core schema (133 files) cold-synced in 4.21 s. Two comment-bump re-syncs took 8.08 s and 8.19 s. The same re-sync on sd4.3 took about 28 s.
- Package version: `1.0.0-beta.7+aieus.sd4.4`.
