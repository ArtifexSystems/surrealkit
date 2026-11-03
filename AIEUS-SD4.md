# AIEUS SurrealDB 4 SurrealKit build

Base: upstream `v1.0.0-beta.7` (`9e7fc4f`).

Changes for AIEUS SurrealDB 4 schema apply:

- `surrealdb` / `surrealdb-types` crates pinned to **4.0.0** (SDK range `>=3.0.0-alpha.1, <5.0.0`).
- Dropped Cargo feature `kv-rocksdb` (removed from SurrealDB 4 crates).
- `--version` prints `surrealkit <CARGO_PKG_VERSION>` so AIEUS `deploy/docker/surrealkit.tag` matching stays exact.
- Package version: `1.0.0-beta.7+aieus.sd4.1`.

Proved `surrealkit sync --no-prune --fail-fast` against SurrealDB **3.3.0** and **4.0.0** (table apply + `__entity` ledger).
