# Plan: Complete GlueSQL Traits for `redb-storage`

## Current state

`redb-storage` (redb 2.6) fully implements `Store`, `StoreMut`, and `Transaction`. All other
traits are empty stubs. The gaps that matter:

| Trait | Gap |
|---|---|
| `Index` | empty → returns error |
| `IndexMut` | empty → returns error |
| `StoreMut` | data mutations have no index awareness |
| `Planner` | uses generic default (functional but suboptimal) |
| `AlterTable` | empty → default impl composes Store+StoreMut, which works |
| `Metadata` | empty → default returns empty iter, which is correct |
| `CustomFunction`/`Mut` | empty → neither reference storage implements these either |

---

## Step 0 — Reorganize `core.rs` following sled-storage pattern

Split the monolithic `core.rs` into focused files, matching the layout of
`storages/sled-storage/src/`:

| New file | Content moved from `core.rs` |
|---|---|
| `src/store.rs` | `// Store` impl block: `fetch_all_schemas`, `fetch_schema`, `fetch_data`, `scan_data` |
| `src/store_mut.rs` | `// StoreMut` impl block: `insert_schema`, `delete_schema`, `append_data`, `insert_data`, `delete_data` |
| `src/transaction.rs` | `// Transaction` impl block: `begin`, `rollback`, `commit` |
| `src/core.rs` | Only what remains: `TransactionState`, `StorageCore`, constants, `new`, `from_database`, and private helpers `data_table_def` / `txn` / `txn_mut` / `take_txn` |

Update `lib.rs` to declare the three new modules alongside the existing ones:
```rust
mod store;
mod store_mut;
mod transaction;
```

**No behavioral change** — pure reorganization. It is a prerequisite so that
subsequent steps can write index infrastructure directly into the correct file
(`store_mut.rs` for index-aware mutations, `core.rs` for new shared helpers) without
having to work inside a single growing file.

---

## Step 1 — Extend `core.rs` with index infrastructure

- Add `pub(crate) const IDX_TABLE_PREFIX: &str = "__GLUESQL_IDX__"`
- Update `data_table_def` to also reject names starting with `IDX_TABLE_PREFIX`
- Add `pub(crate) fn index_table_name(table_name, index_name) -> String` static method
- Add `pub(crate) fn read_schema(txn: &WriteTransaction, table_name) -> Result<Option<Schema>>`
  helper (used by `index_mut.rs`)
- Make `db` and `state` fields `pub(crate)` (required by the new modules)

---

## Step 2 — New file: `src/index_sync.rs`

Port verbatim from `redb4-storage/src/index_sync.rs`, changing only:
- `Redb4Storage::index_table_name(...)` → `StorageCore::index_table_name(...)`

`IndexSync` holds `table_name`, `columns`, and `indexes`; provides `insert`, `delete`, `update`
on a `&WriteTransaction`. The `eval_index_key` free function stays identical.

---

## Step 3 — Update `StoreMut` methods in `core.rs` for index consistency

Four methods change, all following redb4-storage's **two-pass pattern** (mutate data table
first, then update indexes):

- **`delete_schema`** — enumerate `schema.indexes`, call `txn.delete_table` for each
  `__GLUESQL_IDX__{table}__{index}` table before deleting the data table
- **`append_data`** — collect `(row_key, row)` pairs during insert, then call
  `IndexSync::insert` for each
- **`insert_data`** — track old rows during upsert, then call `IndexSync::update` (existing
  key) or `IndexSync::insert` (new key) for each
- **`delete_data`** — collect deleted `(row_key, row)` pairs, then call `IndexSync::delete`
  for each

A private `build_index_sync(txn, table_name) -> Option<IndexSync>` helper (same as
redb4-storage) skips index work when the schema has no indexes.

---

## Step 4 — New file: `src/index.rs`

Port from `redb4-storage/src/index.rs`, substituting:
- `&Redb4Storage` → `&StorageCore`
- `Redb4Storage::index_table_name` → `StorageCore::index_table_name`
- `Redb4Storage::data_table_def` → `StorageCore::data_table_def`

The redb 2.6 API difference: `db.begin_read()` is an inherent method (no `ReadableDatabase`
trait import needed). The `collect_row_keys` and `entries_to_row_keys` helpers are identical.

---

## Step 5 — New file: `src/index_mut.rs`

Port from `redb4-storage/src/index_mut.rs`, substituting:
- `&mut Redb4Storage` → `&mut StorageCore`
- All `Redb4Storage::*` static methods → `StorageCore::*`
- `read_schema` → the helper added in Step 1

Logic is identical: `create_index` reads the schema, pushes the new `SchemaIndex`, saves
updated schema, then populates index entries for all existing rows using `IndexSync`.
`drop_index` removes from schema, then calls `txn.delete_table` on the index table.

---

## Step 6 — Update `src/lib.rs`

1. Add `mod index; mod index_sync; mod index_mut;`
2. Add imports: `ast::Statement` and
   `plan::{fetch_schema_map, plan_aggregate, plan_index, plan_join, plan_primary_key, plan_schemaless, validate}`
3. Replace `impl Index for RedbStorage {}` with full delegation:
   ```rust
   async fn scan_indexed_data(...) -> Result<RowIter<'a>> {
       index::scan_indexed_data(&self.0, ...).await.map_err(Into::into)
   }
   ```
4. Replace `impl IndexMut for RedbStorage {}` with full delegation to
   `index_mut::create_index` / `drop_index`
5. Replace `impl Planner for RedbStorage {}` with the identical custom impl from redb4-storage
   (the six `plan_*` pipeline calls)

`AlterTable`, `Metadata`, `CustomFunction`, `CustomFunctionMut` stay as empty impls — the
defaults are correct for this storage.

---

## Step 7 — Update `tests/redb_storage.rs`

Add five macro calls to match redb4-storage's test coverage:

```rust
generate_index_tests!(tokio::test, RedbTester);
generate_alter_table_tests!(tokio::test, RedbTester);
generate_alter_table_index_tests!(tokio::test, RedbTester);
generate_transaction_alter_table_tests!(tokio::test, RedbTester);
generate_transaction_index_tests!(tokio::test, RedbTester);
```

---

## File summary

| File | Action |
|---|---|
| `src/core.rs` | **Split** (Step 0): keep struct/helpers only — then **add** constants, `index_table_name`, `read_schema`, update `data_table_def` guard, make `db`/`state` `pub(crate)` (Step 1) |
| `src/store.rs` | **New** (Step 0): Store impl extracted from `core.rs` |
| `src/store_mut.rs` | **New** (Step 0): StoreMut impl extracted from `core.rs` — then **update** 4 methods for index awareness (Step 3) |
| `src/transaction.rs` | **New** (Step 0): Transaction impl extracted from `core.rs` |
| `src/index_sync.rs` | **New** (Step 2, ported from redb4-storage) |
| `src/index.rs` | **New** (Step 4, ported from redb4-storage) |
| `src/index_mut.rs` | **New** (Step 5, ported from redb4-storage) |
| `src/lib.rs` | Add `store`/`store_mut`/`transaction` module decls (Step 0) — then add `index`/`index_sync`/`index_mut` decls, full `Index`/`IndexMut`/`Planner` impls (Step 6) |
| `tests/redb_storage.rs` | Add 5 test macros (Step 7) |

No `Cargo.toml` changes needed — all required crates (`gluesql-core` with its `plan` module,
`bincode`, `uuid`, `futures`) are already present.
