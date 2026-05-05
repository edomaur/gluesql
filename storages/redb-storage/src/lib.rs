#![deny(clippy::str_to_string)]

mod core;
mod error;
mod index;
mod index_mut;
mod index_sync;
mod migration;
mod store;
mod store_mut;
mod transaction;

pub use migration::{MigrationReport, REDB_STORAGE_FORMAT_VERSION, migrate_to_latest};

use {
    async_trait::async_trait,
    core::StorageCore,
    gluesql_core::{
        ast::Statement,
        data::{Key, Schema, Value},
        error::Result,
        plan::{
            fetch_schema_map, plan_aggregate, plan_index, plan_join, plan_primary_key,
            plan_schemaless, validate,
        },
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, Index, IndexMut, Metadata, Planner,
            RowIter, Store, StoreMut, Transaction,
        },
    },
    redb::Database,
    std::path::Path,
};

pub struct RedbStorage(StorageCore);

impl RedbStorage {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        StorageCore::new(filename).map(Self).map_err(Into::into)
    }

    pub fn from_database(db: Database) -> Result<Self> {
        StorageCore::from_database(db).map(Self).map_err(Into::into)
    }
}

#[async_trait]
impl Store for RedbStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        self.0.fetch_all_schemas().map_err(Into::into)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        self.0.fetch_schema(table_name).map_err(Into::into)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        self.0.fetch_data(table_name, key).map_err(Into::into)
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        self.0.scan_data(table_name).map_err(Into::into)
    }
}

#[async_trait]
impl StoreMut for RedbStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        self.0.insert_schema(schema).map_err(Into::into)
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        self.0.delete_schema(table_name).map_err(Into::into)
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        self.0.append_data(table_name, rows).await
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        self.0.insert_data(table_name, rows).await
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        self.0.delete_data(table_name, keys).await
    }
}

#[async_trait]
impl Transaction for RedbStorage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        self.0.begin(autocommit).map_err(Into::into)
    }

    async fn rollback(&mut self) -> Result<()> {
        self.0.rollback().map_err(Into::into)
    }

    async fn commit(&mut self) -> Result<()> {
        self.0.commit().map_err(Into::into)
    }
}

#[async_trait]
impl Index for RedbStorage {
    async fn scan_indexed_data<'a>(
        &'a self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&gluesql_core::ast::IndexOperator, Value)>,
    ) -> Result<RowIter<'a>> {
        index::scan_indexed_data(&self.0, table_name, index_name, asc, cmp_value)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl IndexMut for RedbStorage {
    async fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &gluesql_core::ast::OrderByExpr,
    ) -> Result<()> {
        index_mut::create_index(&mut self.0, table_name, index_name, column)
            .await
            .map_err(Into::into)
    }

    async fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        index_mut::drop_index(&mut self.0, table_name, index_name)
            .await
            .map_err(Into::into)
    }
}

impl AlterTable for RedbStorage {}
impl Metadata for RedbStorage {}
impl CustomFunction for RedbStorage {}
impl CustomFunctionMut for RedbStorage {}

#[async_trait]
impl Planner for RedbStorage {
    async fn plan(&self, statement: Statement) -> Result<Statement> {
        let schema_map = fetch_schema_map(self, &statement).await?;
        validate(&schema_map, &statement)?;

        let statement = plan_schemaless(&schema_map, statement)?;
        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_index(&schema_map, statement);
        let statement = plan_join(&schema_map, statement);
        let statement = plan_aggregate(statement);

        Ok(statement)
    }
}
