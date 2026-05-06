#![deny(clippy::str_to_string)]

mod error;
mod index;
mod index_mut;
mod index_sync;
mod store;
mod store_mut;
mod transaction;

use {
    async_trait::async_trait,
    error::StorageError,
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
    redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction},
    std::path::Path,
};

pub const REDB4_STORAGE_FORMAT_VERSION: u32 = 1;

pub(crate) const SCHEMA_TABLE_NAME: &str = "__SCHEMA__";
pub(crate) const META_TABLE_NAME: &str = "__GLUESQL_META__";
pub(crate) const IDX_TABLE_PREFIX: &str = "__GLUESQL_IDX__";
pub(crate) const META_VERSION_KEY: &str = "storage_format_version";

pub(crate) const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> =
    TableDefinition::new(SCHEMA_TABLE_NAME);
pub(crate) const META_TABLE: TableDefinition<&str, u32> = TableDefinition::new(META_TABLE_NAME);

pub(crate) enum TransactionState {
    None,
    Active {
        txn: Box<WriteTransaction>,
        autocommit: bool,
    },
}

pub struct Redb4Storage {
    pub(crate) db: Database,
    pub(crate) state: TransactionState,
}

impl Redb4Storage {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let path = filename.as_ref();
        let db = if path.exists() {
            let db = Database::open(path).map_err(StorageError::from)?;
            ensure_format_version(&db)?;
            db
        } else {
            let db = Database::create(path).map_err(StorageError::from)?;
            initialize_format_version(&db)?;
            db
        };

        Ok(Self {
            db,
            state: TransactionState::None,
        })
    }

    pub fn from_database(db: Database) -> Result<Self> {
        ensure_format_version(&db)?;
        Ok(Self {
            db,
            state: TransactionState::None,
        })
    }

    /// Create a fully in-memory Redb4Storage (for testing).
    pub fn new_in_memory() -> Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(StorageError::from)?;
        initialize_format_version(&db)?;
        Ok(Self {
            db,
            state: TransactionState::None,
        })
    }

    pub(crate) fn txn(&self) -> std::result::Result<&WriteTransaction, StorageError> {
        match &self.state {
            TransactionState::Active { txn, .. } => Ok(txn),
            TransactionState::None => Err(StorageError::TransactionNotFound),
        }
    }

    pub(crate) fn txn_mut(&mut self) -> std::result::Result<&mut WriteTransaction, StorageError> {
        match &mut self.state {
            TransactionState::Active { txn, .. } => Ok(txn),
            TransactionState::None => Err(StorageError::TransactionNotFound),
        }
    }

    pub(crate) fn take_txn(&mut self) -> Option<WriteTransaction> {
        match std::mem::replace(&mut self.state, TransactionState::None) {
            TransactionState::Active { txn, .. } => Some(*txn),
            TransactionState::None => None,
        }
    }

    pub(crate) fn data_table_def(
        table_name: &str,
    ) -> std::result::Result<TableDefinition<'_, &'static [u8], Vec<u8>>, StorageError> {
        if table_name == SCHEMA_TABLE_NAME
            || table_name == META_TABLE_NAME
            || table_name.starts_with(IDX_TABLE_PREFIX)
        {
            return Err(StorageError::ReservedTableName(table_name.to_owned()));
        }
        Ok(TableDefinition::new(table_name))
    }

    pub(crate) fn index_table_name(table_name: &str, index_name: &str) -> String {
        format!("{IDX_TABLE_PREFIX}{table_name}__{index_name}")
    }
}

fn initialize_format_version(db: &Database) -> Result<()> {
    let txn = db.begin_write().map_err(StorageError::from)?;
    let mut table = txn.open_table(META_TABLE).map_err(StorageError::from)?;
    table
        .insert(META_VERSION_KEY, REDB4_STORAGE_FORMAT_VERSION)
        .map_err(StorageError::from)?;
    drop(table);
    txn.commit().map_err(StorageError::from)?;
    Ok(())
}

fn ensure_format_version(db: &Database) -> Result<()> {
    let txn = db.begin_read().map_err(StorageError::from)?;
    match txn.open_table(META_TABLE) {
        Ok(table) => {
            let version = table
                .get(META_VERSION_KEY)
                .map_err(StorageError::from)?
                .map(|v| v.value())
                .ok_or(StorageError::MissingFormatVersionMetadata)?;

            if version == REDB4_STORAGE_FORMAT_VERSION {
                Ok(())
            } else if version > REDB4_STORAGE_FORMAT_VERSION {
                Err(StorageError::UnsupportedNewerFormatVersion(version).into())
            } else {
                Err(StorageError::UnsupportedFormatVersion(version).into())
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {
            Err(StorageError::MissingFormatVersionMetadata.into())
        }
        Err(e) => Err(StorageError::from(e).into()),
    }
}

/// Read a schema directly from an open write transaction (used by StoreMut methods).
pub(crate) fn read_schema(
    txn: &WriteTransaction,
    table_name: &str,
) -> std::result::Result<Option<Schema>, StorageError> {
    let table = txn.open_table(SCHEMA_TABLE)?;
    let schema: Option<Schema> = table
        .get(table_name)?
        .map(|v| bincode::deserialize(&v.value()))
        .transpose()?;
    Ok(schema)
}

#[async_trait]
impl Store for Redb4Storage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        store::fetch_all_schemas(self).map_err(Into::into)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        store::fetch_schema(self, table_name).map_err(Into::into)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        store::fetch_data(self, table_name, key).map_err(Into::into)
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        store::scan_data(self, table_name).map_err(Into::into)
    }
}

#[async_trait]
impl StoreMut for Redb4Storage {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        store_mut::insert_schema(self, schema).map_err(Into::into)
    }

    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        store_mut::delete_schema(self, table_name).map_err(Into::into)
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        store_mut::append_data(self, table_name, rows)
            .await
            .map_err(Into::into)
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        store_mut::insert_data(self, table_name, rows)
            .await
            .map_err(Into::into)
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        store_mut::delete_data(self, table_name, keys)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl Transaction for Redb4Storage {
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        transaction::begin(self, autocommit).map_err(Into::into)
    }

    async fn rollback(&mut self) -> Result<()> {
        transaction::rollback(self).map_err(Into::into)
    }

    async fn commit(&mut self) -> Result<()> {
        transaction::commit(self).map_err(Into::into)
    }
}

#[async_trait]
impl Index for Redb4Storage {
    async fn scan_indexed_data<'a>(
        &'a self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&gluesql_core::ast::IndexOperator, Value)>,
    ) -> Result<RowIter<'a>> {
        index::scan_indexed_data(self, table_name, index_name, asc, cmp_value)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl IndexMut for Redb4Storage {
    async fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &gluesql_core::ast::OrderByExpr,
    ) -> Result<()> {
        index_mut::create_index(self, table_name, index_name, column)
            .await
            .map_err(Into::into)
    }

    async fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        index_mut::drop_index(self, table_name, index_name)
            .await
            .map_err(Into::into)
    }
}

impl AlterTable for Redb4Storage {}
impl Metadata for Redb4Storage {}
impl CustomFunction for Redb4Storage {}
impl CustomFunctionMut for Redb4Storage {}

#[async_trait]
impl Planner for Redb4Storage {
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
