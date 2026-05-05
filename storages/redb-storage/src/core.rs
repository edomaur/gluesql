use {
    super::{
        error::StorageError,
        migration::{ensure_storage_format_version_supported, initialize_storage_format_version},
    },
    bincode::deserialize,
    gluesql_core::data::Schema,
    redb::{Builder, Database, ReadableTable, TableDefinition, WriteTransaction},
    std::path::Path,
};

pub(super) const SCHEMA_TABLE_NAME: &str = "__SCHEMA__";
pub(super) const STORAGE_META_TABLE_NAME: &str = "__GLUESQL_META__";
pub(crate) const IDX_TABLE_PREFIX: &str = "__GLUESQL_IDX__";
pub(super) const SCHEMA_TABLE: TableDefinition<&str, Vec<u8>> =
    TableDefinition::new(SCHEMA_TABLE_NAME);

pub(super) type Result<T> = std::result::Result<T, StorageError>;

pub enum TransactionState {
    None,
    Active {
        txn: Box<WriteTransaction>,
        autocommit: bool,
    },
}

pub struct StorageCore {
    pub(crate) db: Database,
    pub(crate) state: TransactionState,
}

impl StorageCore {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let path = filename.as_ref();
        let db = if path.exists() {
            let db = Database::open(path)?;
            ensure_storage_format_version_supported(&db)?;
            db
        } else {
            let db = Builder::new()
                .create_with_file_format_v3(true)
                .create(path)?;
            initialize_storage_format_version(&db)?;
            db
        };

        Ok(Self {
            db,
            state: TransactionState::None,
        })
    }

    pub fn from_database(db: Database) -> Result<Self> {
        ensure_storage_format_version_supported(&db)?;

        Ok(Self {
            db,
            state: TransactionState::None,
        })
    }

    pub(crate) fn data_table_def(
        table_name: &str,
    ) -> Result<TableDefinition<'_, &'static [u8], Vec<u8>>> {
        if matches!(table_name, SCHEMA_TABLE_NAME | STORAGE_META_TABLE_NAME)
            || table_name.starts_with(IDX_TABLE_PREFIX)
        {
            return Err(StorageError::ReservedTableName(table_name.to_owned()));
        }

        Ok(TableDefinition::new(table_name))
    }

    pub(crate) fn index_table_name(table_name: &str, index_name: &str) -> String {
        format!("{IDX_TABLE_PREFIX}{table_name}__{index_name}")
    }

    pub(crate) fn read_schema(
        txn: &WriteTransaction,
        table_name: &str,
    ) -> Result<Option<Schema>> {
        let table = txn.open_table(SCHEMA_TABLE)?;
        let schema = table
            .get(table_name)?
            .map(|v| deserialize(&v.value()))
            .transpose()?;
        Ok(schema)
    }

    pub(super) fn txn(&self) -> Result<&WriteTransaction> {
        match &self.state {
            TransactionState::Active { txn, .. } => Ok(txn),
            TransactionState::None => Err(StorageError::TransactionNotFound),
        }
    }

    pub(super) fn txn_mut(&mut self) -> Result<&mut WriteTransaction> {
        match &mut self.state {
            TransactionState::Active { txn, .. } => Ok(txn),
            TransactionState::None => Err(StorageError::TransactionNotFound),
        }
    }

    pub(super) fn take_txn(&mut self) -> Option<WriteTransaction> {
        match std::mem::replace(&mut self.state, TransactionState::None) {
            TransactionState::Active { txn, .. } => Some(*txn),
            TransactionState::None => None,
        }
    }
}
