use {
    super::{
        core::{StorageCore, TransactionState, SCHEMA_TABLE},
        error::StorageError,
    },
    async_stream::try_stream,
    bincode::deserialize,
    futures::stream::iter,
    gluesql_core::{
        data::{Key, Schema, Value},
        store::RowIter,
    },
    redb::ReadableTable,
};

type Result<T> = std::result::Result<T, StorageError>;

impl StorageCore {
    pub fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let txn = self.txn()?;
        let table = txn.open_table(SCHEMA_TABLE)?;

        table
            .iter()?
            .map(|entry| {
                let value = entry?.1.value();
                let schema = deserialize(&value)?;
                Ok(schema)
            })
            .collect()
    }

    pub fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let schema = match &self.state {
            TransactionState::Active { txn, .. } => txn
                .open_table(SCHEMA_TABLE)?
                .get(table_name)?
                .map(|v| deserialize(&v.value())),
            TransactionState::None => self
                .db
                .begin_write()?
                .open_table(SCHEMA_TABLE)?
                .get(table_name)?
                .map(|v| deserialize(&v.value())),
        }
        .transpose()?;

        Ok(schema)
    }

    pub fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>> {
        let txn = self.txn()?;
        let table_def = Self::data_table_def(table_name)?;
        let table = txn.open_table(table_def)?;

        let key = key.to_cmp_be_bytes()?;
        let key = key.as_slice();
        let row = table
            .get(key)?
            .map(|v| deserialize(&v.value()))
            .transpose()?
            .map(|(_, row): (Key, Vec<Value>)| row);

        Ok(row)
    }

    pub fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>> {
        if let TransactionState::Active { autocommit, txn } = &self.state
            && !autocommit
        {
            let table_def = Self::data_table_def(table_name)?;
            let table = txn.open_table(table_def)?;

            let rows: Vec<_> = table
                .iter()?
                .map(|entry| {
                    let value = entry?.1.value();
                    let (key, row): (Key, Vec<Value>) = deserialize(&value)?;

                    Ok((key, row))
                })
                .collect::<Result<_>>()?;

            return Ok(Box::pin(iter(rows.into_iter().map(Ok))));
        }

        let read_txn = self.db.begin_read()?;
        let table_def = Self::data_table_def(table_name)?;
        let table = read_txn.open_table(table_def)?;

        let rows = try_stream! {
            for entry in table.iter().map_err(Into::<StorageError>::into)? {
                let value = entry.map_err(Into::<StorageError>::into)?.1.value();
                let (key, row): (Key, Vec<Value>) = deserialize(&value).map_err(Into::<StorageError>::into)?;

                yield (key, row);
            }
        };

        Ok(Box::pin(rows))
    }
}
