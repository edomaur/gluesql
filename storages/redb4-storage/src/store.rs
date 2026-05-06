use {
    super::{
        Redb4Storage, SCHEMA_TABLE, StorageError, TransactionState, error::StorageError as SE,
    },
    async_stream::try_stream,
    postcard::from_bytes,
    futures::stream::iter,
    gluesql_core::{
        data::{Key, Schema, Value},
        store::RowIter,
    },
    redb::{ReadableDatabase, ReadableTable},
};

type Result<T> = std::result::Result<T, StorageError>;

pub fn fetch_all_schemas(storage: &Redb4Storage) -> Result<Vec<Schema>> {
    let txn = storage.txn()?;
    let table = txn.open_table(SCHEMA_TABLE)?;
    table
        .iter()?
        .map(|entry| {
            let value = entry?.1.value();
            let schema: Schema = from_bytes(&value)?;
            Ok(schema)
        })
        .collect()
}

pub fn fetch_schema(storage: &Redb4Storage, table_name: &str) -> Result<Option<Schema>> {
    match &storage.state {
        TransactionState::Active { txn, .. } => {
            let table = txn.open_table(SCHEMA_TABLE)?;
            let schema: Option<Schema> = table
                .get(table_name)?
                .map(|v| from_bytes(&v.value()))
                .transpose()?;
            Ok(schema)
        }
        TransactionState::None => {
            let read_txn = storage.db.begin_read()?;
            let schema: Option<Schema> = match read_txn.open_table(SCHEMA_TABLE) {
                Ok(table) => table
                    .get(table_name)?
                    .map(|v| from_bytes(&v.value()))
                    .transpose()?,
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(e) => return Err(SE::RedbTable(e)),
            };
            Ok(schema)
        }
    }
}

pub fn fetch_data(
    storage: &Redb4Storage,
    table_name: &str,
    key: &Key,
) -> Result<Option<Vec<Value>>> {
    let txn = storage.txn()?;
    let table_def = Redb4Storage::data_table_def(table_name)?;
    let table = txn.open_table(table_def)?;

    let key_bytes = key.to_cmp_be_bytes()?;
    let row = table
        .get(key_bytes.as_slice())?
        .map(|v| from_bytes::<(Key, Vec<Value>)>(&v.value()))
        .transpose()?
        .map(|(_, row)| row);

    Ok(row)
}

pub fn scan_data<'a>(storage: &'a Redb4Storage, table_name: &str) -> Result<RowIter<'a>> {
    if let TransactionState::Active { autocommit, txn } = &storage.state
        && !autocommit
    {
        let table_def = Redb4Storage::data_table_def(table_name)?;
        let table = txn.open_table(table_def)?;

        let rows: Vec<_> = table
            .iter()?
            .map(|entry| {
                let value = entry?.1.value();
                let (key, row): (Key, Vec<Value>) = from_bytes(&value)?;
                Ok((key, row))
            })
            .collect::<Result<_>>()?;

        return Ok(Box::pin(iter(rows.into_iter().map(Ok))));
    }

    let table_name = table_name.to_owned();
    let read_txn = storage.db.begin_read()?;

    let rows = try_stream! {
        let table_def = Redb4Storage::data_table_def(&table_name)
            .map_err(Into::<StorageError>::into)?;
        let table = match read_txn.open_table(table_def) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return,
            Err(e) => Err(SE::RedbTable(e))?,
        };
        for entry in table.iter().map_err(Into::<StorageError>::into)? {
            let value = entry.map_err(Into::<StorageError>::into)?.1.value();
            let (key, row): (Key, Vec<Value>) =
                from_bytes(&value).map_err(Into::<StorageError>::into)?;
            yield (key, row);
        }
    };

    Ok(Box::pin(rows))
}
