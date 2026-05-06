use {
    super::{Redb4Storage, SCHEMA_TABLE, StorageError, index_sync::IndexSync, read_schema},
    postcard::{from_bytes, to_allocvec},
    gluesql_core::{
        data::{Key, Schema, Value},
        error::Result,
    },
    redb::ReadableTable,
    redb::{TableDefinition, WriteTransaction},
    uuid::Uuid,
};

type SResult<T> = std::result::Result<T, StorageError>;

pub fn insert_schema(storage: &mut Redb4Storage, schema: &Schema) -> SResult<()> {
    let data_def = Redb4Storage::data_table_def(&schema.table_name)?;
    let txn = storage.txn_mut()?;
    let mut table = txn.open_table(SCHEMA_TABLE)?;
    let value = to_allocvec(&schema)?;
    table.insert(schema.table_name.as_str(), value)?;
    drop(table);
    txn.open_table(data_def)?;
    Ok(())
}

pub fn delete_schema(storage: &mut Redb4Storage, table_name: &str) -> SResult<()> {
    let data_def = Redb4Storage::data_table_def(table_name)?;
    let txn = storage.txn_mut()?;

    let maybe_schema = read_schema(txn, table_name)?;

    // Drop all index tables for this table
    if let Some(ref schema) = maybe_schema {
        for index in &schema.indexes {
            let idx_name = Redb4Storage::index_table_name(table_name, &index.name);
            let idx_def: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new(&idx_name);
            let _ = txn.delete_table(idx_def);
        }
    }

    // Drop the data table (ignore if it doesn't exist yet)
    let _ = txn.delete_table(data_def);

    // Remove schema entry
    let mut schema_table = txn.open_table(SCHEMA_TABLE)?;
    schema_table.remove(table_name)?;

    Ok(())
}

pub async fn append_data(
    storage: &mut Redb4Storage,
    table_name: &str,
    rows: Vec<Vec<Value>>,
) -> Result<()> {
    let data_def = Redb4Storage::data_table_def(table_name).map_err(StorageError::from)?;
    let txn = storage.txn_mut().map_err(StorageError::from)?;

    let maybe_sync = build_index_sync(txn, table_name)?;

    // First pass: insert all rows into data table and collect row keys
    let mut inserted: Vec<(Vec<u8>, Vec<Value>)> = Vec::with_capacity(rows.len());
    {
        let mut data_table = txn.open_table(data_def).map_err(StorageError::from)?;
        for row in rows {
            let key = Key::Uuid(Uuid::now_v7().as_u128());
            let row_key = key.to_cmp_be_bytes().map_err(StorageError::Glue)?;
            let value = to_allocvec(&(&key, &row)).map_err(StorageError::from)?;
            data_table
                .insert(row_key.as_slice(), value)
                .map_err(StorageError::from)?;
            inserted.push((row_key, row));
        }
    }

    // Second pass: update indexes
    if let Some(ref sync) = maybe_sync {
        for (row_key, row) in &inserted {
            sync.insert(txn, row_key.as_slice(), row).await?;
        }
    }

    Ok(())
}

pub async fn insert_data(
    storage: &mut Redb4Storage,
    table_name: &str,
    rows: Vec<(Key, Vec<Value>)>,
) -> Result<()> {
    let data_def = Redb4Storage::data_table_def(table_name).map_err(StorageError::from)?;
    let txn = storage.txn_mut().map_err(StorageError::from)?;

    let maybe_sync = build_index_sync(txn, table_name)?;

    // First pass: upsert rows and track old/new values for index sync
    let mut changes: Vec<(Vec<u8>, Option<Vec<Value>>, Vec<Value>)> =
        Vec::with_capacity(rows.len());
    {
        let mut data_table = txn.open_table(data_def).map_err(StorageError::from)?;
        for (key, new_row) in rows {
            let row_key = key.to_cmp_be_bytes().map_err(StorageError::Glue)?;
            let old_row: Option<Vec<Value>> = data_table
                .get(row_key.as_slice())
                .map_err(StorageError::from)?
                .map(|v| from_bytes::<(Key, Vec<Value>)>(&v.value()))
                .transpose()
                .map_err(StorageError::from)?
                .map(|(_, row)| row);

            let value = to_allocvec(&(&key, &new_row)).map_err(StorageError::from)?;
            data_table
                .insert(row_key.as_slice(), value)
                .map_err(StorageError::from)?;
            changes.push((row_key, old_row, new_row));
        }
    }

    // Second pass: update indexes
    if let Some(ref sync) = maybe_sync {
        for (row_key, old_row, new_row) in &changes {
            if let Some(old) = old_row {
                sync.update(txn, row_key.as_slice(), old, new_row).await?;
            } else {
                sync.insert(txn, row_key.as_slice(), new_row).await?;
            }
        }
    }

    Ok(())
}

pub async fn delete_data(
    storage: &mut Redb4Storage,
    table_name: &str,
    keys: Vec<Key>,
) -> Result<()> {
    let data_def = Redb4Storage::data_table_def(table_name).map_err(StorageError::from)?;
    let txn = storage.txn_mut().map_err(StorageError::from)?;

    let maybe_sync = build_index_sync(txn, table_name)?;

    // First pass: remove rows from data table and collect deleted rows for index sync
    let mut deleted: Vec<(Vec<u8>, Vec<Value>)> = Vec::with_capacity(keys.len());
    {
        let mut data_table = txn.open_table(data_def).map_err(StorageError::from)?;
        for key in keys {
            let row_key = key.to_cmp_be_bytes().map_err(StorageError::Glue)?;
            let maybe_bytes: Option<Vec<u8>> = {
                let guard = data_table
                    .get(row_key.as_slice())
                    .map_err(StorageError::from)?;
                guard.map(|v| v.value().to_vec())
            };
            if let Some(bytes) = maybe_bytes {
                let (_, row): (Key, Vec<Value>) =
                    from_bytes(&bytes).map_err(StorageError::from)?;
                data_table
                    .remove(row_key.as_slice())
                    .map_err(StorageError::from)?;
                deleted.push((row_key, row));
            }
        }
    }

    // Second pass: remove from indexes
    if let Some(ref sync) = maybe_sync {
        for (row_key, row) in &deleted {
            sync.delete(txn, row_key.as_slice(), row).await?;
        }
    }

    Ok(())
}

fn build_index_sync(txn: &WriteTransaction, table_name: &str) -> Result<Option<IndexSync>> {
    let schema = read_schema(txn, table_name).map_err(StorageError::from)?;
    Ok(schema
        .filter(|s| !s.indexes.is_empty())
        .map(|s| IndexSync::new(table_name, &s)))
}
