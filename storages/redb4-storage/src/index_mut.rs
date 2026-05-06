use {
    super::{Redb4Storage, SCHEMA_TABLE, StorageError, index_sync::IndexSync, read_schema},
    postcard::{from_bytes, to_allocvec},
    gluesql_core::{
        ast::OrderByExpr,
        chrono::Utc,
        data::{Key, SchemaIndex, SchemaIndexOrd, Value},
        error::{IndexError, Result},
    },
    redb::{ReadableTable, TableDefinition},
};

pub async fn create_index(
    storage: &mut Redb4Storage,
    table_name: &str,
    index_name: &str,
    column: &OrderByExpr,
) -> Result<()> {
    let txn = storage.txn_mut().map_err(StorageError::from)?;

    let mut schema = read_schema(txn, table_name)
        .map_err(StorageError::from)?
        .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;

    if schema.indexes.iter().any(|idx| idx.name == index_name) {
        return Err(IndexError::IndexNameAlreadyExists(index_name.to_owned()).into());
    }

    let new_index = SchemaIndex {
        name: index_name.to_owned(),
        expr: column.expr.clone(),
        order: SchemaIndexOrd::Both,
        created: Utc::now().naive_utc(),
    };
    schema.indexes.push(new_index.clone());

    // Save updated schema
    {
        let mut schema_table = txn.open_table(SCHEMA_TABLE).map_err(StorageError::from)?;
        let schema_bytes = to_allocvec(&schema).map_err(StorageError::from)?;
        schema_table
            .insert(table_name, schema_bytes)
            .map_err(StorageError::from)?;
    }

    // Collect all existing rows to populate the new index
    let data_def = Redb4Storage::data_table_def(table_name).map_err(StorageError::from)?;
    let existing_rows: Vec<(Vec<u8>, Vec<Value>)> = {
        let data_table = txn.open_table(data_def).map_err(StorageError::from)?;
        data_table
            .iter()
            .map_err(StorageError::from)?
            .map(|entry| {
                let v = entry.map_err(StorageError::from)?.1.value();
                let (key, row): (Key, Vec<Value>) = from_bytes(&v).map_err(StorageError::from)?;
                let key_bytes = key.to_cmp_be_bytes().map_err(StorageError::Glue)?;
                Ok((key_bytes, row))
            })
            .collect::<std::result::Result<Vec<_>, StorageError>>()?
    };

    // Populate the index for all existing rows
    let columns = schema
        .column_defs
        .as_ref()
        .map(|defs| defs.iter().map(|d| d.name.clone()).collect::<Vec<_>>());
    let sync = IndexSync {
        table_name: table_name.to_owned(),
        columns,
        indexes: vec![new_index],
    };
    for (row_key, row) in &existing_rows {
        sync.insert(txn, row_key.as_slice(), row).await?;
    }

    Ok(())
}

pub async fn drop_index(
    storage: &mut Redb4Storage,
    table_name: &str,
    index_name: &str,
) -> Result<()> {
    let txn = storage.txn_mut().map_err(StorageError::from)?;

    let mut schema = read_schema(txn, table_name)
        .map_err(StorageError::from)?
        .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;

    let idx_pos = schema
        .indexes
        .iter()
        .position(|idx| idx.name == index_name)
        .ok_or_else(|| IndexError::IndexNameDoesNotExist(index_name.to_owned()))?;
    schema.indexes.remove(idx_pos);

    // Save updated schema
    {
        let mut schema_table = txn.open_table(SCHEMA_TABLE).map_err(StorageError::from)?;
        let schema_bytes = to_allocvec(&schema).map_err(StorageError::from)?;
        schema_table
            .insert(table_name, schema_bytes)
            .map_err(StorageError::from)?;
    }

    // Drop the index redb table
    let idx_name = Redb4Storage::index_table_name(table_name, index_name);
    let idx_def: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new(&idx_name);
    let _ = txn.delete_table(idx_def);

    Ok(())
}
