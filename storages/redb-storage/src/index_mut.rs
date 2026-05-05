use {
    super::{
        core::{StorageCore, SCHEMA_TABLE},
        error::StorageError,
        index_sync::IndexSync,
    },
    bincode::{deserialize, serialize},
    gluesql_core::{
        ast::OrderByExpr,
        chrono::Utc,
        data::{Key, SchemaIndex, SchemaIndexOrd, Value},
        error::{IndexError, Result},
    },
    redb::{ReadableTable, TableDefinition},
};

pub async fn create_index(
    storage: &mut StorageCore,
    table_name: &str,
    index_name: &str,
    column: &OrderByExpr,
) -> Result<()> {
    let txn = storage.txn_mut().map_err(StorageError::from)?;

    let mut schema = StorageCore::read_schema(txn, table_name)
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

    {
        let mut schema_table = txn.open_table(SCHEMA_TABLE).map_err(StorageError::from)?;
        let schema_bytes = serialize(&schema).map_err(StorageError::from)?;
        schema_table
            .insert(table_name, schema_bytes)
            .map_err(StorageError::from)?;
    }

    let data_def = StorageCore::data_table_def(table_name).map_err(StorageError::from)?;
    let existing_rows: Vec<(Vec<u8>, Vec<Value>)> = {
        let data_table = txn.open_table(data_def).map_err(StorageError::from)?;
        data_table
            .iter()
            .map_err(StorageError::from)?
            .map(|entry| {
                let v = entry.map_err(StorageError::from)?.1.value();
                let (key, row): (Key, Vec<Value>) =
                    deserialize(&v).map_err(StorageError::from)?;
                let key_bytes = key.to_cmp_be_bytes().map_err(StorageError::Glue)?;
                Ok((key_bytes, row))
            })
            .collect::<std::result::Result<Vec<_>, StorageError>>()?
    };

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
    storage: &mut StorageCore,
    table_name: &str,
    index_name: &str,
) -> Result<()> {
    let txn = storage.txn_mut().map_err(StorageError::from)?;

    let mut schema = StorageCore::read_schema(txn, table_name)
        .map_err(StorageError::from)?
        .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;

    let idx_pos = schema
        .indexes
        .iter()
        .position(|idx| idx.name == index_name)
        .ok_or_else(|| IndexError::IndexNameDoesNotExist(index_name.to_owned()))?;
    schema.indexes.remove(idx_pos);

    {
        let mut schema_table = txn.open_table(SCHEMA_TABLE).map_err(StorageError::from)?;
        let schema_bytes = serialize(&schema).map_err(StorageError::from)?;
        schema_table
            .insert(table_name, schema_bytes)
            .map_err(StorageError::from)?;
    }

    let idx_name = StorageCore::index_table_name(table_name, index_name);
    let idx_def: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new(&idx_name);
    let _ = txn.delete_table(idx_def);

    Ok(())
}
