use {
    super::{
        core::{StorageCore, TransactionState},
        error::StorageError,
    },
    bincode::deserialize,
    futures::stream::iter,
    gluesql_core::{
        ast::IndexOperator,
        data::{Key, Value},
        error::{Error, Result},
        store::RowIter,
    },
    redb::{ReadableTable, TableDefinition},
    std::ops::Bound,
};

pub async fn scan_indexed_data<'a>(
    storage: &'a StorageCore,
    table_name: &str,
    index_name: &str,
    asc: Option<bool>,
    cmp_value: Option<(&IndexOperator, Value)>,
) -> Result<RowIter<'a>> {
    let txn = match &storage.state {
        TransactionState::Active { txn, .. } => txn.as_ref(),
        TransactionState::None => {
            return Err(Error::StorageMsg(
                "scan_indexed_data requires an active transaction".to_owned(),
            ));
        }
    };

    let idx_name = StorageCore::index_table_name(table_name, index_name);
    let idx_def: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new(&idx_name);

    let row_key_lists: Vec<Vec<u8>> = match txn.open_table(idx_def) {
        Ok(idx_table) => collect_row_keys(&idx_table, asc, cmp_value)?,
        Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
        Err(e) => return Err(StorageError::from(e).into()),
    };

    let data_def = StorageCore::data_table_def(table_name).map_err(StorageError::from)?;
    let data_table = txn.open_table(data_def).map_err(StorageError::from)?;

    let rows: Vec<std::result::Result<(Key, Vec<Value>), gluesql_core::error::Error>> =
        row_key_lists
            .into_iter()
            .filter_map(|row_key| {
                let result = data_table
                    .get(row_key.as_slice())
                    .map_err(StorageError::from)
                    .map_err(gluesql_core::error::Error::from)
                    .and_then(|v| match v {
                        Some(v) => {
                            let (key, row): (Key, Vec<Value>) = deserialize(&v.value())
                                .map_err(StorageError::from)
                                .map_err(gluesql_core::error::Error::from)?;
                            Ok(Some((key, row)))
                        }
                        None => Ok(None),
                    });
                result.transpose()
            })
            .collect();

    Ok(Box::pin(iter(rows)))
}

fn collect_row_keys(
    idx_table: &impl ReadableTable<&'static [u8], Vec<u8>>,
    asc: Option<bool>,
    cmp_value: Option<(&IndexOperator, Value)>,
) -> Result<Vec<Vec<u8>>> {
    let entries = match cmp_value {
        None => {
            let items: Vec<_> = match asc {
                Some(false) => idx_table
                    .iter()
                    .map_err(StorageError::from)?
                    .rev()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(StorageError::from)?,
                _ => idx_table
                    .iter()
                    .map_err(StorageError::from)?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(StorageError::from)?,
            };
            entries_to_row_keys(items)
        }
        Some((op, value)) => {
            let key_bytes = value
                .to_cmp_be_bytes()
                .map_err(gluesql_core::error::Error::from)?;
            let key_slice = key_bytes.as_slice();

            match op {
                IndexOperator::Eq => {
                    let v = idx_table
                        .get(key_slice)
                        .map_err(StorageError::from)?;
                    match v {
                        Some(v) => deserialize::<Vec<Vec<u8>>>(&v.value())
                            .map_err(StorageError::from)?,
                        None => Vec::new(),
                    }
                }
                IndexOperator::Gt => {
                    let items = idx_table
                        .range::<&[u8]>((Bound::Excluded(key_slice), Bound::Unbounded))
                        .map_err(StorageError::from)?
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(StorageError::from)?;
                    let mut keys = entries_to_row_keys(items);
                    if asc == Some(false) {
                        keys.reverse();
                    }
                    keys
                }
                IndexOperator::GtEq => {
                    let items = idx_table
                        .range(key_slice..)
                        .map_err(StorageError::from)?
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(StorageError::from)?;
                    let mut keys = entries_to_row_keys(items);
                    if asc == Some(false) {
                        keys.reverse();
                    }
                    keys
                }
                IndexOperator::Lt => {
                    let items = idx_table
                        .range(..key_slice)
                        .map_err(StorageError::from)?
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(StorageError::from)?;
                    let mut keys = entries_to_row_keys(items);
                    if asc == Some(false) {
                        keys.reverse();
                    }
                    keys
                }
                IndexOperator::LtEq => {
                    let items = idx_table
                        .range(..=key_slice)
                        .map_err(StorageError::from)?
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(StorageError::from)?;
                    let mut keys = entries_to_row_keys(items);
                    if asc == Some(false) {
                        keys.reverse();
                    }
                    keys
                }
            }
        }
    };

    Ok(entries)
}

fn entries_to_row_keys(
    items: Vec<(
        redb::AccessGuard<&[u8]>,
        redb::AccessGuard<Vec<u8>>,
    )>,
) -> Vec<Vec<u8>> {
    items
        .into_iter()
        .flat_map(|(_, v)| {
            deserialize::<Vec<Vec<u8>>>(&v.value()).unwrap_or_default()
        })
        .collect()
}
