use {
    super::{Redb4Storage, StorageError},
    bincode::{deserialize, serialize},
    gluesql_core::{
        ast::Expr,
        data::{Schema, SchemaIndex, Value},
        error::Result,
        executor::{RowContext, evaluate_stateless},
    },
    redb::{ReadableTable, TableDefinition, WriteTransaction},
};

pub struct IndexSync {
    pub(crate) table_name: String,
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) indexes: Vec<SchemaIndex>,
}

impl IndexSync {
    pub fn new(table_name: &str, schema: &Schema) -> Self {
        let columns = schema.column_defs.as_ref().map(|defs| {
            defs.iter().map(|d| d.name.clone()).collect()
        });
        Self {
            table_name: table_name.to_owned(),
            columns,
            indexes: schema.indexes.clone(),
        }
    }

    pub async fn insert(
        &self,
        txn: &WriteTransaction,
        row_key: &[u8],
        row: &[Value],
    ) -> Result<()> {
        for index in &self.indexes {
            self.insert_one(txn, index, row_key, row).await?;
        }
        Ok(())
    }

    pub async fn delete(
        &self,
        txn: &WriteTransaction,
        row_key: &[u8],
        row: &[Value],
    ) -> Result<()> {
        for index in &self.indexes {
            self.delete_one(txn, index, row_key, row).await?;
        }
        Ok(())
    }

    pub async fn update(
        &self,
        txn: &WriteTransaction,
        row_key: &[u8],
        old_row: &[Value],
        new_row: &[Value],
    ) -> Result<()> {
        self.delete(txn, row_key, old_row).await?;
        self.insert(txn, row_key, new_row).await?;
        Ok(())
    }

    pub async fn insert_one(
        &self,
        txn: &WriteTransaction,
        index: &SchemaIndex,
        row_key: &[u8],
        row: &[Value],
    ) -> Result<()> {
        let idx_val_bytes = eval_index_key(&index.expr, self.columns.as_deref(), row).await?;
        let idx_name = Redb4Storage::index_table_name(&self.table_name, &index.name);
        let idx_def: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new(&idx_name);
        let mut idx_table = txn.open_table(idx_def).map_err(StorageError::from)?;

        let mut row_keys: Vec<Vec<u8>> = idx_table
            .get(idx_val_bytes.as_slice())
            .map_err(StorageError::from)?
            .map(|v| deserialize(&v.value()))
            .transpose()
            .map_err(StorageError::from)?
            .unwrap_or_default();

        if !row_keys.contains(&row_key.to_vec()) {
            row_keys.push(row_key.to_vec());
            let serialized = serialize(&row_keys).map_err(StorageError::from)?;
            idx_table
                .insert(idx_val_bytes.as_slice(), serialized)
                .map_err(StorageError::from)?;
        }

        Ok(())
    }

    async fn delete_one(
        &self,
        txn: &WriteTransaction,
        index: &SchemaIndex,
        row_key: &[u8],
        row: &[Value],
    ) -> Result<()> {
        let idx_val_bytes = eval_index_key(&index.expr, self.columns.as_deref(), row).await?;
        let idx_name = Redb4Storage::index_table_name(&self.table_name, &index.name);
        let idx_def: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new(&idx_name);
        let mut idx_table = match txn.open_table(idx_def) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(e) => return Err(StorageError::from(e).into()),
        };

        let row_keys: Vec<Vec<u8>> = match idx_table
            .get(idx_val_bytes.as_slice())
            .map_err(StorageError::from)?
        {
            Some(v) => deserialize(&v.value()).map_err(StorageError::from)?,
            None => return Ok(()),
        };

        let updated: Vec<Vec<u8>> = row_keys
            .into_iter()
            .filter(|k| k.as_slice() != row_key)
            .collect();

        if updated.is_empty() {
            idx_table
                .remove(idx_val_bytes.as_slice())
                .map_err(StorageError::from)?;
        } else {
            let serialized = serialize(&updated).map_err(StorageError::from)?;
            idx_table
                .insert(idx_val_bytes.as_slice(), serialized)
                .map_err(StorageError::from)?;
        }

        Ok(())
    }
}

pub async fn eval_index_key(
    expr: &Expr,
    columns: Option<&[String]>,
    row: &[Value],
) -> Result<Vec<u8>> {
    let context = columns.map(|cols| RowContext::RefVecData {
        columns: cols,
        values: row,
    });
    let evaluated = evaluate_stateless(context, expr).await?;
    let value: Value = evaluated.try_into()?;
    Ok(value.to_cmp_be_bytes()?)
}
