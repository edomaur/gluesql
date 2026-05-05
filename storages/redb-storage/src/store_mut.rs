use {
    super::{
        core::{StorageCore, SCHEMA_TABLE},
        error::StorageError,
    },
    bincode::serialize,
    gluesql_core::data::{Key, Schema, Value},
    uuid::Uuid,
};

type Result<T> = std::result::Result<T, StorageError>;

impl StorageCore {
    pub fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        let data_def = Self::data_table_def(&schema.table_name)?;
        let txn = self.txn_mut()?;
        let mut table = txn.open_table(SCHEMA_TABLE)?;
        let value = serialize(&schema)?;
        table.insert(schema.table_name.as_str(), value)?;
        txn.open_table(data_def)?;

        Ok(())
    }

    pub fn delete_schema(&mut self, table_name: &str) -> Result<()> {
        let table_def = Self::data_table_def(table_name)?;
        let txn = self.txn_mut()?;
        let mut table = txn.open_table(SCHEMA_TABLE)?;
        table.remove(table_name)?;
        txn.delete_table(table_def)?;

        Ok(())
    }

    pub fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        let table_def = Self::data_table_def(table_name)?;
        let txn = self.txn_mut()?;
        let mut table = txn.open_table(table_def)?;

        for row in rows {
            let key = Key::Uuid(Uuid::now_v7().as_u128());
            let value = serialize(&(&key, row))?;
            let table_key = key.to_cmp_be_bytes()?;
            let table_key = table_key.as_slice();
            table.insert(table_key, value)?;
        }

        Ok(())
    }

    pub fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()> {
        let table_def = Self::data_table_def(table_name)?;
        let txn = self.txn_mut()?;
        let mut table = txn.open_table(table_def)?;

        for (key, row) in rows {
            let value = serialize(&(&key, row))?;
            let table_key = key.to_cmp_be_bytes()?;
            let table_key = table_key.as_slice();
            table.insert(table_key, value)?;
        }

        Ok(())
    }

    pub fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
        let table_def = Self::data_table_def(table_name)?;
        let txn = self.txn_mut()?;
        let mut table = txn.open_table(table_def)?;

        for key in keys {
            let table_key = key.to_cmp_be_bytes()?;
            let table_key = table_key.as_slice();
            table.remove(table_key)?;
        }

        Ok(())
    }
}
