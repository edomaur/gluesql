use super::{
    core::{StorageCore, TransactionState},
    error::StorageError,
};

type Result<T> = std::result::Result<T, StorageError>;

impl StorageCore {
    pub fn begin(&mut self, autocommit: bool) -> Result<bool> {
        match (&self.state, autocommit) {
            (TransactionState::Active { .. }, true) => Ok(false),
            (TransactionState::Active { .. }, false) => {
                Err(StorageError::NestedTransactionNotSupported)
            }
            (TransactionState::None, _) => {
                let write_txn = self.db.begin_write()?;
                self.state = TransactionState::Active {
                    txn: Box::new(write_txn),
                    autocommit,
                };

                Ok(autocommit)
            }
        }
    }

    pub fn rollback(&mut self) -> Result<()> {
        if let Some(txn) = self.take_txn() {
            txn.abort()?;
        }

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if let Some(txn) = self.take_txn() {
            txn.commit()?;
        }

        Ok(())
    }
}
