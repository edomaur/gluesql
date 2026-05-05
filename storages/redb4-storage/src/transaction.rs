use super::{Redb4Storage, StorageError, TransactionState};

type Result<T> = std::result::Result<T, StorageError>;

pub fn begin(storage: &mut Redb4Storage, autocommit: bool) -> Result<bool> {
    match (&storage.state, autocommit) {
        (TransactionState::Active { .. }, true) => Ok(false),
        (TransactionState::Active { .. }, false) => {
            Err(StorageError::NestedTransactionNotSupported)
        }
        (TransactionState::None, _) => {
            let write_txn = storage.db.begin_write()?;
            storage.state = TransactionState::Active {
                txn: Box::new(write_txn),
                autocommit,
            };
            Ok(autocommit)
        }
    }
}

pub fn rollback(storage: &mut Redb4Storage) -> Result<()> {
    if let Some(txn) = storage.take_txn() {
        txn.abort()?;
    }
    Ok(())
}

pub fn commit(storage: &mut Redb4Storage) -> Result<()> {
    if let Some(txn) = storage.take_txn() {
        txn.commit()?;
    }
    Ok(())
}
