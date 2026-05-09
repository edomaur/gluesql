use super::{Redb4Storage, StorageError, TransactionState};

type Result<T> = std::result::Result<T, StorageError>;

pub fn begin(storage: &mut Redb4Storage, autocommit: bool) -> Result<bool> {
    match (&storage.state, autocommit) {
        (TransactionState::Active { .. }, true) => Ok(false),
        (TransactionState::Active { .. }, false) => {
            Err(StorageError::NestedTransactionNotSupported)
        }
        // An injected transaction is already "in progress"; treat it like an active one.
        (TransactionState::Injected { .. }, _) => Ok(false),
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
    if matches!(storage.state, TransactionState::Injected { .. }) {
        return Ok(());
    }
    if let Some(txn) = storage.take_txn() {
        txn.abort()?;
    }
    Ok(())
}

pub fn commit(storage: &mut Redb4Storage) -> Result<()> {
    if matches!(storage.state, TransactionState::Injected { .. }) {
        return Ok(());
    }
    if let Some(txn) = storage.take_txn() {
        txn.commit()?;
    }
    Ok(())
}
