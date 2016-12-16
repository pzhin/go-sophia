package sophia

// TxStatus transactional status
type TxStatus int

const (
	// TxOk means that transaction has been completed with error
	TxError TxStatus = -1
	// TxOk means that transaction has been completed
	TxOk TxStatus = 0
	// TxRollback status means that transaction has been rollbacked by another concurrent transaction
	TxRollback TxStatus = 1
	// TxLock status means that transaction is not finished and waiting for concurrent transaction to complete.
	// In that case commit should be retried later or transaction can be rollbacked.
	TxLock TxStatus = 2
)

// Multi-statement transaction is automatically processed when Set(), Delete(), Upsert(), Get() are used on a transactional object.
// The BeginTx() function is used to start a multi-statement transaction.
// During transaction, no updates are written to the database files until a Commit() is called.
// On commit, all modifications that were made are written to the log file in a single batch.
// To discard any changes made during transaction operation, Destroy() function should be used.
// No nested transactions are supported.
// There are no limit on a number of concurrent transactions.
// Any number of databases can be involved in a multi-statement transaction.
type Transaction interface {
	DataStore
	// Commit commits the transaction and returns it's status.
	// Any error happened during multi-statement transaction does not rollback a transaction.
	Commit() TxStatus
	// Rollback rollbacks transaction and destroy transaction object.
	Rollback() error
}

type transaction struct {
	*dataStore
}

func (tx *transaction) Commit() TxStatus {
	return TxStatus(spCommit(tx.ptr))
}

func (tx *transaction) Rollback() error {
	return spDestroy(tx.ptr)
}
