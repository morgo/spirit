package status

// Progress is returned as a struct because we may add more to it later.
// It is designed for wrappers (like a GUI) to be able to summarize the
// current status without parsing log output.
type Progress struct {
	CurrentState State  // current state, i.e. CopyRows
	Summary      string // text based representation, i.e. "12.5% copyRows ETA 1h 30m"

	// Tables contains per-table progress for multi-table migrations.
	// For single-table migrations, this will have one entry.
	Tables []TableProgress
}

// TableProgress tracks progress for a single table in the migration.
type TableProgress struct {
	TableName  string // name of the table being migrated
	RowsCopied uint64 // rows copied so far
	RowsTotal  uint64 // total rows expected
	IsComplete bool   // true if this table's copy is complete
}
