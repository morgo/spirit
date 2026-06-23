package status

import "time"

// Progress is returned as a struct because we may add more to it later.
// It is designed for wrappers (like a GUI) to be able to summarize the
// current status without parsing log output.
// ETAState describes the availability of the row-copy ETA estimate, so callers
// can distinguish "still measuring" from a real estimate without parsing the
// Summary string. It mirrors the cases GetETA renders as text.
type ETAState string

const (
	// ETANone means there is no copy ETA because the migration is not in the
	// row-copy phase. ETA is 0.
	ETANone ETAState = ""
	// ETAMeasuring means a copy is in progress but no copy rate has been measured
	// yet, so no estimate is available (Summary shows "ETA TBD"). ETA is 0.
	ETAMeasuring ETAState = "measuring"
	// ETAReady means ETA holds a current remaining-time estimate.
	ETAReady ETAState = "ready"
	// ETADue means the copy is essentially complete (Summary shows "ETA DUE").
	// ETA is 0.
	ETADue ETAState = "due"
)

type Progress struct {
	CurrentState State  // current state, i.e. CopyRows
	Summary      string // text based representation, i.e. "12.5% copyRows ETA 1h 30m"

	// ETA is the estimated remaining row-copy time. It is meaningful only when
	// ETAState is ETAReady, and 0 in every other state. It is the structured form
	// of the ETA embedded in Summary.
	ETA time.Duration

	// ETAState reports whether ETA is available yet — e.g. ETAMeasuring during the
	// initial window before a copy rate is known — so callers can show
	// "calculating" rather than a misleading 0.
	ETAState ETAState

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
