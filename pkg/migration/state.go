package migration

type State int32

const (
	StateInitial State = iota
	StateCopyRows
	StateWaitingOnSentinelTable
	StateApplyChangeset // first mass apply
	StateAnalyzeTable
	StateChecksum
	StatePostChecksum // second mass apply
	StateCutOver
	StateClose
	StateErrCleanup
)

func (s State) String() string {
	switch s {
	case StateInitial:
		return "initial"
	case StateCopyRows:
		return "copyRows"
	case StateWaitingOnSentinelTable:
		return "waitingOnSentinelTable"
	case StateApplyChangeset:
		return "applyChangeset"
	case StateAnalyzeTable:
		return "analyzeTable"
	case StateChecksum:
		return "checksum"
	case StatePostChecksum:
		return "postChecksum"
	case StateCutOver:
		return "cutOver"
	case StateClose:
		return "close"
	case StateErrCleanup:
		return "errCleanup"
	}
	return "unknown"
}
