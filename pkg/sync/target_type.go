package sync

// TargetType represents the type of target database/warehouse
type TargetType int

const (
	TargetTypeUnknown TargetType = iota
	TargetTypeMySQL
	TargetTypePostgreSQL
)

// String returns the string representation of the target type
func (t TargetType) String() string {
	switch t {
	case TargetTypeMySQL:
		return "mysql"
	case TargetTypePostgreSQL:
		return "postgresql"
	default:
		return "unknown"
	}
}

// ParseTargetType parses a string into a TargetType
func ParseTargetType(s string) TargetType {
	switch s {
	case "mysql":
		return TargetTypeMySQL
	case "postgresql":
		return TargetTypePostgreSQL
	default:
		return TargetTypeUnknown
	}
}
