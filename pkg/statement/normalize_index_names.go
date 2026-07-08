package statement

import "fmt"

func init() { registerNormalizer(indexNameNormalizer{}) }

// indexNameNormalizer assigns MySQL's default naming convention to any unnamed
// non-PRIMARY KEY indexes. MySQL names indexes after the first column in the
// index. If that name is already taken, it appends _2, _3, etc.
// This ensures that unnamed indexes like INDEX (col) are treated identically
// to their MySQL-normalized form KEY `col` (`col`) during diff comparisons.
type indexNameNormalizer struct{}

func (indexNameNormalizer) Name() string { return "auto-name-indexes" }

func (indexNameNormalizer) Normalize(ct *CreateTable) *CreateTable {
	// Track all used index names (including already-named indexes)
	usedNames := make(map[string]int) // name -> highest suffix used (0 = base name)
	for i := range ct.Indexes {
		if ct.Indexes[i].Name != "" {
			usedNames[ct.Indexes[i].Name] = 0
		}
	}

	// Inline column-level UNIQUEs claim their server-assigned names BEFORE any
	// unnamed table-level index is auto-named: the server names indexes in
	// declaration order and column definitions normally precede table-level
	// keys, so in `c int UNIQUE, KEY (c, d)` the unique index gets `c` and the
	// unnamed key is pushed to `c_2`. The names are only reserved here — not
	// materialized into ct.Indexes — so the inline declaration stays
	// column-level state (Column.Unique) everywhere else. effectiveIndexes
	// recomputes the same names at diff time; keep the two in sync.
	for _, col := range ct.Columns {
		if !col.Unique {
			continue
		}
		name := col.Name
		for suffix := 2; ; suffix++ {
			if _, taken := usedNames[name]; !taken {
				break
			}
			name = fmt.Sprintf("%s_%d", col.Name, suffix)
		}
		usedNames[name] = 0
	}

	for i := range ct.Indexes {
		idx := &ct.Indexes[i]
		if idx.Name != "" || idx.Type == "PRIMARY KEY" {
			continue
		}

		// Determine the base name from the first column.
		// For expression indexes the first column name may be empty;
		// fall back to a functional_index convention similar to MySQL 8.0.
		var baseName string
		switch {
		case len(idx.Columns) > 0 && idx.Columns[0] != "":
			baseName = idx.Columns[0]
		case len(idx.ColumnList) > 0 && idx.ColumnList[0].Name != "":
			baseName = idx.ColumnList[0].Name
		case len(idx.ColumnList) > 0:
			baseName = "functional_index"
		default:
			baseName = "idx"
		}

		// Find a unique name following MySQL's convention:
		// first try baseName, then baseName_2, baseName_3, ...
		candidate := baseName
		if _, taken := usedNames[candidate]; taken {
			// Start from _2 and increment
			suffix := 2
			if prev, ok := usedNames[baseName]; ok && prev >= 2 {
				suffix = prev + 1
			}
			for {
				candidate = fmt.Sprintf("%s_%d", baseName, suffix)
				if _, taken := usedNames[candidate]; !taken {
					break
				}
				suffix++
			}
		}

		idx.Name = candidate
		usedNames[candidate] = 0
		// Track the highest suffix used for this base name
		if candidate != baseName {
			// Extract suffix number
			var suffixNum int
			if _, err := fmt.Sscanf(candidate, baseName+"_%d", &suffixNum); err == nil {
				if current, ok := usedNames[baseName]; !ok || suffixNum > current {
					usedNames[baseName] = suffixNum
				}
			}
		}
	}
	return ct
}
