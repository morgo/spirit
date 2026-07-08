package statement

// This file holds accessor helpers on the collection types (Indexes, Columns,
// Constraints) and their elements — presence checks and name lookups. They are
// methods on those slice/element types, not on CreateTable.

func (indexes Indexes) HasInvisible() bool {
	for _, idx := range indexes {
		if idx.Invisible != nil && *idx.Invisible {
			return true
		}
	}

	return false
}

func (constraints Constraints) HasForeignKeys() bool {
	for _, c := range constraints {
		if c.Type == "FOREIGN KEY" {
			return true
		}
	}

	return false
}

// ByName is a generic function that finds an element by name in any slice of types with Name field
// NOTE: This function assumes that names are unique within the slice! That will be true for
// "canonical" CREATE TABLE statements as returned by SHOW CREATE TABLE, but may not be true for
// arbitrary input.
func ByName[T HasName](slice []T, name string) *T {
	for _, item := range slice {
		if item.GetName() == name {
			return &item
		}
	}

	return nil
}

func (i Index) GetName() string {
	return i.Name
}

func (c Column) GetName() string {
	return c.Name
}

func (c Constraint) GetName() string {
	return c.Name
}

func (indexes Indexes) ByName(name string) *Index {
	return ByName(indexes, name)
}

func (columns Columns) ByName(name string) *Column {
	return ByName(columns, name)
}

func (constraints Constraints) ByName(name string) *Constraint {
	return ByName(constraints, name)
}
