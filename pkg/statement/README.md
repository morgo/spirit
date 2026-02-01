# Statement

The statement package provides SQL statement parsing and analysis capabilities for Spirit. It wraps the [TiDB parser](https://github.com/pingcap/tidb/tree/master/pkg/parser) to extract structured information from DDL statements and determine their safety characteristics for online schema changes.

## Design Philosophy

Spirit needs to understand DDL statements to:
1. **Validate** that statements are supported for online migration (i.e., not an INSERT statement)
2. **Extract** table names, schema names, and ALTER clauses
3. **Analyze** whether operations are safe for INPLACE algorithm
4. **Transform** statements (e.g., rewrite `CREATE INDEX` to `ALTER TABLE`)
5. **Parse** CREATE TABLE statements into structured data for comparison

Rather than implementing a custom parser, Spirit leverages the TiDB parser, which provides:
- Battle-tested SQL parsing compatible with MySQL syntax
- AST (Abstract Syntax Tree) representation of statements
- Ability to restore modified ASTs back to SQL

The statement package adds Spirit-specific logic on top of the parser, such as safety analysis and structured CREATE TABLE parsing.

## Core Types

### AbstractStatement

`AbstractStatement` represents a parsed DDL statement with extracted metadata:

```go
type AbstractStatement struct {
    Schema    string          // Schema name (if fully qualified)
    Table     string          // Table name
    Alter     string          // ALTER clause (empty for non-ALTER statements)
    Statement string          // Original SQL statement
    StmtNode  *ast.StmtNode   // Parsed AST node
}
```

**Key Points:**
- For multi-table statements (e.g., `DROP TABLE t1, t2`), only the first table is stored in `Table`
- `Alter` contains the normalized ALTER clause without `ALTER TABLE table_name` prefix
- `StmtNode` provides access to the full AST for advanced operations

### CreateTable

`CreateTable` represents a parsed CREATE TABLE statement with structured access to all components:

```go
type CreateTable struct {
    Raw          *ast.CreateTableStmt
    TableName    string
    Temporary    bool
    IfNotExists  bool
    Columns      Columns
    Indexes      Indexes
    Constraints  Constraints
    TableOptions *TableOptions
    Partition    *PartitionOptions
}
```

This structured representation makes it easy to:
- Compare table definitions
- Extract specific columns or indexes
- Generate modified CREATE TABLE statements
- Validate table structure

## Supported Statements

### ALTER TABLE

The primary statement type for Spirit migrations:

```go
stmts, err := statement.New("ALTER TABLE t1 ADD COLUMN c INT")
// stmts[0].Table = "t1"
// stmts[0].Alter = "ADD COLUMN `c` INT"
```

**Features:**
- Normalizes ALTER clauses (adds backticks, standardizes formatting)
- Supports fully qualified table names (`schema.table`)
- Can parse multiple ALTER statements in one call
- Validates that ALGORITHM and LOCK clauses are not present (Spirit manages these)

### CREATE TABLE

Supports CREATE TABLE for table creation operations:

```go
stmts, err := statement.New("CREATE TABLE t1 (id INT PRIMARY KEY)")
// stmts[0].Table = "t1"
// stmts[0].Alter = "" (empty for non-ALTER)
```

For structured parsing:

```go
ct, err := statement.ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY)")
// ct.TableName = "t1"
// ct.Columns[0].Name = "id"
// ct.Columns[0].Type = "int"
// ct.Columns[0].PrimaryKey = true
```

### CREATE INDEX

Automatically rewritten to ALTER TABLE:

```go
stmts, err := statement.New("CREATE INDEX idx ON t1 (a)")
// stmts[0].Table = "t1"
// stmts[0].Alter = "ADD INDEX idx (a)"
// stmts[0].Statement = "/* rewritten from CREATE INDEX */ ALTER TABLE `t1` ADD INDEX idx (a)"
```

**Limitations:**
- Functional indexes cannot be converted (use `ALTER TABLE ADD INDEX` directly). See [issue 444](https://github.com/block/spirit/issues/444).

### DROP TABLE

Supports DROP TABLE operations:

```go
stmts, err := statement.New("DROP TABLE t1")
// stmts[0].Table = "t1"
// stmts[0].Alter = "" (empty for non-ALTER)
```

**Validation:**
- Multi-table drops must use the same schema (e.g., `DROP TABLE test.t1, test.t2` is valid, but `DROP TABLE test.t1, prod.t2` is not)

### RENAME TABLE

Supports RENAME TABLE operations:

```go
stmts, err := statement.New("RENAME TABLE t1 TO t2")
// stmts[0].Table = "t1"
// stmts[0].Alter = "" (empty for non-ALTER)
```

**Validation:**
- Cannot rename across schemas (e.g., `RENAME TABLE test.t1 TO prod.t2` is rejected)

## Safety Analysis

The statement package provides methods to determine if ALTER operations are safe for online execution.

### AlgorithmInplaceConsideredSafe

Determines if an ALTER statement can use MySQL's INPLACE algorithm safely:

```go
stmt := statement.MustNew("ALTER TABLE t1 RENAME INDEX a TO b")[0]
err := stmt.AlgorithmInplaceConsideredSafe()
// err == nil (safe - metadata-only operation)

stmt = statement.MustNew("ALTER TABLE t1 ADD COLUMN c INT")[0]
err = stmt.AlgorithmInplaceConsideredSafe()
// err == ErrUnsafeForInplace (unsafe - requires table rebuild)
```

This feature exists because some DDL changes in MySQL only respond to the `INPLACE` DDL assertion, even though they are actually `INSTANT` operations (metadata-only). Since not all `INPLACE` operations are safe for online execution, we explicitly parse the statement to identify only known safe operations. See [https://bugs.mysql.com/bug.php?id=113355](https://bugs.mysql.com/bug.php?id=113355).

### AlterContainsUnsupportedClause

Checks for clauses that conflict with Spirit's operation:

```go
stmt := statement.MustNew("ALTER TABLE t1 ADD INDEX (a), ALGORITHM=INPLACE")[0]
err := stmt.AlterContainsUnsupportedClause()
// err != nil (ALGORITHM clause not allowed)
```

**Unsupported Clauses:**
- `ALGORITHM=...` (Spirit manages algorithm selection)
- `LOCK=...` (Spirit manages locking strategy)

### AlterContainsAddUnique

Detects if an ALTER adds a UNIQUE index:

```go
stmt := statement.MustNew("ALTER TABLE t1 ADD UNIQUE INDEX (email)")[0]
err := stmt.AlterContainsAddUnique()
// err == ErrAlterContainsUnique
```

This is used to customize the error message if a checksum operation fails. This is because adding a `UNIQUE` index on non-unique data will result in a checksum failure, and it's helpful to hint this out to the user.

## CREATE TABLE Parsing

The package provides detailed parsing of CREATE TABLE statements into structured data. This is extensively used by the `lint` package.

### Basic Usage

```go
ct, err := statement.ParseCreateTable(`
    CREATE TABLE users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        email VARCHAR(255) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status ENUM('active', 'inactive') DEFAULT 'active'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
`)

// Access columns
for _, col := range ct.Columns {
    fmt.Printf("%s: %s\n", col.Name, col.Type)
}

// Access indexes
for _, idx := range ct.Indexes {
    fmt.Printf("%s (%s): %v\n", idx.Name, idx.Type, idx.Columns)
}

// Access table options
if ct.TableOptions.Engine != nil {
    fmt.Printf("Engine: %s\n", *ct.TableOptions.Engine)
}
```

### Column Information

Each `Column` provides detailed information:

```go
type Column struct {
    Name       string
    Type       string            // "int", "varchar", "decimal", etc.
    Length     *int              // For VARCHAR(100), Length = 100
    Precision  *int              // For DECIMAL(10,2), Precision = 10
    Scale      *int              // For DECIMAL(10,2), Scale = 2
    Unsigned   *bool
    EnumValues []string          // For ENUM('a','b'), EnumValues = ["a", "b"]
    SetValues  []string          // For SET('x','y'), SetValues = ["x", "y"]
    Nullable   bool
    Default    *string
    AutoInc    bool
    PrimaryKey bool              // Column-level PRIMARY KEY
    Unique     bool              // Column-level UNIQUE
    Comment    *string
    Charset    *string
    Collation  *string
}
```

**Example:**

```go
col := ct.Columns.ByName("email")
// col.Name = "email"
// col.Type = "varchar"
// col.Length = 255
// col.Nullable = false
// col.Unique = true
```

### Index Information

Each `Index` provides:

```go
type Index struct {
    Name         string
    Type         string            // "PRIMARY KEY", "UNIQUE", "INDEX", "FULLTEXT"
    Columns      []string
    Invisible    *bool
    Using        *string           // "BTREE", "HASH", etc.
    Comment      *string
    KeyBlockSize *uint64
    ParserName   *string           // For FULLTEXT indexes
}
```

**Example:**

```go
idx := ct.Indexes.ByName("PRIMARY")
// idx.Type = "PRIMARY KEY"
// idx.Columns = ["id"]

idx = ct.Indexes.ByName("email")
// idx.Type = "UNIQUE"
// idx.Columns = ["email"]
```

### Constraint Information

Each `Constraint` represents CHECK or FOREIGN KEY constraints:

```go
type Constraint struct {
    Name       string
    Type       string                // "CHECK", "FOREIGN KEY"
    Columns    []string
    Expression *string               // For CHECK constraints
    References *ForeignKeyReference  // For FOREIGN KEY constraints
    Definition *string               // Full constraint definition
}
```

### Partition Information

For partitioned tables, `PartitionOptions` provides:

```go
type PartitionOptions struct {
    Type         string                // "RANGE", "LIST", "HASH", "KEY"
    Expression   *string               // For HASH and RANGE
    Columns      []string              // For KEY, RANGE COLUMNS, LIST COLUMNS
    Linear       bool
    Partitions   uint64
    Definitions  []PartitionDefinition
    SubPartition *SubPartitionOptions
}
```

## Helper Functions

### RemoveSecondaryIndexes

Removes secondary indexes from a CREATE TABLE statement:

```go
original := `CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(255) UNIQUE,
    name VARCHAR(100),
    INDEX idx_name (name)
)`

modified, err := statement.RemoveSecondaryIndexes(original)
// Result: CREATE TABLE with PRIMARY KEY and UNIQUE, but without idx_name
```

This functionality is used by move tables operations to defer secondary index creation. PRIMARY KEY and UNIQUE indexes are preserved because they're essential to table structure.

### GetMissingSecondaryIndexes

Compares two CREATE TABLE statements and generates ALTER TABLE to add missing indexes:

```go
source := `CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    INDEX idx_email (email),
    INDEX idx_created (created_at)
)`

target := `CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    INDEX idx_email (email)
)`

alterStmt, err := statement.GetMissingSecondaryIndexes(source, target, "t1")
// alterStmt = "ALTER TABLE `t1` ADD INDEX `idx_created` (`created_at`)"
```

This is used in combination with `RemoveSecondaryIndexes` to re-add secondary indexes in move tables operations.

## Usage Examples

### Basic Statement Parsing

```go
stmts, err := statement.New("ALTER TABLE users ADD COLUMN age INT")
if err != nil {
    return err
}

for _, stmt := range stmts {
    fmt.Printf("Table: %s\n", stmt.Table)
    fmt.Printf("Alter: %s\n", stmt.Alter)
    
    if stmt.IsAlterTable() {
        // Perform safety checks
        if err := stmt.AlgorithmInplaceConsideredSafe(); err != nil {
            fmt.Println("Requires Spirit migration")
        } else {
            fmt.Println("Can use native INPLACE")
        }
    }
}
```

### Multiple Statements

```go
sql := `
    ALTER TABLE t1 ADD COLUMN c1 INT;
    ALTER TABLE t2 ADD INDEX (c2);
    ALTER TABLE t3 RENAME INDEX old TO new;
`

stmts, err := statement.New(sql)
if err != nil {
    return err
}

// Process each statement
for _, stmt := range stmts {
    fmt.Printf("Processing %s.%s: %s\n", stmt.Schema, stmt.Table, stmt.Alter)
}
```

### CREATE TABLE Analysis

```go
// Get canonical CREATE TABLE from database
var createStmt string
err := db.QueryRow("SHOW CREATE TABLE users").Scan(&tableName, &createStmt)
if err != nil {
    return err
}

// Parse into structured format
ct, err := statement.ParseCreateTable(createStmt)
if err != nil {
    return err
}

// Check for invisible indexes
if ct.Indexes.HasInvisible() {
    fmt.Println("Table has invisible indexes")
}

// Check for foreign keys
if ct.Constraints.HasForeignKeys() {
    fmt.Println("Table has foreign key constraints")
}

// Find specific column
col := ct.Columns.ByName("email")
if col != nil && col.Unique {
    fmt.Println("Email column has UNIQUE constraint")
}
```

### Safety Validation

```go
stmt := statement.MustNew("ALTER TABLE t1 ADD INDEX (email)")[0]

// Check if safe for INPLACE
if err := stmt.AlgorithmInplaceConsideredSafe(); err != nil {
    switch err {
    case statement.ErrUnsafeForInplace:
        fmt.Println("Requires table rebuild - use Spirit migration")
    case statement.ErrMultipleAlterClauses:
        fmt.Println("Multiple clauses with mixed safety - split into separate ALTERs")
    }
}

// Check for unsupported clauses
if err := stmt.AlterContainsUnsupportedClause(); err != nil {
    fmt.Println("Statement contains ALGORITHM or LOCK clause - remove it")
}

// Check for UNIQUE index
if err := stmt.AlterContainsAddUnique(); err == nil {
    fmt.Println("Safe to add index")
} else {
    fmt.Println("Adding UNIQUE index - may fail if duplicates exist")
}
```

### Table Comparison

```go
// Get source and target CREATE TABLE statements
sourceCreate := getCreateTable(db, "source_table")
targetCreate := getCreateTable(db, "target_table")

// Find missing indexes
alterStmt, err := statement.GetMissingSecondaryIndexes(sourceCreate, targetCreate, "target_table")
if err != nil {
    return err
}

if alterStmt != "" {
    fmt.Println("Need to add indexes:", alterStmt)
    // Execute alterStmt to bring target in sync with source
}
```

## Limitations

1. **Functional Indexes**: `CREATE INDEX` with functional expressions cannot be converted to `ALTER TABLE`
2. **Single Schema**: Multi-table operations must use the same schema
3. **SPATIAL Indexes**: Not fully supported in some helper functions
4. **Statements must be parseable by the TiDB parser**: When encountered, we typically contribute fixes upstream. The most commonly occurring scenarios tend to be complex DEFAULT or CHECK expressions.

## Best Practices

1. **Use SHOW CREATE TABLE**: Always parse the output of `SHOW CREATE TABLE` rather than user-provided CREATE statements. We refer to this in some places as "the canonical show create table".
2. **Use ALTER TABLE**: Use `ALTER TABLE` syntax over `CREATE/DROP INDEX` syntax. The rewriting of `CREATE INDEX` is best-effort and does not support complex expressions.

## See Also

- [TiDB Parser Documentation](https://github.com/pingcap/tidb/tree/master/pkg/parser)
- [MySQL 8.0 Online DDL Operations](https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html)
- [pkg/table](../table/README.md) - Uses statement parsing for table metadata
- [pkg/migration](../migration/README.md) - Uses safety analysis to determine migration strategy
