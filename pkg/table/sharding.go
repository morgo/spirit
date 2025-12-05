package table

// ShardingMetadataProvider is an interface that provides sharding configuration
// for tables. This allows external systems (like Vitess) to provide sharding
// metadata without Spirit having direct dependencies on those systems.
//
// The provider is called during table discovery to optionally configure
// sharding information for each table. If a table doesn't require sharding
// configuration (e.g., in simple MoveTables operations), the provider
// can return empty values.
//
// Example implementation for Vitess:
//
//	type VitessShardingProvider struct {
//	    vschema *vschemapb.Keyspace
//	}
//
//	func (v *VitessShardingProvider) GetShardingMetadata(schemaName, tableName string) (string, HashFunc, error) {
//	    tableVindex := v.vschema.Tables[tableName]
//	    if tableVindex == nil {
//	        return "", nil, nil // No sharding for this table
//	    }
//	    primaryVindex := tableVindex.ColumnVindexes[0]
//	    shardingColumn := primaryVindex.Column
//	    hashFunc := func(value any) (uint64, error) {
//	        return vitessVindexHash(primaryVindex.Name, value)
//	    }
//	    return shardingColumn, hashFunc, nil
//	}
type ShardingMetadataProvider interface {
	// GetShardingMetadata returns the sharding column name and hash function for a table.
	// Returns empty string and nil function if the table doesn't have sharding configuration.
	//
	// Parameters:
	//   - schemaName: The database/schema name
	//   - tableName: The table name
	//
	// Returns:
	//   - shardingColumn: The column name to use for sharding (e.g., "user_id")
	//   - hashFunc: The hash function to apply to the column value
	//   - error: Any error encountered while retrieving metadata
	GetShardingMetadata(schemaName, tableName string) (shardingColumn string, hashFunc HashFunc, err error)
}
