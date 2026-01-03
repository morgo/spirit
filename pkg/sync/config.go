package sync

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete sync configuration
type Config struct {
	Version string     `yaml:"version"`
	Sync    SyncConfig `yaml:"sync"`
}

// SyncConfig contains the main sync configuration
type SyncConfig struct {
	Name        string              `yaml:"name"`
	Source      SourceConfig        `yaml:"source"`
	Target      TargetConfig        `yaml:"target"`
	Projections []ProjectionConfig  `yaml:"projections"`
	Checksum    ChecksumConfig      `yaml:"checksum"`
	Performance PerformanceConfig   `yaml:"performance"`
	Checkpoint  CheckpointConfig    `yaml:"checkpoint"`
	Logging     LoggingConfig       `yaml:"logging"`
}

// SourceConfig defines the MySQL source database
type SourceConfig struct {
	DSN string    `yaml:"dsn"`
	TLS TLSConfig `yaml:"tls,omitempty"`
}

// TLSConfig defines TLS settings
type TLSConfig struct {
	Mode       string `yaml:"mode,omitempty"`        // disabled, preferred, required, verify-ca, verify-identity
	CACert     string `yaml:"ca_cert,omitempty"`
	ClientCert string `yaml:"client_cert,omitempty"`
	ClientKey  string `yaml:"client_key,omitempty"`
}

// TargetConfig defines the target database/warehouse
type TargetConfig struct {
	Type        string              `yaml:"type"` // postgresql, iceberg
	DSN         string              `yaml:"dsn,omitempty"`
	Catalog     *IcebergCatalog     `yaml:"catalog,omitempty"`
	QueryEngine *IcebergQueryEngine `yaml:"query_engine,omitempty"`
}

// IcebergCatalog defines Iceberg catalog configuration
type IcebergCatalog struct {
	Type      string `yaml:"type"`      // glue, hive, rest
	Warehouse string `yaml:"warehouse"` // S3 path
	Database  string `yaml:"database"`
}

// IcebergQueryEngine defines the SQL engine for querying Iceberg
type IcebergQueryEngine struct {
	Type string `yaml:"type"` // trino, spark, dremio
	DSN  string `yaml:"dsn"`
}

// ProjectionConfig defines a table projection
type ProjectionConfig struct {
	Name         string            `yaml:"name"`
	SourceTable  string            `yaml:"source_table"`
	TargetTable  string            `yaml:"target_table"`
	Query        string            `yaml:"query"`
	PrimaryKey   []string          `yaml:"primary_key"`
	TypeMappings map[string]string `yaml:"type_mappings,omitempty"`
}

// ChecksumConfig defines checksum behavior
type ChecksumConfig struct {
	Enabled    bool          `yaml:"enabled"`
	Interval   time.Duration `yaml:"interval"`
	MaxRetries int           `yaml:"max_retries"`
}

// PerformanceConfig defines performance tuning parameters
type PerformanceConfig struct {
	Threads            int           `yaml:"threads"`
	TargetChunkTime    time.Duration `yaml:"target_chunk_time"`
	MaxReplicationLag  time.Duration `yaml:"max_replication_lag"`
}

// CheckpointConfig defines checkpoint behavior
type CheckpointConfig struct {
	Interval time.Duration `yaml:"interval"`
	Table    string        `yaml:"table"`
}

// LoggingConfig defines logging behavior
type LoggingConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // json, text
	Output string `yaml:"output"` // file path or stdout
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Version == "" {
		return fmt.Errorf("version is required")
	}

	if c.Sync.Name == "" {
		return fmt.Errorf("sync.name is required")
	}

	if c.Sync.Source.DSN == "" {
		return fmt.Errorf("sync.source.dsn is required")
	}

	if c.Sync.Target.Type == "" {
		return fmt.Errorf("sync.target.type is required")
	}

	// Validate target type
	switch c.Sync.Target.Type {
	case "postgresql":
		if c.Sync.Target.DSN == "" {
			return fmt.Errorf("sync.target.dsn is required for postgresql target")
		}
	case "iceberg":
		if c.Sync.Target.Catalog == nil {
			return fmt.Errorf("sync.target.catalog is required for iceberg target")
		}
		if c.Sync.Target.Catalog.Type == "" {
			return fmt.Errorf("sync.target.catalog.type is required")
		}
		if c.Sync.Target.Catalog.Warehouse == "" {
			return fmt.Errorf("sync.target.catalog.warehouse is required")
		}
		if c.Sync.Target.Catalog.Database == "" {
			return fmt.Errorf("sync.target.catalog.database is required")
		}
	default:
		return fmt.Errorf("unsupported target type: %s", c.Sync.Target.Type)
	}

	// Validate projections
	if len(c.Sync.Projections) == 0 {
		return fmt.Errorf("at least one projection is required")
	}

	for i, proj := range c.Sync.Projections {
		if proj.Name == "" {
			return fmt.Errorf("projection[%d].name is required", i)
		}
		if proj.SourceTable == "" {
			return fmt.Errorf("projection[%d].source_table is required", i)
		}
		if proj.TargetTable == "" {
			return fmt.Errorf("projection[%d].target_table is required", i)
		}
		if proj.Query == "" {
			return fmt.Errorf("projection[%d].query is required", i)
		}
		if len(proj.PrimaryKey) == 0 {
			return fmt.Errorf("projection[%d].primary_key is required", i)
		}
	}

	// Set defaults
	if c.Sync.Checksum.Interval == 0 {
		c.Sync.Checksum.Interval = 1 * time.Hour
	}
	if c.Sync.Checksum.MaxRetries == 0 {
		c.Sync.Checksum.MaxRetries = 3
	}

	if c.Sync.Performance.Threads == 0 {
		c.Sync.Performance.Threads = 4
	}
	if c.Sync.Performance.TargetChunkTime == 0 {
		c.Sync.Performance.TargetChunkTime = 5 * time.Second
	}
	if c.Sync.Performance.MaxReplicationLag == 0 {
		c.Sync.Performance.MaxReplicationLag = 10 * time.Second
	}

	if c.Sync.Checkpoint.Interval == 0 {
		c.Sync.Checkpoint.Interval = 1 * time.Minute
	}
	if c.Sync.Checkpoint.Table == "" {
		c.Sync.Checkpoint.Table = "_spirit_sync_checkpoint"
	}

	if c.Sync.Logging.Level == "" {
		c.Sync.Logging.Level = "info"
	}
	if c.Sync.Logging.Format == "" {
		c.Sync.Logging.Format = "json"
	}
	if c.Sync.Logging.Output == "" {
		c.Sync.Logging.Output = "stdout"
	}

	return nil
}
