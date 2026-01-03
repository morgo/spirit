package sync

import (
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	config, err := LoadConfig("testdata/example-config.yaml")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Validate basic fields
	if config.Version != "1.0" {
		t.Errorf("expected version 1.0, got %s", config.Version)
	}

	if config.Sync.Name != "production-to-analytics" {
		t.Errorf("expected name production-to-analytics, got %s", config.Sync.Name)
	}

	// Validate source
	if config.Sync.Source.DSN == "" {
		t.Error("source DSN should not be empty")
	}

	// Validate target
	if config.Sync.Target.Type != "postgresql" {
		t.Errorf("expected target type postgresql, got %s", config.Sync.Target.Type)
	}

	// Validate projections
	if len(config.Sync.Projections) != 2 {
		t.Errorf("expected 2 projections, got %d", len(config.Sync.Projections))
	}

	// Validate first projection
	proj := config.Sync.Projections[0]
	if proj.Name != "users_sync" {
		t.Errorf("expected projection name users_sync, got %s", proj.Name)
	}
	if proj.SourceTable != "users" {
		t.Errorf("expected source table users, got %s", proj.SourceTable)
	}
	if len(proj.PrimaryKey) != 1 || proj.PrimaryKey[0] != "user_id" {
		t.Errorf("expected primary key [user_id], got %v", proj.PrimaryKey)
	}

	// Validate defaults
	if config.Sync.Checksum.Interval != 1*time.Hour {
		t.Errorf("expected checksum interval 1h, got %v", config.Sync.Checksum.Interval)
	}

	if config.Sync.Performance.Threads != 4 {
		t.Errorf("expected 4 threads, got %d", config.Sync.Performance.Threads)
	}

	if config.Sync.Checkpoint.Table != "_spirit_sync_checkpoint" {
		t.Errorf("expected checkpoint table _spirit_sync_checkpoint, got %s", config.Sync.Checkpoint.Table)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Version: "1.0",
				Sync: SyncConfig{
					Name: "test",
					Source: SourceConfig{
						DSN: "user:pass@tcp(localhost:3306)/db",
					},
					Target: TargetConfig{
						Type: "postgresql",
						DSN:  "postgres://localhost/db",
					},
					Projections: []ProjectionConfig{
						{
							Name:        "test",
							SourceTable: "users",
							TargetTable: "users",
							Query:       "SELECT * FROM users",
							PrimaryKey:  []string{"id"},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing version",
			config: Config{
				Sync: SyncConfig{
					Name: "test",
				},
			},
			wantErr: true,
		},
		{
			name: "missing sync name",
			config: Config{
				Version: "1.0",
				Sync: SyncConfig{
					Source: SourceConfig{
						DSN: "user:pass@tcp(localhost:3306)/db",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "missing source DSN",
			config: Config{
				Version: "1.0",
				Sync: SyncConfig{
					Name: "test",
				},
			},
			wantErr: true,
		},
		{
			name: "missing target type",
			config: Config{
				Version: "1.0",
				Sync: SyncConfig{
					Name: "test",
					Source: SourceConfig{
						DSN: "user:pass@tcp(localhost:3306)/db",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no projections",
			config: Config{
				Version: "1.0",
				Sync: SyncConfig{
					Name: "test",
					Source: SourceConfig{
						DSN: "user:pass@tcp(localhost:3306)/db",
					},
					Target: TargetConfig{
						Type: "postgresql",
						DSN:  "postgres://localhost/db",
					},
					Projections: []ProjectionConfig{},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
