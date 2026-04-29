package check

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestCheckTableNameConstants(t *testing.T) {
	assert.Positive(t, MaxMigratableTableNameLength)
	assert.Positive(t, NameFormatTimestampExtraChars)
	assert.Less(t, MaxMigratableTableNameLength, utils.MaxTableNameLength)
	assert.Less(t, NameFormatTimestampExtraChars, utils.MaxTableNameLength)
}

func TestCheckTableName(t *testing.T) {
	testTableName := func(name string, skipDropAfterCutover bool) error {
		r := Resources{
			Table: &table.TableInfo{
				TableName: name,
			},
			SkipDropAfterCutover: skipDropAfterCutover,
		}
		return tableNameCheck(t.Context(), r, slog.Default())
	}

	assert.NoError(t, testTableName("a", false))
	assert.NoError(t, testTableName("a", true))

	assert.ErrorContains(t, testTableName("", false), "table name must be at least 1 character")
	assert.ErrorContains(t, testTableName("", true), "table name must be at least 1 character")

	longName := "thisisareallylongtablenamethisisareallylongtablenamethisisareallylongtablename"
	assert.ErrorContains(t, testTableName(longName, false), "table name must be less than")
	assert.ErrorContains(t, testTableName(longName, true), "table name must be less than")

	// A table name at the max migratable length should pass regardless of SkipDropAfterCutover.
	// The SkipDropAfterCutover case is handled by truncation in oldTableName(), not by
	// rejecting the table name at preflight.
	exactFitName := strings.Repeat("x", MaxMigratableTableNameLength)
	assert.NoError(t, testTableName(exactFitName, false))
	assert.NoError(t, testTableName(exactFitName, true))

	// One character over the max should fail
	tooLongName := strings.Repeat("x", MaxMigratableTableNameLength+1)
	assert.ErrorContains(t, testTableName(tooLongName, false), "table name must be less than")
	assert.ErrorContains(t, testTableName(tooLongName, true), "table name must be less than")
}

func TestTruncateTableName(t *testing.T) {
	// Short name that doesn't need truncation
	assert.Equal(t, "mytable", utils.TruncateTableName("mytable", 21))

	// Name that exactly fits (64 - 21 = 43)
	name43 := strings.Repeat("a", 43)
	assert.Equal(t, name43, utils.TruncateTableName(name43, 21))

	// Name that exceeds the limit and needs truncation
	name56 := strings.Repeat("b", 56)
	assert.Equal(t, strings.Repeat("b", 43), utils.TruncateTableName(name56, 21))

	// Verify that the truncated name + suffix fits within MaxTableNameLength
	longName := strings.Repeat("c", MaxMigratableTableNameLength)
	truncated := utils.TruncateTableName(longName, NameFormatTimestampExtraChars)
	result := fmt.Sprintf(NameFormatOldTimeStamp, truncated, "20060102_150405")
	assert.LessOrEqual(t, len(result), utils.MaxTableNameLength)

	// Zero suffix length means full 64 chars available
	name64 := strings.Repeat("d", 64)
	assert.Equal(t, name64, utils.TruncateTableName(name64, 0))

	// Name longer than 64 with zero suffix gets truncated to 64
	name70 := strings.Repeat("e", 70)
	assert.Equal(t, strings.Repeat("e", 64), utils.TruncateTableName(name70, 0))
}
