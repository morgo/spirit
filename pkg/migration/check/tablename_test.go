package check

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestCheckTableNameConstants(t *testing.T) {
	require.Positive(t, MaxMigratableTableNameLength)
	require.Positive(t, NameFormatTimestampExtraChars)
	require.Less(t, MaxMigratableTableNameLength, utils.MaxTableNameLength)
	require.Less(t, NameFormatTimestampExtraChars, utils.MaxTableNameLength)
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

	require.NoError(t, testTableName("a", false))
	require.NoError(t, testTableName("a", true))

	require.ErrorContains(t, testTableName("", false), "table name must be at least 1 character")
	require.ErrorContains(t, testTableName("", true), "table name must be at least 1 character")

	longName := "thisisareallylongtablenamethisisareallylongtablenamethisisareallylongtablename"
	require.ErrorContains(t, testTableName(longName, false), "table name must be less than")
	require.ErrorContains(t, testTableName(longName, true), "table name must be less than")

	// A table name at the max migratable length should pass regardless of SkipDropAfterCutover.
	// The SkipDropAfterCutover case is handled by truncation in oldTableName(), not by
	// rejecting the table name at preflight.
	exactFitName := strings.Repeat("x", MaxMigratableTableNameLength)
	require.NoError(t, testTableName(exactFitName, false))
	require.NoError(t, testTableName(exactFitName, true))

	// One character over the max should fail
	tooLongName := strings.Repeat("x", MaxMigratableTableNameLength+1)
	require.ErrorContains(t, testTableName(tooLongName, false), "table name must be less than")
	require.ErrorContains(t, testTableName(tooLongName, true), "table name must be less than")
}

func TestTruncateTableName(t *testing.T) {
	// Short name that doesn't need truncation
	require.Equal(t, "mytable", utils.TruncateTableName("mytable", 21))

	// Name that exactly fits (64 - 21 = 43)
	name43 := strings.Repeat("a", 43)
	require.Equal(t, name43, utils.TruncateTableName(name43, 21))

	// Name that exceeds the limit and needs truncation
	name56 := strings.Repeat("b", 56)
	require.Equal(t, strings.Repeat("b", 43), utils.TruncateTableName(name56, 21))

	// Verify that the truncated name + suffix fits within MaxTableNameLength
	longName := strings.Repeat("c", MaxMigratableTableNameLength)
	truncated := utils.TruncateTableName(longName, NameFormatTimestampExtraChars)
	result := fmt.Sprintf(NameFormatOldTimeStamp, truncated, "20060102_150405")
	require.LessOrEqual(t, len(result), utils.MaxTableNameLength)

	// Zero suffix length means full 64 chars available
	name64 := strings.Repeat("d", 64)
	require.Equal(t, name64, utils.TruncateTableName(name64, 0))

	// Name longer than 64 with zero suffix gets truncated to 64
	name70 := strings.Repeat("e", 70)
	require.Equal(t, strings.Repeat("e", 64), utils.TruncateTableName(name70, 0))
}
