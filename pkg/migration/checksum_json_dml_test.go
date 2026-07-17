package migration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestJSONChecksumConcurrentDML reproduces (anonymized) a production failure:
// a migration of a table whose rows are large JSON documents of ML feature
// values fails its checksum with "checksum found differences on every attempt
// (3/3)" whenever rows are written concurrently. Rows written by the row-copy
// path are server-side copies and always match; rows written by the binlog
// applier round-trip through go-mysql's JSON rendering
// (RenderJSONAsMySQLText) and a text re-parse on INSERT, so any rendering
// divergence (double formatting, number type tags, large-format JSONB
// decoding) shows up as a per-row checksum mismatch that recopy repairs but
// concurrent DML immediately re-introduces — with enough write traffic every
// checksum attempt finds fresh differences and the migration fails fatally.
//
// The document shape mirrors the production data: nested objects whose values
// are overwhelmingly JSON doubles — integral doubles ("1.0", "36300.0",
// "108822770526.0"), long-fraction doubles ("0.10243553008595987"), values
// pre-rounded to 6 and 12 decimal places, and very small magnitudes that
// MySQL renders in fixed notation ("-0.0000053655185183743015"). Large
// documents are padded past 64KiB so the server stores them in the *large*
// JSONB object format (4-byte offsets), which the ~50KiB production rows use.
//
// The ALTER matches production: a multi-clause MODIFY + ADD COLUMN, which is
// not INSTANT-able and therefore forces a full copy + checksum.
//
// Both change clients are exercised: production runs the experimental GTID
// client, but running the binlog client too tells us whether a failure is
// client-specific (e.g. a decode difference) or shared applier/render
// behavior.
//
// Three flavors are tested:
//
//   - text_doubles: documents INSERTed as JSON text, the way an application
//     writes them. Text-parsed numbers can only be INTEGER/UNSIGNED/DOUBLE.
//   - constructed_decimals: the same documents merged with values built
//     server-side via CAST(... AS DECIMAL(m,n)), which stores DECIMAL-typed
//     JSON scalars (JSON_TYPE = "DECIMAL", rendered at full scale, e.g.
//     "169.090000"). This type is fundamentally NOT round-trippable through
//     the applier's text path: JSON text numbers never parse back to DECIMAL,
//     so a binlog-applied row re-renders as "169.09" — the documented
//     residual of go-mysql's RenderJSONAsMySQLText (go-mysql PR #1147).
//     Under a strict CAST(col AS json) checksum this failed 3/3 exactly like
//     production; table.castExpr now checksums JSON through a text
//     round-trip on both sides, so the type-tag degradation hashes equal
//     while genuine value differences still mismatch. These subtests pin
//     that behavior.
//   - text_to_json: a LONGTEXT→JSON conversion migration where the source
//     column keeps the raw application text — a mix of compact and
//     JSON_PRETTY-printed forms of the same documents. The stored source
//     bytes never match the target's canonical rendering (whitespace, key
//     order, "1.50" vs "1.5"), so the checksum must compare parsed values,
//     not text: castExpr's outermost CAST(... AS json) parses the source
//     text (the inner char cast is an identity for text columns), landing
//     both sides on the same canonical form. DECIMAL-typed scalars cannot
//     exist in a text source (parse yields only INTEGER/UNSIGNED/DOUBLE),
//     which is why this flavor has no decimals variant.
func TestJSONChecksumConcurrentDML(t *testing.T) {
	for _, tc := range []struct {
		name       string
		gtid       bool
		decimals   bool
		textSource bool
	}{
		{"binlog_text_doubles", false, false, false},
		{"gtid_text_doubles", true, false, false},
		{"binlog_constructed_decimals", false, true, false},
		{"gtid_constructed_decimals", true, true, false},
		{"binlog_text_to_json", false, false, true},
		{"gtid_text_to_json", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runJSONChecksumConcurrentDML(t, tc.gtid, tc.decimals, tc.textSource)
		})
	}
}

// decimalPatch wraps a JSON document SQL expression so it also carries four
// DECIMAL-typed scalars, at least two of which ("dec_a", "dec_b") render with
// trailing zeros at their declared scale. docExpr must be a SQL expression
// producing JSON (a quoted literal or a placeholder).
func decimalPatch(docExpr string) string {
	return `JSON_MERGE_PATCH(CAST(` + docExpr + ` AS JSON), JSON_OBJECT(` +
		`'dec_a', CAST(169.09 AS DECIMAL(12,6)), ` +
		`'dec_b', CAST(1.5 AS DECIMAL(10,2)), ` +
		`'dec_c', CAST(1966.666667 AS DECIMAL(16,6)), ` +
		`'dec_d', CAST(0.65705 AS DECIMAL(10,5))))`
}

func runJSONChecksumConcurrentDML(t *testing.T, gtid, decimals, textSource bool) {
	require.False(t, decimals && textSource, "DECIMAL-typed JSON scalars cannot exist in a text source column")
	tableName := "json_chk_binlog"
	if gtid {
		tableName = "json_chk_gtid"
	}
	if decimals {
		tableName += "_dec"
	}
	if textSource {
		tableName += "_txt"
	}
	// docExpr wraps a SQL expression producing the document (a placeholder or
	// quoted literal). pretty only applies to text sources: it stores the
	// JSON_PRETTY rendering (newlines, indentation) so the raw source text
	// diverges maximally from the target's canonical form.
	docExpr := func(placeholder string, pretty bool) string {
		if decimals {
			return decimalPatch(placeholder)
		}
		if textSource && pretty {
			return "JSON_PRETTY(" + placeholder + ")"
		}
		return placeholder
	}
	colType := "json"
	if textSource {
		colType = "longtext"
	}
	tt := testutils.NewTestTable(t, tableName, fmt.Sprintf(`CREATE TABLE %s (
		id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		token varchar(32) NOT NULL,
		customer_token varchar(32) NOT NULL,
		score smallint NOT NULL,
		score_data %s NOT NULL
	)`, tableName, colType))

	smallDoc := buildScoreDoc("1.0", 0)
	largeDoc := buildScoreDoc("1.0", 900) // >64KiB: forces large-format JSONB
	require.Greater(t, len(largeDoc), 1<<16, "large doc must exceed 64KiB to use the large JSONB object format")

	// Named rows that the concurrent-DML goroutine UPDATEs. They exist before
	// the copy starts, so the copier writes them first and the binlog applier
	// then overwrites them — exercising the UPDATE row-image path for both
	// document sizes.
	for i, r := range []struct{ token, doc string }{
		{"update-target-small-1", smallDoc},
		{"update-target-small-2", smallDoc},
		{"update-target-large-1", largeDoc},
		{"update-target-large-2", largeDoc},
	} {
		_, err := tt.DB.ExecContext(t.Context(),
			fmt.Sprintf(`INSERT INTO %s (token, customer_token, score, score_data) VALUES (?, 'C_target', 611, %s)`, tableName, docExpr("?", i%2 == 1)),
			r.token, r.doc)
		require.NoError(t, err)
	}

	// Bulk rows so the copier has work to do while DML races it. For text
	// sources these are stored pretty-printed, so no bulk row's raw text
	// matches the target's canonical rendering.
	tt.SeedRows(t, fmt.Sprintf(
		`INSERT INTO %s (token, customer_token, score, score_data) SELECT 'bulk', 'C_bulk', 611, %s`,
		tableName, docExpr("'"+smallDoc+"'", true)), 800)
	// A population of large-format rows for the copier as well.
	for range 5 {
		_, err := tt.DB.ExecContext(t.Context(), fmt.Sprintf(
			`INSERT INTO %s (token, customer_token, score, score_data) SELECT 'bulk-large', 'C_bulk', 611, %s FROM %s LIMIT 10`,
			tableName, docExpr("?", false), tableName), largeDoc)
		require.NoError(t, err)
	}

	m := NewTestRunner(t, tableName,
		"MODIFY COLUMN score_data JSON NULL, ADD COLUMN score_data_zip BLOB NULL DEFAULT NULL AFTER score_data",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(true),
		WithTestThrottler(),
		WithGTID(gtid))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// DML runs until the migration returns, so divergence keeps flowing in
	// during every checksum attempt. (A finite burst would be repaired by the
	// first attempt's recopy and the retry would then pass vacuously.)
	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(ctx, m) {
			return
		}
		docs := [2]string{buildScoreDoc("1.0", 0), buildScoreDoc("2.0", 0)}
		largeDocs := [2]string{buildScoreDoc("1.0", 900), buildScoreDoc("2.0", 900)}
		for i := 0; ; i++ {
			if ctx.Err() != nil {
				return
			}
			// UPDATEs must change the row or MySQL logs nothing; alternate
			// between two variants that differ in one double value. For text
			// sources also alternate compact/pretty forms so both raw-text
			// shapes flow through the binlog applier mid-migration.
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(
				`UPDATE %s SET score_data = %s, score = score + 1 WHERE token IN ('update-target-small-1','update-target-small-2')`, tableName, docExpr("?", i%2 == 0)),
				docs[i%2])
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(
				`UPDATE %s SET score_data = %s, score = score + 1 WHERE token IN ('update-target-large-1','update-target-large-2')`, tableName, docExpr("?", false)),
				largeDocs[i%2])
			// INSERTs exercise the binlog INSERT row-image path.
			_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(
				`INSERT INTO %s (token, customer_token, score, score_data) VALUES ('insert-during', 'C_new', 700, %s)`, tableName, docExpr("?", i%2 == 1)),
				docs[i%2])
			if i%5 == 0 {
				_, _ = tt.DB.ExecContext(ctx, fmt.Sprintf(
					`INSERT INTO %s (token, customer_token, score, score_data) VALUES ('insert-during-large', 'C_new', 700, %s)`, tableName, docExpr("?", false)),
					largeDocs[i%2])
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	// Mid-run smoking-gun sampler: poll for a row where source and _new hold
	// the SAME JSON value (numeric comparison) but DIFFERENT rendered text —
	// that combination cannot be replication lag; it is a number type-tag
	// change introduced by the applier's text round-trip (e.g. DECIMAL
	// "169.090000" re-stored as DOUBLE "169.09"). It must be captured while
	// the migration is live because the checksum's own recopy repairs it.
	// Skipped for text sources: raw text vs canonical rendering differ on
	// EVERY pretty-printed row by design, so the predicate is meaningless
	// there (and type tags cannot exist in text-sourced JSON anyway).
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		if textSource {
			return
		}
		q := fmt.Sprintf(`
			SELECT s.id, CAST(s.score_data AS CHAR), CAST(n.score_data AS CHAR)
			FROM %s s JOIN %s n USING (id)
			WHERE s.score_data = n.score_data
			  AND CAST(s.score_data AS CHAR) <> CAST(n.score_data AS CHAR)
			LIMIT 1`, tableName, "_"+tableName+"_new")
		for ctx.Err() == nil {
			var id int64
			var srcJSON, dstJSON string
			err := tt.DB.QueryRowContext(ctx, q).Scan(&id, &srcJSON, &dstJSON)
			if err == nil {
				i := firstDiff(srcJSON, dstJSON)
				t.Logf("MID-RUN SMOKING GUN: row id=%d is JSON-value-equal but text-divergent at byte %d (applier type-tag round-trip loss)", id, i)
				t.Logf("    source: …%s…", window(srcJSON, i, 90))
				t.Logf("    _new:   …%s…", window(dstJSON, i, 90))
				return
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()

	migrationErr := m.Run(ctx)
	cancel()
	<-dmlDone
	<-samplerDone
	require.NoError(t, m.Close())

	if migrationErr != nil {
		// The _new table survives a failed run (checkpoint resume relies on
		// it), so we can show WHAT diverged before failing the test.
		dumpJSONDivergence(t, tt, tableName, textSource)
	}
	require.NoError(t, migrationErr,
		"migration of a hot JSON table must pass its checksum; failure here means binlog-applied rows do not byte-match the source (applier JSON round-trip divergence)")

	// Vacuousness guards: the binlog paths must actually have been exercised.
	var insertCount int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE token LIKE 'insert-during%%'`, tableName)).Scan(&insertCount))
	require.Positive(t, insertCount, "no concurrent INSERTs reached the binlog path — test is vacuous")
	var maxScore int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		fmt.Sprintf(`SELECT MAX(score) FROM %s WHERE token LIKE 'update-target%%'`, tableName)).Scan(&maxScore))
	require.Greater(t, maxScore, 611, "no concurrent UPDATEs reached the binlog path — test is vacuous")
	if textSource {
		// The conversion coverage is only meaningful if source rows' raw text
		// actually diverges from the canonical rendering the target holds.
		var nonCanonical int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(
			`SELECT COUNT(*) FROM %s WHERE score_data <> CAST(CAST(score_data AS JSON) AS CHAR)`, tableName)).Scan(&nonCanonical))
		require.Positive(t, nonCanonical, "no source row's text diverges from canonical JSON rendering — whitespace coverage is vacuous")
	}
}

// dumpJSONDivergence logs up to three rows whose content differs between the
// source table and the _new table after a failed run, splitting the JSON
// comparison two ways:
//
//   - json_equal: MySQL JSON-value comparison (numeric, type-insensitive)
//   - text_equal: CAST(... AS CHAR) comparison (what the checksum hashes)
//
// A row with json_equal=1 AND text_equal=0 cannot be benign tail drift from
// DML applied after the applier stopped — it means the stored binary JSON
// documents hold the same values but with different number type tags (e.g.
// double 1.0 re-stored as integer 1), which is precisely an applier
// round-trip artifact. Those rows are ordered first.
//
// For text sources the raw text is parsed first (mirroring the checksum's
// castExpr), since raw-vs-canonical whitespace differences are expected and
// would drown out real divergence.
func dumpJSONDivergence(t *testing.T, tt *testutils.TestTable, tableName string, textSource bool) {
	t.Helper()
	srcExpr := "s.score_data"
	if textSource {
		srcExpr = "CAST(s.score_data AS JSON)"
	}
	query := fmt.Sprintf(`
		SELECT s.id,
		       %[1]s = n.score_data,
		       CAST(%[1]s AS CHAR), CAST(n.score_data AS CHAR),
		       s.created_at, n.created_at, s.updated_at, n.updated_at, s.score, n.score
		FROM %[2]s s JOIN %[3]s n USING (id)
		WHERE CAST(%[1]s AS CHAR) <> CAST(n.score_data AS CHAR)
		   OR s.created_at <> n.created_at
		   OR s.updated_at <> n.updated_at
		   OR s.score <> n.score
		ORDER BY (%[1]s = n.score_data) DESC, s.id
		LIMIT 3`, srcExpr, tableName, "_"+tableName+"_new")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	rows, err := tt.DB.QueryContext(ctx, query)
	if err != nil {
		t.Logf("divergence dump failed: %v", err)
		return
	}
	defer rows.Close() //nolint:errcheck // best-effort diagnostic dump
	found := false
	for rows.Next() {
		var id int64
		var jsonEqual bool
		var srcJSON, dstJSON, srcCreated, dstCreated, srcUpdated, dstUpdated string
		var srcScore, dstScore int
		if err := rows.Scan(&id, &jsonEqual, &srcJSON, &dstJSON,
			&srcCreated, &dstCreated, &srcUpdated, &dstUpdated, &srcScore, &dstScore); err != nil {
			t.Logf("divergence scan failed: %v", err)
			return
		}
		found = true
		verdict := "likely tail drift (DML applied to source after the applier stopped, or repaired by the final recopy)"
		if jsonEqual && srcJSON != dstJSON {
			verdict = "applier round-trip artifact: same JSON value, different number type tags — NOT drift"
		}
		t.Logf("DIVERGENT ROW id=%d json_equal=%v — %s", id, jsonEqual, verdict)
		if srcCreated != dstCreated || srcUpdated != dstUpdated {
			t.Logf("  timestamps: source created=%s updated=%s | target created=%s updated=%s",
				srcCreated, srcUpdated, dstCreated, dstUpdated)
		}
		if srcScore != dstScore {
			t.Logf("  score: source=%d target=%d", srcScore, dstScore)
		}
		if srcJSON != dstJSON {
			i := firstDiff(srcJSON, dstJSON)
			t.Logf("  json text differs at byte %d (source len=%d, target len=%d)", i, len(srcJSON), len(dstJSON))
			t.Logf("    source: …%s…", window(srcJSON, i, 90))
			t.Logf("    target: …%s…", window(dstJSON, i, 90))
		}
	}
	if err := rows.Err(); err != nil {
		t.Logf("divergence dump iteration failed: %v", err)
	}
	if !found {
		t.Logf("no divergent rows found between %s and _%s_new (differences may have been repaired by the final recopy)", tableName, tableName)
	}
}

func firstDiff(a, b string) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func window(s string, center, radius int) string {
	lo := max(0, center-radius)
	hi := min(len(s), center+radius)
	return s[lo:hi]
}

// buildScoreDoc returns a JSON document that mirrors the production data
// shape with anonymized key names but the original numeric literals: an ML
// score record with a features object and a weights (SHAP-like) object. seq
// is spliced in as a document version so alternating UPDATEs always change
// the row. pad appends that many filler features with realistically long key
// names (~80 bytes per entry); pad=900 pushes the document past 64KiB, which
// makes MySQL store it in the large JSONB object format like the ~50KiB
// production documents.
func buildScoreDoc(seq string, pad int) string {
	var b strings.Builder
	b.WriteString(`{"group": "SEGMENT_A", "seq": `)
	b.WriteString(seq)
	b.WriteString(`, "flag": false, "features": {`)
	// Value classes preserved verbatim from the production row: integral
	// doubles across magnitudes, long-fraction doubles, 6dp/12dp pre-rounded
	// values, and small magnitudes MySQL renders in fixed notation.
	b.WriteString(`"f01": 1.0, "f02": 0.0, "f03": 214.0, "f04": 269.0, "f05": 352.0, ` +
		`"f06": -163.0, "f07": 36300.0, "f08": 1863270.0, "f09": 108822770526.0, ` +
		`"f10": 240300104005.0, "f11": 92102400000.0, "f12": 0.10243553008595987, ` +
		`"f13": 0.25, "f14": 0.4, "f15": 0.2857142857142857, "f16": 6310.393363161819, ` +
		`"f17": 6877.391304347826, "f18": 7890.991150442478, "f19": 10247.777419722262, ` +
		`"f20": 0.9393531326083032, "f21": 0.6095849322503547, "f22": 0.8125, ` +
		`"f23": 0.65705, "f24": 1.7746281, "f25": 3.4, "f26": 12.02754821, ` +
		`"f27": 4.433380491146816, "f28": 10.03062787136294, "f29": 18437.37037037037, ` +
		`"f30": 1966.666667, "f31": 1025.292011, "f32": 169.090909, "f33": 929.14876, ` +
		`"f34": 4293.708333, "f35": 1724.104167, "f36": 24.34693877551, ` +
		`"f37": 41.137931034483, "f38": 72.945076923077, "f39": 25.282051282051, ` +
		`"f40": 25.217391304348, "f41": -5412.88, "f42": -1802.0, "f43": -1298.0, ` +
		`"f44": -2760.0, "f45": -90.0, "f46": 2937.0, "f47": 0.7607655502392344, ` +
		`"f48": 0.42857142857142855, "f49": 11.333333333333334, "f50": 16101.333333333334`)
	for i := range pad {
		fmt.Fprintf(&b, `, "pad_metric_with_realistic_key_length_%04d_by_entity_from_entity_token": %s`,
			i, padValues[i%len(padValues)])
	}
	b.WriteString(`}, "weights": {`)
	b.WriteString(`"w01": -0.007554386742413044, "w02": -0.028831277042627335, ` +
		`"w03": -0.00005240345126367174, "w04": -0.0000053655185183743015, ` +
		`"w05": -0.00000023108987079467627, "w06": 0.0000980869008344598, ` +
		`"w07": -0.000007880414159444626, "w08": -0.000008467104635201395, ` +
		`"w09": 0.02172253094613552, "w10": 0.0009676702320575714, ` +
		`"w11": 0.00033491640351712704, "w12": 0.10507708042860033, ` +
		`"w13": -0.06140952557325363, "w14": -0.17975716292858124, ` +
		`"w15": 0.047827593982219696, "w16": 0.0026966624427586794, ` +
		`"w17": -0.0026408566627651453, "w18": 0.0, "w19": -0.08228524774312973, ` +
		`"w20": 0.014391731470823288, "w21": -0.056061357259750366, ` +
		`"w22": 0.12259963154792786, "w23": -0.00015040415746625513, ` +
		`"w24": -0.0013288668124005198, "w25": -0.000007880414159444626`)
	b.WriteString(`}, "run_id": "00000000-0000-4000-8000-000000000000"}`)
	return b.String()
}

// padValues cycles through the interesting double classes for filler entries.
var padValues = []string{
	"1.0", "0.0", "36300.0", "108822770526.0", "0.10243553008595987",
	"1966.666667", "24.34693877551", "-0.0000053655185183743015",
	"6310.393363161819", "-163.0", "0.2857142857142857", "929.14876",
}
