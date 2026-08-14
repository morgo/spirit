package status

import (
	"fmt"
	"strings"
)

// barWidth is the number of cells in a progress bar. Wide enough that a single
// percent is close to a cell, narrow enough to leave room for the fields that
// follow it.
const barWidth = 25

const (
	barFull  = "#"
	barEmpty = "·"
)

// blockIndent prefixes every row of a Block. The header keeps the log
// handler's own prefix (timestamp, level); the rows are indented under it so
// that a block reads as one report rather than as several unrelated lines.
const blockIndent = "  "

// Bar renders fraction as a fixed-width progress bar, e.g.
// "[#######··················]" at 30%. Values outside 0..1 are clamped.
//
// The fill is truncated rather than rounded, so only a genuinely complete
// fraction renders a full bar — 99.9% showing as solid would read as done.
func Bar(fraction float64) string {
	switch {
	case fraction < 0:
		fraction = 0
	case fraction > 1:
		fraction = 1
	}
	filled := int(fraction * barWidth)
	return "[" + strings.Repeat(barFull, filled) + strings.Repeat(barEmpty, barWidth-filled) + "]"
}

// Block is the multi-line report a runner returns from Status(): a header line
// followed by one indented row per subsystem (copier, applier, change feed,
// checkpoint).
//
// The single-line form it replaces had grown to twenty-odd space-separated
// fields, which is dense but not readable: a value that changes width shifts
// every field after it, so the eye cannot follow one number down a scrollback.
// Grouping by subsystem and padding to fixed columns is what makes a spike in
// one field visible across ticks. See
// github.com/block/spirit/issues/329.
//
// Note that the whole block is one log record with newlines in it. Under the
// default slog handler (what the CLI uses) that prints as written. A handler
// that quotes the message — slog's TextHandler, or a JSON handler — will render
// the newlines escaped instead.
//
// The zero value is not useful; construct with NewBlock.
type Block struct {
	header string
	rows   []blockRow
}

type blockRow struct {
	label string
	bar   string // "" for a row without a progress bar
	text  string
}

// NewBlock starts a block with the given header line.
func NewBlock(format string, args ...any) *Block {
	return &Block{header: fmt.Sprintf(format, args...)}
}

// Row appends a labelled row of fields.
func (b *Block) Row(label, format string, args ...any) *Block {
	return b.add(label, "", fmt.Sprintf(format, args...))
}

// BarRow appends a labelled row led by a progress bar of fraction (0..1),
// followed by the given fields.
func (b *Block) BarRow(label string, fraction float64, format string, args ...any) *Block {
	return b.add(label, Bar(fraction), fmt.Sprintf(format, args...))
}

// add drops a row whose text is empty, so a caller can pass a helper that
// reports nothing — a nil applier, a change feed that does not publish stats —
// without having to test for it first. Trailing spaces are trimmed for the
// same reason: they are what is left when such a helper contributes nothing to
// the end of a row.
func (b *Block) add(label, bar, text string) *Block {
	text = strings.TrimRight(text, " ")
	if text == "" {
		return b
	}
	b.rows = append(b.rows, blockRow{label: label, bar: bar, text: text})
	return b
}

// String renders the block. Labels are padded to a common width, and rows
// without a bar start their text where the bars start, so the block has two
// stable columns however many rows the current state prints.
func (b *Block) String() string {
	if len(b.rows) == 0 {
		return b.header
	}
	width := 0
	for _, r := range b.rows {
		width = max(width, len(r.label))
	}
	var sb strings.Builder
	sb.WriteString(b.header)
	for _, r := range b.rows {
		sb.WriteString("\n")
		sb.WriteString(blockIndent)
		sb.WriteString(r.label)
		sb.WriteString(strings.Repeat(" ", width-len(r.label)+1))
		if r.bar != "" {
			sb.WriteString(r.bar)
			sb.WriteString("  ")
		}
		sb.WriteString(r.text)
	}
	return sb.String()
}
