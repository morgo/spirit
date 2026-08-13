// goyacc is the parser generator used to regenerate parser.go and
// hintparser.go from the .y grammar files (see the Makefile one level up).
// It is a separate Go module so its dependencies (modernc.org/*) stay out of
// the main spirit module's dependency graph: goyacc is only ever built when
// regenerating the parser, never as part of a spirit build.
module github.com/block/spirit/pkg/parser/goyacc

go 1.26

require (
	github.com/pingcap/errors v0.11.5-0.20250523034308-74f78ae071ee
	modernc.org/mathutil v1.6.0
	modernc.org/parser v1.1.0
	modernc.org/sortutil v1.2.0
	modernc.org/strutil v1.2.0
	modernc.org/y v1.1.0
)

require (
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	go.uber.org/atomic v1.6.0 // indirect
	modernc.org/golex v1.1.0 // indirect
)
