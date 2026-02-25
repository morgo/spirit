package main

import (
	"github.com/alecthomas/kong"
	"github.com/block/spirit/pkg/buildinfo"
	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/migration"
	"github.com/block/spirit/pkg/move"
)

// Set via -ldflags at build time.
// See Makefile for usage.
var (
	version = ""
	commit  = ""
	date    = ""
)

var cli struct {
	Migrate migration.Migration `cmd:"" help:"Run an online schema change on a table."`
	Move    move.Move           `cmd:"" help:"Move tables between MySQL servers."`
	Lint    lint.LintCmd        `cmd:"" help:"Lint an entire MySQL schema."`
	Diff    lint.DiffCmd        `cmd:"" help:"Diff two MySQL schemas and lint the changes."`
}

func main() {
	buildinfo.Set(version, commit, date)
	ctx := kong.Parse(&cli,
		kong.Name("spirit"),
		kong.Description("Spirit: MySQL schema and data operations"),
		kong.UsageOnError(),
	)
	ctx.FatalIfErrorf(ctx.Run())
}
