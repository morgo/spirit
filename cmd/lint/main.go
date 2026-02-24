package main

import (
	"github.com/alecthomas/kong"
	"github.com/block/spirit/pkg/lint"
)

var cli struct {
	lint.LintCmd `cmd:"" help:"Lint an entire MySQL schema."`
}

func main() {
	ctx := kong.Parse(&cli)
	ctx.FatalIfErrorf(ctx.Run())
}
