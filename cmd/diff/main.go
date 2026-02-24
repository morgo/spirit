package main

import (
	"github.com/alecthomas/kong"
	"github.com/block/spirit/pkg/lint"
)

var cli struct {
	lint.DiffCmd `cmd:"" help:"Diff two MySQL schemas and lint the changes."`
}

func main() {
	ctx := kong.Parse(&cli)
	ctx.FatalIfErrorf(ctx.Run())
}
