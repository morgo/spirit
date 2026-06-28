package migration

import (
	"fmt"

	"github.com/block/spirit/pkg/lint"
)

var (
	// defaultLinterSettings holds settings for linters where we know we want to override linter
	// system defaults or ensure specific behavior.
	defaultLinterSettings = map[string]map[string]string{
		"invisible_index_before_drop": {
			"raiseError": "false",
		},
	}
)

func printLinters(config lint.Config) error {
	fmt.Printf("Linting is enabled with the following linters:\n")
	linters := lint.List()
	for _, linterName := range linters {
		l, err := lint.Get(linterName)
		// Skip linters that are not enabled
		if !config.IsEnabled(linterName) {
			continue
		}

		if err != nil {
			return err
		}
		fmt.Printf(" + %s", l)
		if cfg, ok := config.Settings[linterName]; ok {
			fmt.Print(" (with config")
			for k, v := range cfg {
				fmt.Printf(" %s=%s", k, v)
			}
			fmt.Print(") ")
		}
		fmt.Println()
	}
	return nil
}
