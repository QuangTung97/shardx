// +build tools

package tools

import (
	_ "github.com/fzipp/gocyclo"
	_ "github.com/kisielk/errcheck"
	_ "github.com/matryer/moq"
	_ "golang.org/x/lint/golint"
)
