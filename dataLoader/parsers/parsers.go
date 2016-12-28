package parsers

import (
	"flag"
	"fmt"
	"io"

	"github.com/bcrusu/pregel"
)

type Parser interface {
	Next() *pregel.Edge
}

var (
	parserType string
)

func init() {
	flag.StringVar(&parserType, "parser", "dimacs", "Parser type. Available parsers: dimacs")
}

func NewParser(reader io.Reader) (Parser, error) {
	var parser Parser
	var err error

	switch parserType {
	case "dimacs":
		parser = NewDimacsParser(reader)
	default:
		err = fmt.Errorf("Invalid parser type '%s'", parserType)
	}

	return parser, err
}
