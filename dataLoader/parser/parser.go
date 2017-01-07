package parser

import (
	"fmt"
	"io"

	"github.com/bcrusu/mesos-pregel"
)

type VertexParser interface {
	Next() (*pregel.Vertex, error)
}

type EdgeParser interface {
	Next() (*pregel.Edge, error)
}

func NewVertexParser(name string, reader io.Reader) (VertexParser, error) {
	var parser VertexParser
	var err error

	switch name {
	case "dimacs":
		parser = NewDimacsVertexParser(reader)
	default:
		err = fmt.Errorf("Invalid parser '%s'", name)
	}

	return parser, err
}

func NewEdgeParser(name string, reader io.Reader) (EdgeParser, error) {
	var parser EdgeParser
	var err error

	switch name {
	case "dimacs":
		parser = NewDimacsEdgeParser(reader)
	default:
		err = fmt.Errorf("Invalid parser '%s'", name)
	}

	return parser, err
}
