package parsers

import "github.com/bcrusu/pregel/data/graph"

type Parser interface {
	Next() *graph.Edge
}
