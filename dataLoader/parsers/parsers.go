package parsers

import "github.com/bcrusu/pregel"

type Parser interface {
	Next() *pregel.Edge
}
