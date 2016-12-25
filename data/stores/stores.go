package stores

import "github.com/bcrusu/pregel/data/graph"

type Store interface {
	Write(edges []*graph.Edge) error
	Connect() error
	Close()
}
