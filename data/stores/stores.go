package stores

import "github.com/bcrusu/pregel/data/graph"

type Store interface {
	Write(edge *graph.Edge)
	Connect() error
	Close()
}
