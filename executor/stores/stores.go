package stores

import "github.com/bcrusu/pregel/common/graph"

type Store interface {
	LoadEdges() []*graph.Edge

	Connect() error
	Close()
}
