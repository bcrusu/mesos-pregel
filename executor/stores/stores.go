package stores

import "github.com/bcrusu/pregel"

type Store interface {
	LoadEdges() []*pregel.Edge
	SaveMessage() error
	LoadMessages() []pregel.Message

	Connect() error
	Close()
}
