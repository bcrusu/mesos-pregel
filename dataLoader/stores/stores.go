package stores

import "github.com/bcrusu/pregel"

type Store interface {
	Write(edges []*pregel.Edge) error
	Connect() error
	Close()
}
