package data

import "github.com/bcrusu/pregel/data"

type Parser interface {
	Next() (edge *data.Edge, success bool)
}

type Store interface {
	Write(edge *data.Edge)
	Connect() error
	Close()
}

type Edge struct {
	FromNode string
	ToNode   string
	Weight   int
}

func Run() {

}
