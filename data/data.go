package data

type Edge struct {
	FromNode string
	ToNode   string
	Weight   int
}

func Run() {

}

type Parser interface {
	Next() (edge *Edge, success bool)
}

type Store interface {
	Write(edge *Edge)
	Connect() error
	Close()
}
