package pregel

type Vertex struct {
	Id    string
	Value []byte
}

type Edge struct {
	From   string
	To     string
	Weight int
}

type Message interface {
	Combine(other Message) Message
	Serialize() []byte
}
