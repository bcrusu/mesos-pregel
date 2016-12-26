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
