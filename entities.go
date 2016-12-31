package pregel

type Vertex struct {
	ID    string
	Value []byte
}

type Edge struct {
	From  string
	To    string
	Value []byte
}

type VertexOperationType int
type EdgeOperationType int

const (
	_ VertexOperationType = iota
	VertexAdded
	VertexRemoved
	VertexValueChanged
)

const (
	_ EdgeOperationType = iota
	EdgeAdded
	EdgeRemoved
	EdgeValueChanged
)

type VertexMessage struct {
	From      string
	To        string
	JobID     string
	Superstep int
	Value     []byte
}

type VertexHalted struct {
	ID        string
	JobID     string
	Superstep int
}

type VertexOperation struct {
	ID          string
	JobID       string
	Superstep   int
	PerformedBy string
	Type        VertexOperationType
	Value       []byte
}

type EdgeOperation struct {
	From        string
	To          string
	JobID       string
	Superstep   int
	PerformedBy string
	Type        EdgeOperationType
	Value       []byte
}
