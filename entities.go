package pregel

type Vertex struct {
	ID    string
	Value []byte
}

type Edge struct {
	From   string
	To     string
	Weight int //TODO: rename to 'Value' & change type
}

type VertexOperationType int
type EdgeOperationType int

const (
	_ VertexOperationType = iota
	VertexAdded
	VertexRemoved
	VertexValueChanged
	VertexMessageReceived
	VertexHalted
)

const (
	_ EdgeOperationType = iota
	EdgeAdded
	EdgeRemoved
	EdgeValueChanged
)

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
