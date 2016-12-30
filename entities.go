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
	VertexAdded           VertexOperationType = 0
	VertexRemoved         VertexOperationType = 1
	VertexValueChanged    VertexOperationType = 2
	VertexMessageReceived VertexOperationType = 3
)

const (
	EdgeAdded        EdgeOperationType = 0
	EdgeRemoved      EdgeOperationType = 1
	EdgeValueChanged EdgeOperationType = 2
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
