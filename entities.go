package pregel

type Job struct {
	ID              string
	Status          JobStatus
	Superstep       int
	Store           string
	StoreParams     []byte
	Algorithm       string
	AlgorithmParams []byte
	VerticesPerTask int
	//TODO: creation Date
}

type Vertex struct {
	ID    string
	Value []byte
}

type Edge struct {
	From  string
	To    string
	Value []byte
}

type JobStatus int
type VertexOperationType int
type EdgeOperationType int

const (
	_ JobStatus = iota
	JobRunning
	JobCompleted
	JobFailed
)

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
	ID        string
	JobID     string
	Superstep int
	Type      VertexOperationType
	Value     []byte
}

type EdgeOperation struct {
	From      string
	To        string
	JobID     string
	Superstep int
	Type      EdgeOperationType
	Value     []byte
}
