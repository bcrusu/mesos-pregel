package algorithm

type Handlers interface {
	HandleDuplicateVertexValue(id string, value1 interface{}, value2 interface{}) (finalValue interface{}, err error)
	HandleMissingVertex(id string) error

	HandleDuplicateEdgeValue(from string, to string, value1 interface{}, value2 interface{}) (finalValue interface{}, err error)
	HandleMissingEdge(from string, to string) error
}
