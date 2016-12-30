package algorithm

type Handlers interface {
	HandleDuplicateVertexValue(id string, value1 interface{}, value2 interface{}) (finalValue interface{}, err error)
	HandleDuplicateEdgeValue(from string, to string, value1 interface{}, value2 interface{}) (finalValue interface{}, err error)
}
