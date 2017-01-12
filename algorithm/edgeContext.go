package algorithm

type EdgeContext struct {
	from         *VertexContext
	To           string
	Value        interface{}
	MutableValue interface{}
}

func NewEdgeContext(from *VertexContext, to string, value interface{}, mutableValue interface{}) *EdgeContext {
	return &EdgeContext{from: from, To: to, Value: value, MutableValue: mutableValue}
}

func (c *EdgeContext) SetValue(value interface{}) {
	c.from.op.SetEdgeValue(c.from.id, c.To, value)
}

func (c *EdgeContext) Remove() {
	c.from.op.RemoveEdge(c.from.id, c.To)
}
