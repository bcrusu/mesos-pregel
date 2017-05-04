package algorithm

import (
	"github.com/bcrusu/mesos-pregel/aggregator"
)

type VertexContext struct {
	id                       string
	Edges                    []*EdgeContext
	Value                    interface{}
	MutableValue             interface{}
	Superstep                int
	PrevSuperstepAggregators *aggregator.ImmutableAggregatorSet
	op                       ContextOperations
}

func NewVertexContext(id string, superstep int, value interface{}, mutableValue interface{},
	operations ContextOperations, prevSuperstepAggregators *aggregator.ImmutableAggregatorSet) *VertexContext {
	return &VertexContext{
		id:                       id,
		Superstep:                superstep,
		Value:                    value,
		MutableValue:             mutableValue,
		PrevSuperstepAggregators: prevSuperstepAggregators,
		op: operations,
	}
}

func (c *VertexContext) ID() string {
	return c.id
}

func (c *VertexContext) AddVertex(id string, value interface{}) {
	c.op.AddVertex(id, value)
}

func (c *VertexContext) RemoveVertex(id string) {
	c.op.RemoveVertex(id)
}

func (c *VertexContext) Remove() {
	c.op.RemoveVertex(c.id)
}

func (c *VertexContext) SetVertexValue(id string, value interface{}) {
	c.op.SetVertexValue(id, value)
}

func (c *VertexContext) SetValue(value interface{}) {
	c.op.SetVertexValue(c.id, value)
}

func (c *VertexContext) SendMessageTo(to string, message interface{}) {
	c.op.SendVertexMessage(to, message)
}

func (c *VertexContext) VoteToHalt() {
	c.op.VoteToHalt(c.id)
}

func (c *VertexContext) AddEdge(from string, to string, value interface{}) {
	c.op.AddEdge(from, to, value)
}

func (c *VertexContext) RemoveEdge(from string, to string) {
	c.op.RemoveEdge(from, to)
}

func (c *VertexContext) SetEdgeValue(from string, to string, value interface{}) {
	c.op.SetEdgeValue(from, to, value)
}

func (c *VertexContext) SetAggregator(id string, aggType string, value interface{}) error {
	return c.op.SetAggregator(id, aggType, value)
}

func (c *VertexContext) RemoveAggregator(id string) {
	c.op.RemoveAggregator(id)
}
