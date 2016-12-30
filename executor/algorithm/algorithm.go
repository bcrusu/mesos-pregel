package algorithm

import (
	"github.com/bcrusu/pregel/encoding"
	"github.com/gogo/protobuf/proto"
)

type Algorithm interface {
	Compute(context *VertexContext, messages []proto.Message) error
	Combine(first proto.Message, second proto.Message) (proto.Message, error)

	VertexMessageEncoder() encoding.Encoder
	VertexValueEncoder() encoding.Encoder
	EdgeValueEncoder() encoding.Encoder
}

type Operations interface {
	AddVertex(id string, value proto.Message)
	RemoveVertex(id string)
	SetVertexValue(id string, value proto.Message)
	SendVertexMessage(to string, message proto.Message)

	VoteToHalt()

	AddEdge(from string, to string, value proto.Message)
	RemoveEdge(from string, to string)
	SetEdgeValue(from string, to string, value proto.Message)
}

type VertexContext struct {
	id        string
	Edges     []*EdgeContext
	Value     proto.Message
	Superstep int
	op        Operations
}

func NewVertexContext(id string, superstep int, value proto.Message, operations Operations) *VertexContext {
	result := &VertexContext{id: id, Superstep: superstep, Value: value, op: operations}
	//TODO
	return result
}

func (c *VertexContext) ID() string {
	return c.id
}

func (c *VertexContext) AddVertex(id string, value proto.Message) {
	c.op.AddVertex(id, value)
}

func (c *VertexContext) RemoveVertex(id string) {
	c.op.RemoveVertex(id)
}

func (c *VertexContext) Remove() {
	c.op.RemoveVertex(c.id)
}

func (c *VertexContext) SetVertexValue(id string, value proto.Message) {
	c.op.SetVertexValue(id, value)
}

func (c *VertexContext) SetValue(value proto.Message) {
	c.op.SetVertexValue(c.id, value)
}

func (c *VertexContext) SendMessageTo(to string, message proto.Message) {
	c.op.SendVertexMessage(to, message)
}

func (c *VertexContext) VoteToHalt() {
	c.op.VoteToHalt()
}

func (c *VertexContext) AddEdge(from string, to string, value proto.Message) {
	c.op.AddEdge(from, to, value)
}

func (c *VertexContext) RemoveEdge(from string, to string) {
	c.op.RemoveEdge(from, to)
}

func (c *VertexContext) SetEdgeValue(from string, to string, value proto.Message) {
	c.op.SetEdgeValue(from, to, value)
}

type EdgeContext struct {
	from  *VertexContext
	To    string
	Value proto.Message
}

func NewEdgeContext(from *VertexContext, to string, value proto.Message) *EdgeContext {
	return &EdgeContext{from: from, To: to, Value: value}
}

func (c *EdgeContext) SetValue(value proto.Message) {
	c.from.op.SetEdgeValue(c.from.id, c.To, value)
}

func (c *EdgeContext) Remove() {
	c.from.op.RemoveEdge(c.from.id, c.To)
}
