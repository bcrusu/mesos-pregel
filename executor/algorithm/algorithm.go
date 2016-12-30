package algorithm

import "github.com/bcrusu/pregel/encoding"

type Algorithm interface {
	Compute(context *VertexContext, message interface{}) error
	VertexMessageCombiner() VertexMessageCombiner

	VertexMessageEncoder() encoding.Encoder
	VertexValueEncoder() encoding.Encoder
	EdgeValueEncoder() encoding.Encoder

	Handlers() Handlers
}

type VertexMessageCombiner func(first interface{}, second interface{}) interface{}

type Operations interface {
	AddVertex(id string, value interface{})
	RemoveVertex(id string)
	SetVertexValue(id string, value interface{})
	SendVertexMessage(to string, message interface{})

	VoteToHalt()

	AddEdge(from string, to string, value interface{})
	RemoveEdge(from string, to string)
	SetEdgeValue(from string, to string, value interface{})
}
