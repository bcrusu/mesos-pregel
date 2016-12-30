package main

import (
	"github.com/bcrusu/pregel/executor/algorithm"
	"github.com/gogo/protobuf/proto"
)

type pregelOperations struct {
	performedByVertexID string
}

func NewPregelOperations(performedByVertexID string) *algorithm.Operations {
	return &pregelOperations{performedByVertexID}
}

func (op *pregelOperations) AddVertex(id string, value proto.Message) {

}

func (op *pregelOperations) RemoveVertex(id string) {

}

func (op *pregelOperations) SetVertexValue(id string, value proto.Message) {

}

func (op *pregelOperations) SendVertexMessage(to string, message proto.Message) {

}

func (op *pregelOperations) VoteToHalt(id string) {

}

func (op *pregelOperations) AddEdge(from string, to string, value proto.Message) {

}

func (op *pregelOperations) RemoveEdge(from string, to string) {

}

func (op *pregelOperations) SetEdgeValue(from string, to string, value proto.Message) {

}
