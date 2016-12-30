package main

import (
	"sync"

	"github.com/bcrusu/pregel"
	"github.com/bcrusu/pregel/executor/algorithm"
	"github.com/pkg/errors"
)

type PregelOperations struct {
	performedBy string
	algorithm   algorithm.Algorithm

	addedVertices            map[string]interface{}
	addedVerticesMutex       sync.Mutex
	removedVertices          map[string]bool
	removedVerticesMutex     sync.Mutex
	changedVertexValues      map[string]interface{}
	changedVertexValuesMutex sync.Mutex
	vertexMessages           map[string]interface{}
	vertexMessagesMutex      sync.Mutex
	haltedVertices           map[string]bool
	haltedVerticesMutex      sync.Mutex
	addedEdges               map[edge]interface{}
	addedEdgesMutex          sync.Mutex
	removedEdges             map[edge]bool
	removedEdgesMutex        sync.Mutex
	changedEdgeValues        map[edge]interface{}
	changedEdgeValuesMutex   sync.Mutex
}

func NewPregelOperations(performedBy string, algorithm algorithm.Algorithm) *PregelOperations {
	result := &PregelOperations{performedBy: performedBy, algorithm: algorithm}
	result.addedVertices = make(map[string]interface{})
	result.removedVertices = make(map[string]bool)
	result.changedVertexValues = make(map[string]interface{})
	result.vertexMessages = make(map[string]interface{})
	result.haltedVertices = make(map[string]bool)
	result.addedEdges = make(map[edge]interface{})
	result.removedEdges = make(map[edge]bool)
	result.changedEdgeValues = make(map[edge]interface{})
	return result
}

func (op *PregelOperations) AddVertex(id string, value interface{}) {
	op.addedVerticesMutex.Lock()
	defer op.addedVerticesMutex.Unlock()

	if value1, contains := op.addedVertices[id]; contains {
		var err error
		if value, err = op.algorithm.Handlers().HandleDuplicateVertexValue(id, value1, value); err != nil {
			//TODO:
		}
	}

	op.addedVertices[id] = value
}

func (op *PregelOperations) RemoveVertex(id string) {
	op.removedVerticesMutex.Lock()
	defer op.removedVerticesMutex.Unlock()
	op.removedVertices[id] = true
}

func (op *PregelOperations) SetVertexValue(id string, value interface{}) {
	op.changedVertexValuesMutex.Lock()
	defer op.changedVertexValuesMutex.Unlock()

	if value1, contains := op.changedVertexValues[id]; contains {
		var err error
		if value, err = op.algorithm.Handlers().HandleDuplicateVertexValue(id, value1, value); err != nil {
			//TODO:
		}
	}

	op.changedVertexValues[id] = value
}

func (op *PregelOperations) SendVertexMessage(to string, message interface{}) {
	op.vertexMessagesMutex.Lock()
	defer op.vertexMessagesMutex.Unlock()

	if message1, contains := op.vertexMessages[to]; contains {
		message = op.algorithm.VertexMessageCombiner()(message1, message)
	}

	op.vertexMessages[to] = message
}

func (op *PregelOperations) VoteToHalt() {
	op.haltedVerticesMutex.Lock()
	defer op.haltedVerticesMutex.Unlock()
	op.haltedVertices[op.performedBy] = true
}

func (op *PregelOperations) AddEdge(from string, to string, value interface{}) {
	op.addedEdgesMutex.Lock()
	defer op.addedEdgesMutex.Unlock()

	edge := edge{from, to}
	if value1, contains := op.addedEdges[edge]; contains {
		var err error
		if value, err = op.algorithm.Handlers().HandleDuplicateEdgeValue(from, to, value1, value); err != nil {
			//TODO:
		}
	}

	op.addedEdges[edge] = value
}

func (op *PregelOperations) RemoveEdge(from string, to string) {
	op.removedEdgesMutex.Lock()
	defer op.removedEdgesMutex.Unlock()
	op.removedEdges[edge{from, to}] = true
}

func (op *PregelOperations) SetEdgeValue(from string, to string, value interface{}) {
	op.changedEdgeValuesMutex.Lock()
	defer op.changedEdgeValuesMutex.Unlock()

	edge := edge{from, to}
	if value1, contains := op.changedEdgeValues[edge]; contains {
		var err error
		if value, err = op.algorithm.Handlers().HandleDuplicateEdgeValue(from, to, value1, value); err != nil {
			//TODO:
		}
	}

	op.changedEdgeValues[edge] = value
}

func (op *PregelOperations) GetEntities(jobId string, superstep int) ([]*pregel.VertexOperation, []*pregel.EdgeOperation, error) {
	v := make([]*pregel.VertexOperation, 0, op.getVertexOperationsCount())
	e := make([]*pregel.EdgeOperation, 0, op.getEdgeOperationsCount())

	for id, value := range op.addedVertices {
		bytes, err := op.algorithm.VertexValueEncoder().Marshal(value)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "marshal failed - addedVertices: %s", id)
		}
		v = append(v, &pregel.VertexOperation{ID: id, JobID: jobId, Superstep: superstep, PerformedBy: op.performedBy, Type: pregel.VertexAdded, Value: bytes})
	}

	for id := range op.removedVertices {
		v = append(v, &pregel.VertexOperation{ID: id, JobID: jobId, Superstep: superstep, PerformedBy: op.performedBy, Type: pregel.VertexRemoved, Value: nil})
	}

	for id, value := range op.changedVertexValues {
		bytes, err := op.algorithm.VertexValueEncoder().Marshal(value)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "marshal failed - changedVertexValues: %s", id)
		}
		v = append(v, &pregel.VertexOperation{ID: id, JobID: jobId, Superstep: superstep, PerformedBy: op.performedBy, Type: pregel.VertexValueChanged, Value: bytes})
	}

	for id, message := range op.vertexMessages {
		bytes, err := op.algorithm.VertexMessageEncoder().Marshal(message)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "marshal failed - vertexMessages: %s", id)
		}
		v = append(v, &pregel.VertexOperation{ID: id, JobID: jobId, Superstep: superstep, PerformedBy: op.performedBy, Type: pregel.VertexMessageReceived, Value: bytes})
	}

	for id := range op.haltedVertices {
		v = append(v, &pregel.VertexOperation{ID: id, JobID: jobId, Superstep: superstep, PerformedBy: op.performedBy, Type: pregel.VertexHalted, Value: nil})
	}

	for edge, value := range op.addedEdges {
		bytes, err := op.algorithm.EdgeValueEncoder().Marshal(value)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "marshal failed - addedEdges: %+v", edge)
		}
		e = append(e, &pregel.EdgeOperation{From: edge.from, To: edge.to, JobID: jobId, Superstep: superstep, PerformedBy: op.performedBy, Type: pregel.EdgeAdded, Value: bytes})
	}

	for edge := range op.removedEdges {
		e = append(e, &pregel.EdgeOperation{From: edge.from, To: edge.to, JobID: jobId, Superstep: superstep, PerformedBy: op.performedBy, Type: pregel.EdgeRemoved, Value: nil})
	}

	for edge, value := range op.changedEdgeValues {
		bytes, err := op.algorithm.EdgeValueEncoder().Marshal(value)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "marshal failed - changedEdgeValues: %+v", edge)
		}
		e = append(e, &pregel.EdgeOperation{From: edge.from, To: edge.to, JobID: jobId, Superstep: superstep, PerformedBy: op.performedBy, Type: pregel.EdgeValueChanged, Value: bytes})
	}

	return v, e, nil
}

func (op *PregelOperations) getVertexOperationsCount() int {
	result := len(op.addedVertices)
	result += len(op.removedVertices)
	result += len(op.changedVertexValues)
	result += len(op.vertexMessages)
	result += len(op.haltedVertices)
	return result
}

func (op *PregelOperations) getEdgeOperationsCount() int {
	result := len(op.addedEdges)
	result += len(op.removedEdges)
	result += len(op.changedEdgeValues)
	return result
}