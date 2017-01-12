package graph

import (
	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/algorithm"
	"github.com/pkg/errors"
)

func applyGraphOperations(graph *Graph, vertexOps []*pregel.VertexOperation, edgeOps []*pregel.EdgeOperation, algorithm algorithm.Algorithm) error {
	// edge removals
	for _, operation := range edgeOps {
		if operation.Type != pregel.EdgeRemoved {
			continue
		}

		from := operation.From
		to := operation.To

		if ok := graph.HasEdge(from, to); !ok {
			if err := algorithm.Handlers().OnMissingEdge(from, to); err != nil {
				return err
			}
		} else {
			graph.removeEdge(from, to)
		}
	}

	// vertex removals
	for _, operation := range vertexOps {
		if operation.Type != pregel.VertexRemoved {
			continue
		}

		vertexID := operation.ID

		if ok := graph.HasVertex(vertexID); !ok {
			if err := algorithm.Handlers().OnMissingVertex(vertexID); err != nil {
				return err
			}
		} else {
			graph.removeVertex(vertexID)
		}
	}

	// vertex additions
	for _, operation := range vertexOps {
		if operation.Type != pregel.VertexAdded {
			continue
		}

		vertexID := operation.ID

		value, err := algorithm.VertexValueEncoder().Unmarshal(operation.Value)
		if err != nil {
			return errors.Wrapf(err, "unmarshal failed - vertex: %s", vertexID)
		}

		if prevValue, ok := graph.VertexValue(vertexID); ok {
			if value, err = algorithm.Handlers().OnDuplicateVertex(vertexID, prevValue, value); err != nil {
				return err
			}
		}

		graph.setVertexValue(vertexID, value)
	}

	// edge additions
	for _, operation := range edgeOps {
		if operation.Type != pregel.EdgeAdded {
			continue
		}

		from := operation.From
		to := operation.To

		value, err := algorithm.EdgeValueEncoder().Unmarshal(operation.Value)
		if err != nil {
			return errors.Wrapf(err, "unmarshal failed - edge from %s to %s", from, to)
		}

		if prevValue, ok := graph.EdgeValue(from, to); ok {
			if value, err = algorithm.Handlers().OnDuplicateEdge(from, to, prevValue, value); err != nil {
				return err
			}
		}

		graph.setEdgeValue(from, to, value)
	}

	// vertex value mutations
	for _, operation := range vertexOps {
		if operation.Type != pregel.VertexValueChanged {
			continue
		}

		vertexID := operation.ID

		value, err := algorithm.VertexValueEncoder().Unmarshal(operation.Value)
		if err != nil {
			return errors.Wrapf(err, "unmarshal failed - vertex: %s", vertexID)
		}

		if ok := graph.HasVertex(vertexID); !ok {
			if err = algorithm.Handlers().OnMissingVertex(vertexID); err != nil {
				return err
			}
		}

		graph.setVertexValue(vertexID, value)
	}

	// edge value mutations
	for _, operation := range edgeOps {
		if operation.Type != pregel.EdgeValueChanged {
			continue
		}

		from := operation.From
		to := operation.To

		value, err := algorithm.EdgeValueEncoder().Unmarshal(operation.Value)
		if err != nil {
			return errors.Wrapf(err, "unmarshal failed - edge from %s to %s", from, to)
		}

		if ok := graph.HasEdge(from, to); !ok {
			if err = algorithm.Handlers().OnMissingEdge(from, to); err != nil {
				return err
			}
		}

		graph.setEdgeValue(from, to, value)
	}

	return nil
}
