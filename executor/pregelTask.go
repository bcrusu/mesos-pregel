package main

import (
	"sync"

	"github.com/bcrusu/pregel"
	"github.com/bcrusu/pregel/executor/algorithm"
	"github.com/bcrusu/pregel/executor/algorithmImpl"
	"github.com/bcrusu/pregel/executor/graph"
	"github.com/bcrusu/pregel/executor/stores"
	"github.com/bcrusu/pregel/protos"
	"github.com/pkg/errors"
)

type PregelTask struct {
	jobID     string
	store     stores.Store
	algorithm algorithm.Algorithm
	mutex     sync.Mutex

	currentSuperstep int
	graph            *graph.Graph
}

func NewPregelTask(params protos.PregelTaskParams) (*PregelTask, error) {
	store, err := stores.NewStore(params.StoreType, params.StoreParams, params.EntityRange)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize store: %v", params.StoreType)
	}

	algorithm, err := algorithmImpl.NewAlgorithm(params.AlgorithmType, params.AlgorithmParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize algorithm: %v", params.AlgorithmType)
	}

	return &PregelTask{jobID: params.JobId, store: store, algorithm: algorithm}, nil
}

func (task *PregelTask) ExecSuperstep(superstep int) error {
	task.mutex.Lock()
	defer task.mutex.Unlock()

	if task.currentSuperstep == superstep {
		return nil
	}

	prevSuperstep := superstep - 1

	if err := task.loadSuperstep(prevSuperstep); err != nil {
		return err
	}

	messages, err := task.loadVertexMessages(prevSuperstep)
	if err != nil {
		return err
	}

	halted, err := task.loadHaltedVertices(prevSuperstep)
	if err != nil {
		return err
	}

	task.processMessages(messages, halted)

	//TODO
	return nil
}

func (task *PregelTask) processMessages(messages map[string]interface{}, halted map[string]bool) {
	operations := NewPregelOperations(task.algorithm)

	graph := task.graph
	for _, id := range graph.Vertices() {

		vertexValue, _ := graph.VertexValue(id)
		vertexContext := algorithm.NewVertexContext(id, superstep, vertexValue, operations)

		// if edges, contains := task.edges[id]; contains {

		// }
	}
}

func (task *PregelTask) loadSuperstep(superstep int) error {
	if superstep < task.currentSuperstep {
		return errors.Errorf("cannot load past superstep - current superstep: %d; asked to load %d", task.currentSuperstep, superstep)
	}

	if task.graph == nil {
		if err := task.loadGraph(); err != nil {
			return err
		}
	}

	if task.currentSuperstep < superstep {
		if err := task.fastForwardToSuperstep(superstep); err != nil {
			return err
		}
	}

	return nil
}

func (task *PregelTask) loadGraph() error {
	// load vertices
	vertices, err := task.store.LoadVertices()
	if err != nil {
		return errors.Wrap(err, "failed to load graph vertices")
	}

	graph := graph.NewGraph(len(vertices))

	for _, vertex := range vertices {
		value, err := task.algorithm.VertexValueEncoder().Unmarshal(vertex.Value)
		if err != nil {
			return errors.Wrapf(err, "unmarshal failed - vertex: %s", vertex.ID)
		}

		graph.SetVertexValue(vertex.ID, value)
	}

	// load edges
	edges, err := task.store.LoadEdges()
	if err != nil {
		return errors.Wrap(err, "failed to load graph edges")
	}

	for _, edge := range edges {
		value, err := task.algorithm.VertexValueEncoder().Unmarshal(edge.Value)
		if err != nil {
			return errors.Wrapf(err, "unmarshal failed - edge: from=%s to=%s", edge.From, edge.To)
		}

		graph.SetEdgeValue(edge.From, edge.To, value)
	}

	task.graph = graph
	return nil
}

func (task *PregelTask) fastForwardToSuperstep(toSuperstep int) error {
	graph := task.graph.Clone()

	// apply the superstep diffs
	for superstep := task.currentSuperstep + 1; superstep <= toSuperstep; superstep++ {
		vertexOps, err := task.store.LoadVertexOperations(task.jobID, superstep)
		if err != nil {
			return errors.Wrapf(err, "failed to load vertex operations for superstep %d", superstep)
		}

		if err = applyVertexOperations(graph, vertexOps, task.algorithm); err != nil {
			return err
		}

		edgeOps, err := task.store.LoadEdgeOperations(task.jobID, superstep)
		if err != nil {
			return errors.Wrapf(err, "failed to load edge operations for superstep %d", superstep)
		}

		if err := applyEdgeOperations(graph, edgeOps, task.algorithm); err != nil {
			return err
		}
	}

	task.graph = graph
	task.currentSuperstep = toSuperstep
	return nil
}

func (task *PregelTask) loadVertexMessages(superstep int) (map[string]interface{}, error) {
	messages, err := task.store.LoadVertexMessages(task.jobID, superstep)
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{}, len(messages))
	for _, message := range messages {
		value, err := task.algorithm.VertexMessageEncoder().Unmarshal(message.Value)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal failed - vertex message to %s", message.To)
		}

		if value1, contains := result[message.To]; contains {
			value = task.algorithm.VertexMessageCombiner()(value1, value)
		}

		result[message.To] = value
	}

	return result, nil
}

func (task *PregelTask) loadHaltedVertices(superstep int) (map[string]bool, error) {
	halted, err := task.store.LoadHaltedVertices(task.jobID, superstep)
	if err != nil {
		return nil, err
	}

	result := make(map[string]bool, len(halted))
	for _, id := range halted {
		result[id] = true
	}

	return result, nil
}

func applyVertexOperations(graph *graph.Graph, operations []*pregel.VertexOperation, algorithm algorithm.Algorithm) error {
	for _, operation := range operations {
		vertexID := operation.ID

		switch operation.Type {
		case pregel.VertexAdded, pregel.VertexValueChanged:
			value, err := algorithm.VertexValueEncoder().Unmarshal(operation.Value)
			if err != nil {
				return errors.Wrapf(err, "unmarshal failed - vertex: %s", vertexID)
			}

			if prevValue, ok := graph.VertexValue(vertexID); ok {
				if value, err = algorithm.Handlers().HandleDuplicateVertexValue(vertexID, prevValue, value); err != nil {
					return err
				}
			}

			graph.SetVertexValue(vertexID, value)
		case pregel.VertexRemoved:
			if _, ok := graph.VertexValue(vertexID); !ok {
				if err := algorithm.Handlers().HandleMissingVertex(vertexID); err != nil {
					return err
				}
			} else {
				graph.RemoveVertex(vertexID)
			}
		default:
			return errors.Errorf("invalid vertex operation type %v", operation.Type)
		}
	}

	return nil
}

func applyEdgeOperations(graph *graph.Graph, operations []*pregel.EdgeOperation, algorithm algorithm.Algorithm) error {
	for _, operation := range operations {
		from := operation.From
		to := operation.To

		switch operation.Type {
		case pregel.EdgeAdded, pregel.EdgeValueChanged:
			value, err := algorithm.EdgeValueEncoder().Unmarshal(operation.Value)
			if err != nil {
				return errors.Wrapf(err, "unmarshal failed - edge from %s to %s", from, to)
			}

			if prevValue, ok := graph.EdgeValue(from, to); ok {
				if value, err = algorithm.Handlers().HandleDuplicateEdgeValue(from, to, prevValue, value); err != nil {
					return err
				}
			}

			graph.SetEdgeValue(from, to, value)
		case pregel.EdgeRemoved:
			if _, ok := graph.EdgeValue(from, to); !ok {
				if err := algorithm.Handlers().HandleMissingEdge(from, to); err != nil {
					return err
				}
			} else {
				graph.RemoveEdge(from, to)
			}
		default:
			return errors.Errorf("invalid edge operation type %v", operation.Type)
		}
	}

	return nil
}
