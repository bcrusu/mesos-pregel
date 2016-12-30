package main

import (
	"sync"

	"github.com/bcrusu/pregel"
	"github.com/bcrusu/pregel/executor/algorithm"
	"github.com/bcrusu/pregel/executor/algorithmImpl"
	"github.com/bcrusu/pregel/executor/stores"
	"github.com/bcrusu/pregel/protos"
	"github.com/pkg/errors"
)

type PregelTask struct {
	params           protos.PregelTaskParams
	store            stores.Store
	algorithm        algorithm.Algorithm
	mutex            sync.Mutex
	currentSuperstep int

	vertices verticesMap
	edges    edgesMap
}

type edge struct {
	from string
	to   string
}

type verticesMap map[string]interface{}
type edgesMap map[edge]interface{}

func NewPregelTask(params protos.PregelTaskParams) (*PregelTask, error) {
	store, err := stores.NewStore(params.StoreType, params.StoreParams, params.EntityRange)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize store: %v", params.StoreType)
	}

	algorithm, err := algorithmImpl.NewAlgorithm(params.AlgorithmType, params.AlgorithmParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize algorithm: %v", params.AlgorithmType)
	}

	return &PregelTask{params: params, store: store, algorithm: algorithm}, nil
}

func (task *PregelTask) NextSuperstep() error {
	task.mutex.Lock()
	defer task.mutex.Unlock()

	//TODO
	return nil
}

func (task *PregelTask) loadEntities() error {
	vertices, err := task.store.LoadVertices()
	if err != nil {
		return errors.Wrap(err, "failed to load graph vertices")
	}

	v := make(map[string]interface{}, len(vertices))
	for _, vertex := range vertices {
		value, err := task.algorithm.VertexValueEncoder().Unmarshal(vertex.Value)
		if err != nil {
			return errors.Wrapf(err, "unmarshal failed - vertex: %s", vertex.ID)
		}

		v[vertex.ID] = value
	}

	edges, err := task.store.LoadEdges()
	if err != nil {
		return errors.Wrap(err, "failed to load graph edges")
	}

	e := make(map[edge]interface{}, len(edges))
	for _, edgeEntity := range edges {
		key := edge{edgeEntity.From, edgeEntity.To}
		value, err := task.algorithm.VertexValueEncoder().Unmarshal(edgeEntity.Value)
		if err != nil {
			return errors.Wrapf(err, "unmarshal failed - edge: %+v", key)
		}

		//TODO: handle missing vertices

		e[key] = value
	}

	task.vertices = v
	task.edges = e
	return nil
}

func (task *PregelTask) fastForwardToSuperstep(toSuperstep int) (verticesMap, edgesMap, error) {
	// shallow-copy the original vertices & edges before applying the diffs
	v := make(map[string]interface{}, len(task.vertices))
	for key, value := range task.vertices {
		v[key] = value
	}

	e := make(map[edge]interface{}, len(task.edges))
	for key, value := range task.edges {
		e[key] = value
	}

	// apply the superstep diffs
	for superstep := task.currentSuperstep + 1; superstep <= toSuperstep; superstep++ {
		vertexOps, err := task.store.LoadVertexOperations(task.params.JobId, superstep)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to load vertex operations for superstep %d", superstep)
		}

		if err := applyVertexOperations(v, vertexOps, task.algorithm); err != nil {
			return nil, nil, err
		}

		edgeOps, err := task.store.LoadEdgeOperations(task.params.JobId, superstep)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to load edge operations for superstep %d", superstep)
		}

		if err := applyEdgeOperations(e, edgeOps, task.algorithm); err != nil {
			return nil, nil, err
		}
	}

	return v, e, nil
}

func applyVertexOperations(vertices verticesMap, operations []*pregel.VertexOperation, algorithm algorithm.Algorithm) error {
	for _, operation := range operations {
		vertexID := operation.ID

		switch operation.Type {
		case pregel.VertexAdded, pregel.VertexValueChanged:
			value, err := algorithm.VertexValueEncoder().Unmarshal(operation.Value)
			if err != nil {
				return errors.Wrapf(err, "unmarshal failed - vertex: %s", vertexID)
			}

			if value1, contains := vertices[vertexID]; contains {
				if value, err = algorithm.Handlers().HandleDuplicateVertexValue(vertexID, value1, value); err != nil {
					return err
				}
			}

			vertices[vertexID] = value
		case pregel.VertexRemoved:
			if _, contains := vertices[vertexID]; !contains {
				if err := algorithm.Handlers().HandleMissingVertex(vertexID); err != nil {
					return err
				}
			} else {
				delete(vertices, vertexID)
			}
		default:
			return errors.Errorf("invalid vertex operation type %v", operation.Type)
		}
	}

	return nil
}

func applyEdgeOperations(edges edgesMap, operations []*pregel.EdgeOperation, algorithm algorithm.Algorithm) error {
	for _, operation := range operations {
		key := edge{operation.From, operation.To}

		switch operation.Type {
		case pregel.EdgeAdded, pregel.EdgeValueChanged:
			value, err := algorithm.EdgeValueEncoder().Unmarshal(operation.Value)
			if err != nil {
				return errors.Wrapf(err, "unmarshal failed - edge: %+v", key)
			}

			if value1, contains := edges[key]; contains {
				if value, err = algorithm.Handlers().HandleDuplicateEdgeValue(key.from, key.to, value1, value); err != nil {
					return err
				}
			}

			edges[key] = value
		case pregel.EdgeRemoved:
			if _, contains := edges[key]; !contains {
				if err := algorithm.Handlers().HandleMissingEdge(key.from, key.to); err != nil {
					return err
				}
			} else {
				delete(edges, key)
			}
		default:
			return errors.Errorf("invalid edge operation type %v", operation.Type)
		}
	}

	return nil
}
