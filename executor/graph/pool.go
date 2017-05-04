package graph

import (
	"sync"

	"github.com/bcrusu/mesos-pregel/algorithm"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/pkg/errors"
)

type Pool struct {
	jobID          string
	store          store.GraphStore
	storeConnected bool
	algorithm      algorithm.Algorithm
	mutex          sync.Mutex
	graphs         map[string]*Graph
}

func NewPool(params *protos.ExecTaskParams) (*Pool, error) {
	store, err := store.New(params.Store, params.StoreParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize store: %v", params.Store)
	}

	algorithm, err := algorithm.New(params.Algorithm, params.AlgorithmParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize algorithm: %v", params.Algorithm)
	}

	return &Pool{
		jobID:     params.JobId,
		store:     store,
		algorithm: algorithm,
		graphs:    make(map[string]*Graph),
	}, nil
}

func (p *Pool) Get(superstep int, vrange store.VertexRange) (*Graph, error) {
	key := getKey(vrange)
	var graph *Graph

	p.mutex.Lock()
	if graph, ok := p.graphs[key]; ok && graph.superstep <= superstep {
		delete(p.graphs, key)
	}
	p.mutex.Unlock()

	if err := p.connectGraphStore(); err != nil {
		return nil, err
	}

	if graph == nil {
		var err error
		if graph, err = p.loadGraph(vrange); err != nil {
			return nil, err
		}
		graph.key = key
	}

	if err := p.fastForwardToSuperstep(graph, superstep, vrange); err != nil {
		return nil, err
	}

	return graph, nil
}

func (p *Pool) Release(graph *Graph) {
	p.mutex.Lock()
	p.graphs[graph.key] = graph
	p.mutex.Unlock()
}

func (p *Pool) Close() {
	if !p.storeConnected {
		return
	}

	p.store.Close()
	p.storeConnected = false
}

func (p *Pool) loadGraph(vrange store.VertexRange) (*Graph, error) {
	// load vertices
	vertices, err := p.store.LoadVertices(vrange)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load graph vertices")
	}

	graph := NewGraph(len(vertices))

	for _, vertex := range vertices {
		value, err := p.algorithm.VertexValueEncoder().Unmarshal(vertex.Value)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal failed - vertex: %s", vertex.ID)
		}

		graph.setVertexValue(vertex.ID, value)
	}

	// load edges
	edges, err := p.store.LoadEdges(vrange)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load graph edges")
	}

	for _, edge := range edges {
		value, err := p.algorithm.EdgeValueEncoder().Unmarshal(edge.Value)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal failed - edge: from=%s to=%s", edge.From, edge.To)
		}

		graph.setEdgeValue(edge.From, edge.To, value)
	}

	return graph, nil
}

func (p *Pool) fastForwardToSuperstep(graph *Graph, to int, vrange store.VertexRange) error {
	// apply the superstep diffs
	for superstep := graph.superstep + 1; superstep <= to; superstep++ {
		vertexOps, err := p.store.LoadVertexOperations(p.jobID, superstep, vrange)
		if err != nil {
			return errors.Wrapf(err, "failed to load vertex operations for superstep %d", superstep)
		}

		edgeOps, err := p.store.LoadEdgeOperations(p.jobID, superstep, vrange)
		if err != nil {
			return errors.Wrapf(err, "failed to load edge operations for superstep %d", superstep)
		}

		if err = applyGraphOperations(graph, vertexOps, edgeOps, p.algorithm); err != nil {
			return err
		}

		graph.superstep = superstep
	}

	return nil
}

func (p *Pool) connectGraphStore() error {
	if p.storeConnected {
		return nil
	}

	err := p.store.Connect()
	if err != nil {
		return err
	}

	p.storeConnected = true
	return nil
}

func getKey(vrange store.VertexRange) string {
	return string(vrange)
}
