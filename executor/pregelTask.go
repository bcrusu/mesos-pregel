package main

import (
	"sync"

	"github.com/bcrusu/mesos-pregel/executor/algorithm"
	"github.com/bcrusu/mesos-pregel/executor/algorithmImpl"
	"github.com/bcrusu/mesos-pregel/executor/graph"
	"github.com/bcrusu/mesos-pregel/executor/messagesProcessor"
	"github.com/bcrusu/mesos-pregel/executor/store"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/pkg/errors"
)

type PregelTask struct {
	jobID     string
	store     store.Store
	algorithm algorithm.Algorithm
	mutex     sync.Mutex

	currentSuperstep int
	graph            *graph.Graph
}

func NewPregelTask(params protos.PregelTaskParams) (*PregelTask, error) {
	store, err := store.NewStore(params.Store, params.StoreParams, params.EntityRange)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize store: %v", params.Store)
	}

	algorithm, err := algorithmImpl.NewAlgorithm(params.Algorithm, params.AlgorithmParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize algorithm: %v", params.Algorithm)
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

	processor := messagesProcessor.New(task.jobID, superstep, task.graph, task.algorithm)
	processResult, err := processor.Process(messages, halted)
	if err != nil {
		return err
	}

	err = task.saveResult(processResult)
	if err != nil {
		return err
	}

	return nil
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

		edgeOps, err := task.store.LoadEdgeOperations(task.jobID, superstep)
		if err != nil {
			return errors.Wrapf(err, "failed to load edge operations for superstep %d", superstep)
		}

		if err = applyGraphOperations(graph, vertexOps, edgeOps, task.algorithm); err != nil {
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

func (task *PregelTask) saveResult(result *messagesProcessor.ProcessResult) error {
	//TODO
	return nil
}
