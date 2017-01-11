package main

import (
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/algorithm"
	"github.com/bcrusu/mesos-pregel/executor/graph"
	"github.com/bcrusu/mesos-pregel/executor/messagesProcessor"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/pkg/errors"
)

type PregelTask struct {
	ID        int32
	jobID     string
	store     store.GraphStore
	algorithm algorithm.Algorithm
	mutex     sync.Mutex

	currentSuperstep int
	graph            *graph.Graph
}

func NewPregelTask(params protos.ExecTaskParams) (*PregelTask, error) {
	store, err := store.New(params.Store, params.StoreParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize store: %v", params.Store)
	}

	algorithm, err := algorithm.New(params.Algorithm, params.AlgorithmParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize algorithm: %v", params.Algorithm)
	}

	return &PregelTask{
		ID:        params.TaskId,
		jobID:     params.JobId,
		store:     store,
		algorithm: algorithm}, nil
}

func (task *PregelTask) ExecSuperstep(params *protos.ExecSuperstepParams) (*protos.ExecSuperstepResult, error) {
	task.mutex.Lock()
	defer task.mutex.Unlock()

	startTime := time.Now()
	superstep := int(params.Superstep)
	prevSuperstep := superstep - 1

	if task.currentSuperstep >= superstep {
		return nil, errors.Errorf("cannot execute past superstep - current superstep: %d; asked to execute %d", task.currentSuperstep, superstep)
	}

	aggregatorSet, err := aggregator.NewSetFromMessages(params.Aggregators)
	if err != nil {
		return nil, err
	}

	if err := task.store.Connect(); err != nil {
		return nil, err
	}
	defer task.store.Close()

	vranges := make([]store.VertexRange, len(params.VertexRanges))
	for i, vrange := range params.VertexRanges {
		vranges[i] = store.VertexRange(vrange)
	}

	if err := task.loadSuperstep(prevSuperstep, vranges); err != nil {
		return nil, err
	}

	messages, err := task.loadVertexMessages(prevSuperstep, vranges)
	if err != nil {
		return nil, err
	}

	halted, err := task.loadHaltedVertices(prevSuperstep, vranges)
	if err != nil {
		return nil, err
	}

	processor := messagesProcessor.New(task.jobID, superstep, task.graph, task.algorithm, aggregatorSet)
	processResult, err := processor.Process(messages, halted)
	if err != nil {
		return nil, err
	}

	err = task.saveEntities(processResult.Entities)
	if err != nil {
		return nil, err
	}

	aggregators, err := aggregator.ConvertSetToProto(aggregatorSet)
	if err != nil {
		return nil, err
	}

	elapsed := time.Since(startTime)

	return &protos.ExecSuperstepResult{
		Aggregators: aggregators,
		Stats: &protos.ExecSuperstepResult_Stats{
			TotalDuration:     int64(elapsed),
			ComputedCount:     int32(processResult.Stats.ComputedCount),
			ComputeDuration:   int64(processResult.Stats.ComputeDuration),
			HaltedCount:       int32(processResult.Stats.HaltedCount),
			SentMessagesCount: int32(processResult.Stats.SentMessagesCount),
			InactiveCount:     int32(processResult.Stats.InactiveCount),
		},
	}, nil
}

func (task *PregelTask) loadSuperstep(superstep int, vranges []store.VertexRange) error {
	if superstep < task.currentSuperstep {
		return errors.Errorf("cannot load past superstep - current superstep: %d; asked to load %d", task.currentSuperstep, superstep)
	}

	if task.graph == nil {
		if err := task.loadGraph(vranges); err != nil {
			return err
		}
	}

	if task.currentSuperstep < superstep {
		if err := task.fastForwardToSuperstep(superstep, vranges); err != nil {
			return err
		}
	}

	return nil
}

func (task *PregelTask) loadGraph(vranges []store.VertexRange) error {
	// load vertices
	vertices, err := task.store.LoadVertices(vranges)
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
	edges, err := task.store.LoadEdges(vranges)
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

func (task *PregelTask) fastForwardToSuperstep(toSuperstep int, vranges []store.VertexRange) error {
	graph := task.graph.Clone()

	// apply the superstep diffs
	for superstep := task.currentSuperstep + 1; superstep <= toSuperstep; superstep++ {
		vertexOps, err := task.store.LoadVertexOperations(task.jobID, superstep, vranges)
		if err != nil {
			return errors.Wrapf(err, "failed to load vertex operations for superstep %d", superstep)
		}

		edgeOps, err := task.store.LoadEdgeOperations(task.jobID, superstep, vranges)
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

func (task *PregelTask) loadVertexMessages(superstep int, vranges []store.VertexRange) (map[string]interface{}, error) {
	messages, err := task.store.LoadVertexMessages(task.jobID, superstep, vranges)
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

func (task *PregelTask) loadHaltedVertices(superstep int, vranges []store.VertexRange) (map[string]bool, error) {
	halted, err := task.store.LoadHaltedVertices(task.jobID, superstep, vranges)
	if err != nil {
		return nil, err
	}

	result := make(map[string]bool, len(halted))
	for _, id := range halted {
		result[id] = true
	}

	return result, nil
}

func (task *PregelTask) saveEntities(entities *messagesProcessor.ProcessResultEntities) error {
	if err := task.store.SaveVertexMessages(entities.VertexMessages); err != nil {
		return err
	}

	if err := task.store.SaveHaltedVertices(entities.HaltedVertices); err != nil {
		return err
	}

	if err := task.store.SaveVertexOperations(entities.VertexOperations); err != nil {
		return err
	}

	if err := task.store.SaveEdgeOperations(entities.EdgeOperations); err != nil {
		return err
	}

	return nil
}
