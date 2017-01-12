package main

import (
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
	jobID     string
	store     store.GraphStore
	algorithm algorithm.Algorithm
	graph     *graph.Graph
}

func NewPregelTask(params *protos.ExecTaskParams, graph *graph.Graph) (*PregelTask, error) {
	store, err := store.New(params.Store, params.StoreParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize store: %v", params.Store)
	}

	algorithm, err := algorithm.New(params.Algorithm, params.AlgorithmParams)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize algorithm: %v", params.Algorithm)
	}

	return &PregelTask{
		jobID:     params.JobId,
		store:     store,
		algorithm: algorithm,
		graph:     graph,
	}, nil
}

func (task *PregelTask) ExecSuperstep(params *protos.ExecSuperstepParams) (*protos.ExecSuperstepResult, error) {
	startTime := time.Now()
	superstep := int(params.Superstep)
	prevSuperstep := superstep - 1

	aggregatorSet, err := aggregator.NewSetFromMessages(params.Aggregators)
	if err != nil {
		return nil, err
	}

	if err := task.store.Connect(); err != nil {
		return nil, err
	}
	defer task.store.Close()

	vrange := store.VertexRange(params.VertexRange)

	messages, err := task.loadVertexMessages(prevSuperstep, vrange)
	if err != nil {
		return nil, err
	}

	halted, err := task.loadHaltedVertices(prevSuperstep, vrange)
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
		Stats: &protos.Stats{
			TotalDuration:     int64(elapsed),
			ComputedCount:     int32(processResult.Stats.ComputedCount),
			ComputeDuration:   int64(processResult.Stats.ComputeDuration),
			HaltedCount:       int32(processResult.Stats.HaltedCount),
			SentMessagesCount: int32(processResult.Stats.SentMessagesCount),
			InactiveCount:     int32(processResult.Stats.InactiveCount),
		},
	}, nil
}

func (task *PregelTask) loadVertexMessages(superstep int, vrange store.VertexRange) (map[string]interface{}, error) {
	messages, err := task.store.LoadVertexMessages(task.jobID, superstep, vrange)
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

func (task *PregelTask) loadHaltedVertices(superstep int, vrange store.VertexRange) (map[string]bool, error) {
	halted, err := task.store.LoadHaltedVertices(task.jobID, superstep, vrange)
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
