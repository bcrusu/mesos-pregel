package messagesProcessor

import (
	"fmt"
	"sync"
	"time"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/aggregator"
	"github.com/bcrusu/mesos-pregel/algorithm"
	"github.com/bcrusu/mesos-pregel/executor/graph"
)

const (
	MaxComputeParallelism = 32
)

type MessagesProcessor struct {
	jobID                    string
	superstep                int
	graph                    *graph.Graph
	algorithm                algorithm.Algorithm
	prevSuperstepAggregators *aggregator.ImmutableAggregatorSet
}

type ProcessResult struct {
	Stats       *ProcessResultStats
	Entities    *ProcessResultEntities
	Aggregators *aggregator.AggregatorSet
}

type ProcessResultStats struct {
	ComputedCount     int
	ComputeDuration   time.Duration
	SentMessagesCount int
	HaltedCount       int
	InactiveCount     int
}

type ProcessResultEntities struct {
	VertexMessages   []*pregel.VertexMessage
	VertexOperations []*pregel.VertexOperation
	HaltedVertices   []*pregel.VertexHalted
	EdgeOperations   []*pregel.EdgeOperation
}

type computeRequest struct {
	context *algorithm.VertexContext
	message interface{}
}

type computeResult struct {
	computeDuration time.Duration
}

func New(jobID string, superstep int, graph *graph.Graph, algorithm algorithm.Algorithm,
	prevSuperstepAggregators *aggregator.ImmutableAggregatorSet) *MessagesProcessor {
	return &MessagesProcessor{jobID, superstep, graph, algorithm, prevSuperstepAggregators}
}

func (proc *MessagesProcessor) Process(messages map[string]interface{}, haltedVertices map[string]bool) (*ProcessResult, error) {
	stopChan := make(chan struct{})
	errorChan := make(chan error, MaxComputeParallelism)
	inactiveChan := make(chan string)
	operations := newContextOperations(proc.algorithm, errorChan)

	requestChan := proc.getComputeRequestChan(messages, haltedVertices, operations, errorChan, inactiveChan, stopChan)
	resultChan := proc.getComputeResultChan(requestChan, errorChan)

	// process results and gather statistics
	var err error
	stopped := false
	var wg sync.WaitGroup

	var inactiveCount int
	var computedCount int
	var computeDuration time.Duration

	go func() {
		for _ = range inactiveChan {
			inactiveCount++
		}
	}()

	go func() {
		for e := range errorChan {
			// stop on first error
			if !stopped {
				close(stopChan)
				stopped = true
				err = e
			}
		}
	}()

	go func() {
		for result := range resultChan {
			computedCount++
			computeDuration += result.computeDuration
		}
		wg.Done()
	}()

	// wait resultChan processing to complete
	wg.Add(1)
	wg.Wait()
	close(inactiveChan)
	close(errorChan)

	if err != nil {
		return nil, err
	}

	entities, err := operations.GetEntities(proc.jobID, proc.superstep)
	if err != nil {
		return nil, err
	}

	return &ProcessResult{
		Stats: &ProcessResultStats{
			ComputedCount:     computedCount,
			ComputeDuration:   computeDuration,
			SentMessagesCount: len(entities.VertexMessages),
			HaltedCount:       len(entities.HaltedVertices),
			InactiveCount:     inactiveCount,
		},
		Entities:    entities,
		Aggregators: operations.aggregators,
	}, nil
}

func (proc *MessagesProcessor) createVertexContext(id string, operations *contextOperations) *algorithm.VertexContext {
	graph := proc.graph

	value, _ := graph.VertexValue(id)
	mutableValue, _ := graph.VertexMutableValue(id)
	vertexContext := algorithm.NewVertexContext(id, proc.superstep, value, mutableValue, operations, proc.prevSuperstepAggregators)

	edges := graph.EdgesFrom(id)
	vertexContext.Edges = make([]*algorithm.EdgeContext, len(edges))
	for i, to := range edges {
		value, _ := graph.EdgeValue(id, to)
		mutableValue, _ := graph.EdgeMutableValue(id, to)
		vertexContext.Edges[i] = algorithm.NewEdgeContext(vertexContext, to, value, mutableValue)
	}

	return vertexContext
}

func getUniqueVertices(graph *graph.Graph, messages map[string]interface{}) map[string]bool {
	result := make(map[string]bool)
	for _, id := range graph.Vertices() {
		result[id] = true
	}

	for id := range messages {
		result[id] = true
	}

	return result
}

func (proc *MessagesProcessor) getComputeRequestChan(messages map[string]interface{}, haltedVertices map[string]bool,
	operations *contextOperations, errorChan chan error, inactiveChan chan string,
	stopChan chan struct{}) chan *computeRequest {
	ch := make(chan *computeRequest)

	go func() {
	loop:
		for id := range getUniqueVertices(proc.graph, messages) {
			message := messages[id]
			halted := haltedVertices[id]

			if message == nil && halted {
				inactiveChan <- id
				continue
			}

			if !proc.graph.HasVertex(id) {
				// vertices must be created explicitly in the previous superstep
				errorChan <- fmt.Errorf("graph does not contain vertex %s", id)
				continue
			}

			context := proc.createVertexContext(id, operations)

			select {
			case <-stopChan:
				break loop
			case ch <- &computeRequest{context, message}:
			}
		}

		close(ch)
	}()

	return ch
}

func (proc *MessagesProcessor) getComputeResultChan(requestChan chan *computeRequest, errorChan chan error) chan *computeResult {
	ch := make(chan *computeResult, MaxComputeParallelism)
	var wg sync.WaitGroup
	wg.Add(MaxComputeParallelism)

	for i := 0; i < MaxComputeParallelism; i++ {
		go func() {
			defer wg.Done()

			for item := range requestChan {
				start := time.Now()
				err := proc.algorithm.Compute(item.context, item.message)
				elapsed := time.Since(start)

				if err != nil {
					errorChan <- err
				} else {
					ch <- &computeResult{computeDuration: elapsed}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}
