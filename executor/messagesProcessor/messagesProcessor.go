package messagesProcessor

import (
	"sync"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/executor/algorithm"
	"github.com/bcrusu/mesos-pregel/executor/graph"
)

const (
	MaxComputeParallelism = 32
)

type MessagesProcessor struct {
	jobID     string
	superstep int
	graph     *graph.Graph
	algorithm algorithm.Algorithm
}

type ProcessResult struct {
	vertexMessages   []*pregel.VertexMessage
	vertexOperations []*pregel.VertexOperation
	haltedVertices   []*pregel.VertexHalted
	edgeOperations   []*pregel.EdgeOperation
}

type computeRequest struct {
	context *algorithm.VertexContext
	message interface{}
}

type computeResult struct {
}

func New(jobID string, superstep int, graph *graph.Graph, algorithm algorithm.Algorithm) *MessagesProcessor {
	return &MessagesProcessor{jobID, superstep, graph, algorithm}
}

func (proc *MessagesProcessor) Process(messages map[string]interface{}, haltedVertices map[string]bool) (*ProcessResult, error) {
	stopChan := make(chan struct{})
	errorChan := make(chan error, MaxComputeParallelism)
	operations := newContextOperations(proc.algorithm, errorChan)

	requestChan := proc.getComputeRequestChan(messages, haltedVertices, operations, errorChan, stopChan)
	resultChan := proc.getComputeResultChan(requestChan, errorChan)

	// process results
	var err error
	stopped := false

	for _ = range resultChan {
		// check for errors
		select {
		case err = <-errorChan:
			if !stopped {
				close(stopChan)
				stopped = true
			}
		default:
			// noop
		}

		// process result (e.g. gather statistics, etc.)
	}

	if err != nil {
		return nil, err
	}

	return operations.GetProcessResult(proc.jobID, proc.superstep)
}

func (proc *MessagesProcessor) createVertexContext(id string, operations *contextOperations) *algorithm.VertexContext {
	graph := proc.graph

	vertexValue, _ := graph.VertexValue(id)
	vertexContext := algorithm.NewVertexContext(id, proc.superstep, vertexValue, operations)

	edges := graph.EdgesFrom(id)
	vertexContext.Edges = make([]*algorithm.EdgeContext, len(edges))
	for i, to := range edges {
		edgeValue, _ := graph.EdgeValue(id, to)
		vertexContext.Edges[i] = algorithm.NewEdgeContext(vertexContext, to, edgeValue)
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
	operations *contextOperations, errorChan chan error, stopChan chan struct{}) chan *computeRequest {
	ch := make(chan *computeRequest)

	go func() {
	loop:
		for id := range getUniqueVertices(proc.graph, messages) {
			message := messages[id]
			halted := haltedVertices[id]

			if message == nil && halted {
				continue
			}

			if !proc.graph.HasVertex(id) {
				if err := proc.algorithm.Handlers().OnMissingVertex(id); err != nil {
					errorChan <- err
					continue
				}
			}

			context := proc.createVertexContext(id, operations)

			select {
			case <-stopChan:
				break loop
			case ch <- &computeRequest{context, message}:
				//noop
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
				err := proc.algorithm.Compute(item.context, item.message)

				if err != nil {
					errorChan <- err
				} else {
					ch <- &computeResult{}
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
