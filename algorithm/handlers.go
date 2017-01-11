package algorithm

import (
	"github.com/golang/glog"
)

type Handlers struct {
	OnDuplicateVertex DuplicateVertexFunc
	OnMissingVertex   MissingVertexFunc
	OnDuplicateEdge   DuplicateEdgeFunc
	OnMissingEdge     MissingEdgeFunc
}

type DuplicateVertexFunc func(id string, value1 interface{}, value2 interface{}) (finalValue interface{}, err error)
type MissingVertexFunc func(id string) error
type DuplicateEdgeFunc func(from string, to string, value1 interface{}, value2 interface{}) (finalValue interface{}, err error)
type MissingEdgeFunc func(from string, to string) error

func DefaultHandlers() *Handlers {
	return &Handlers{onDuplicateVertex, onMissingVertex, onDuplicateEdge, onMissingEdge}
}

func onDuplicateVertex(id string, value1 interface{}, value2 interface{}) (interface{}, error) {
	glog.Infof("running default onDuplicateVertex handler; vertex %s", id)
	return value1, nil
}

func onMissingVertex(id string) error {
	glog.Infof("running default onMissingVertex handler; vertex %s", id)
	return nil
}

func onDuplicateEdge(from string, to string, value1 interface{}, value2 interface{}) (interface{}, error) {
	glog.Infof("running default onDuplicateEdge handler; edge from %s to %s", from, to)
	return value1, nil
}

func onMissingEdge(from string, to string) error {
	glog.Infof("running default onMissingEdge handler; edge from %s to %s", from, to)
	return nil
}
