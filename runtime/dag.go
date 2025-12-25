package runtime

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/warriorguo/workflow/types"
)

var (
	_ types.DAG = &dagEntity{}
)

type vertexType int

const (
	vertexNode vertexType = 1
	vertexCond vertexType = 2
	vertexDAG  vertexType = 3
)

type vertexEntity struct {
	name string
	typ  vertexType

	handler     types.NodeHandler
	dag         *dagEntity
	condHandler types.BooleanHandler
}

type globalVertex struct {
	mu sync.Mutex

	vertex map[string]*vertexEntity
}

func newGlobalVertex() *globalVertex {
	return &globalVertex{vertex: map[string]*vertexEntity{}}
}

func (gv *globalVertex) formatKey(dagName, vertexName string) string {
	return fmt.Sprintf("%s.%s", dagName, vertexName)
}

func (gv *globalVertex) register(dagName, vertexName string, entity *vertexEntity) error {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	key := gv.formatKey(dagName, vertexName)
	if _, exists := gv.vertex[key]; exists {
		return errors.AlreadyExistsf("key: %s", vertexName)
	}
	gv.vertex[key] = entity
	return nil
}

func (gv *globalVertex) exists(dagName, vertexName string) bool {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	_, exists := gv.vertex[gv.formatKey(dagName, vertexName)]
	return exists
}

func (gv *globalVertex) get(dagName, vertexName string) *vertexEntity {
	gv.mu.Lock()
	defer gv.mu.Unlock()

	return gv.vertex[gv.formatKey(dagName, vertexName)]
}

type dagEntity struct {
	dagExecutePlan

	belongFlow *flow
}

func newDAGEntity(name string, belongFlow *flow) *dagEntity {
	dag := &dagEntity{belongFlow: belongFlow}
	dag.Name = name
	dag.Vertex = make(map[string]*vertexInfo)
	dag.Links = make(map[string]string)
	return dag
}

func (de *dagEntity) Node(vertex string, handler types.NodeHandler, options ...types.ExecutionOption) error {
	if handler == nil {
		return errors.BadRequestf("vertex node:%s handler is nil", vertex)
	}
	v := &vertexEntity{}
	v.name = vertex
	v.typ = vertexNode
	v.handler = handler

	if err := de.belongFlow.gl.register(de.Name, vertex, v); err != nil {
		return errors.Trace(err)
	}

	de.Vertex[vertex] = makeNodeVertexInfo()
	if de.StartVertex == "" {
		de.StartVertex = vertex
	}
	return nil
}

func (de *dagEntity) SubDAG(vertex string, dagName string) error {
	otherDagEntity, exists := de.belongFlow.getDAG(dagName)
	if !exists {
		return errors.NotFoundf("DAG: %s", dagName)
	}

	v := &vertexEntity{}
	v.name = vertex
	v.typ = vertexDAG
	v.dag = otherDagEntity

	if err := de.belongFlow.gl.register(de.Name, vertex, v); err != nil {
		return errors.Trace(err)
	}
	if de.StartVertex == "" {
		de.StartVertex = vertex
	}
	de.Vertex[vertex] = makeDAGVertexInfo(&otherDagEntity.dagExecutePlan)
	return nil
}

func (de *dagEntity) Condition(vertex, trueVertex, falseVertex string, handler types.BooleanHandler, options ...types.ExecutionOption) error {
	if handler == nil {
		return errors.BadRequestf("vertex node:%s handler is nil", vertex)
	}

	if !de.belongFlow.gl.exists(de.Name, trueVertex) {
		return errors.NotFoundf("true vertex: %v", trueVertex)
	}
	if !de.belongFlow.gl.exists(de.Name, falseVertex) {
		return errors.NotFoundf("false vertex: %v", falseVertex)
	}

	v := &vertexEntity{}
	v.name = vertex
	v.typ = vertexCond
	v.condHandler = handler

	if err := de.belongFlow.gl.register(de.Name, vertex, v); err != nil {
		return errors.Trace(err)
	}

	// correct the start vertex
	if de.StartVertex == "" || trueVertex == de.StartVertex || falseVertex == de.StartVertex {
		de.StartVertex = vertex
	}

	de.Vertex[vertex] = makeCondVertexInfo(trueVertex, falseVertex)
	return nil
}

func (de *dagEntity) Edge(from, to string) error {
	if link, exists := de.Links[from]; exists {
		return errors.AlreadyExistsf("from %s to %s", from, link)
	}

	fromVertex := de.belongFlow.gl.get(de.Name, from)
	if fromVertex == nil {
		return errors.NotFoundf("from: %v", from)
	}
	if fromVertex.typ == vertexCond {
		return errors.BadRequestf("from: %v can not set edge", from)
	}

	if !de.belongFlow.gl.exists(de.Name, to) {
		return errors.NotFoundf("to: %v", to)
	}

	if checkLinkValid(de.Links, to, from) {
		return errors.Forbiddenf("%s -> %s is linked", to, from)
	}
	de.Links[from] = to

	// correct the start vertex
	if to == de.StartVertex {
		de.StartVertex = from
	}
	return nil
}

func checkLinkValid(links map[string]string, from, to string) bool {
	for {
		var exists bool = false
		if from, exists = links[from]; !exists {
			return false
		}
		if from == to {
			return true
		}
	}
}
