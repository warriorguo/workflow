package runtime

import (
	"github.com/warriorguo/workflow/utils"
	"github.com/juju/errors"
)

const (
	DAGPlanPath = "/dag/"
)

type vertexInfo struct {
	Type vertexType `json:",omitempty"`

	TrueVertex  string `json:",omitempty"`
	FalseVertex string `json:",omitempty"`

	DAG *dagExecutePlan `json:",omitempty"`
}

func makeNodeVertexInfo() *vertexInfo {
	return &vertexInfo{vertexNode, "", "", nil}
}

func makeDAGVertexInfo(dagPlan *dagExecutePlan) *vertexInfo {
	return &vertexInfo{vertexDAG, "", "", dagPlan}
}

func makeCondVertexInfo(trueVertex, falseVertex string) *vertexInfo {
	return &vertexInfo{vertexCond, trueVertex, falseVertex, nil}
}

/**
 * dagExecutePlan structure support storeable and also
 * aims to dynamically generates runtime context.
 */
type dagExecutePlan struct {
	Name        string `json:",omitempty"`
	StartVertex string `json:",omitempty"`

	Vertex map[string]*vertexInfo `json:",omitempty"`
	/**
	 * Links store relationship of each vertex
	 * if 2 vertex has a link `a -> b`
	 * then in this map `a` would be Key and `b` would be Value
	 */
	Links map[string]string `json:",omitempty"`
}

type delayVisitHandler func(m map[string]runContext) error

func (dt *dagExecutePlan) visitNext(vertex string, visitHandler func(nextVertex []string)) bool {
	if v, exists := dt.Links[vertex]; exists {
		visitHandler([]string{v})
		return true
	}
	if v := dt.Vertex[vertex]; v != nil && v.Type == vertexCond {
		visitHandler([]string{v.TrueVertex, v.FalseVertex})
		return true
	}
	return false
}

func (dt *dagExecutePlan) calcEndVertex(prefix string) []string {
	depth := 0
	endVertex := make([]string, 0)

	var visitHandler func(nextVertex []string)
	visitHandler = func(nextVertex []string) {
		if depth++; depth > 2*len(dt.Vertex) {
			return
		}
		for _, vertex := range nextVertex {
			if !dt.visitNext(vertex, visitHandler) {
				endVertex = append(endVertex, prefix+vertex)
			}
		}
	}
	if !dt.visitNext(dt.StartVertex, visitHandler) {
		endVertex = append(endVertex, prefix+dt.StartVertex)
	}
	return utils.UniqueSlice(endVertex)
}

func (dt *dagExecutePlan) generateRuntime(gl *globalVertex, parentDAGPath, entrypoint utils.Path) (*dagRuntime, error) {
	/**
	 * entrypoint.First() is supposed to be equal to dt.Name
	 * if entrypoint is `abc.node1`, after abc is supposed to the same as this DAG name.
	 * node1 would be one of the node of this DAG
	 * So after entrypoint.Next(), the entrypoint comes to `node1`, which can used for address the node
	 */
	entrypoint = entrypoint.Next()
	/**
	 * dagPath on the other side stands for the path of this DAG
	 */
	dagPath := parentDAGPath.AddString(dt.Name)

	rt := newDAGRuntime(dagPath)
	rt.name = dt.Name

	var (
		delayHandlers = make([]delayVisitHandler, 0, len(dt.Vertex))
		rcMap         = make(map[string]runContext)
	)
	for vertex, info := range dt.Vertex {
		rc, delayVisit, err := dt.generateRunContext(gl, vertex, info, dt.Links[vertex], dagPath, entrypoint)
		if err != nil {
			return nil, errors.Trace(err)
		}

		delayHandlers = append(delayHandlers, delayVisit)
		rcMap[vertex] = rc
	}

	for _, dh := range delayHandlers {
		if err := dh(rcMap); err != nil {
			return nil, errors.Trace(err)
		}
	}

	exists := false
	entrypointVertex := ""
	if entrypointVertex, exists = entrypoint.First(); !exists {
		entrypointVertex = dt.StartVertex
	}

	if rt.runningRC, exists = rcMap[entrypointVertex]; !exists {
		return nil, errors.NotFoundf("can not find rc:%v", entrypointVertex)
	}

	return rt, nil
}

func (dt *dagExecutePlan) generateNodeRunContext(v *vertexEntity, vertex string, info *vertexInfo, nextVertex string, path utils.Path) (runContext, delayVisitHandler, error) {
	nr := newNodeRuntime()
	nr.name = vertex
	nr.path = path.AddString(vertex)

	switch info.Type {
	case vertexNode:
		nr.nodeType = node
		nr.node.handler = v.handler
		nr.node.nextVertex = nextVertex

		return nr, func(m map[string]runContext) error {
			exists := false
			if nr.node.nextVertex == "" {
				nr.node.nextRC = Termination
			} else if nr.node.nextRC, exists = m[nr.node.nextVertex]; !exists {
				return errors.NotFoundf("can not find rc:%v", nr.node.nextVertex)
			}
			return nil
		}, nil

	case vertexCond:
		nr.nodeType = condition
		nr.cond.handler = v.condHandler
		nr.cond.trueVertex = info.TrueVertex
		nr.cond.falseVertex = info.FalseVertex

		return nr, func(m map[string]runContext) error {
			exists := false
			if nr.cond.trueVertex == "" {
				nr.cond.trueRC = Termination
			} else if nr.cond.trueRC, exists = m[nr.cond.trueVertex]; !exists {
				return errors.NotFoundf("can not find rc:%v", nr.cond.trueRC)
			}
			if nr.cond.falseVertex == "" {
				nr.cond.falseRC = Termination
			} else if nr.cond.falseRC, exists = m[nr.cond.falseVertex]; !exists {
				return errors.NotFoundf("can not find rc:%v", nr.cond.trueRC)
			}
			return nil
		}, nil

	}
	return nil, nil, errors.NotSupportedf("unknown type:%v", info.Type)
}

func (dt *dagExecutePlan) generateRunContext(gl *globalVertex, vertex string, info *vertexInfo, nextVertex string, path, entrypointPath utils.Path) (runContext, delayVisitHandler, error) {
	v := gl.get(dt.Name, vertex)
	if v == nil {
		return nil, nil, errors.NotFoundf("dag:%s vertex:%s", dt.Name, vertex)
	}

	if v.typ != info.Type {
		return nil, nil, errors.Errorf("unexpected unmatch type:%v!=%v on %s.%s", v.typ, info.Type, dt.Name, vertex)
	}

	if info.Type == vertexDAG {
		dr, err := info.DAG.generateRuntime(gl, path, entrypointPath)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		dr.name = vertex
		dr.nextVertex = nextVertex
		// dr.runningRC
		return dr, func(m map[string]runContext) error {
			exists := false
			if dr.nextVertex == "" {
				dr.nextRC = Termination
			} else if dr.nextRC, exists = m[dr.nextVertex]; !exists {
				return errors.NotFoundf("can not find rc:%v", dr.nextVertex)
			}
			return nil
		}, nil
	}

	return dt.generateNodeRunContext(v, vertex, info, nextVertex, path)
}
