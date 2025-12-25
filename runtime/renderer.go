package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/warriorguo/workflow/types"
	"github.com/juju/errors"
)

func (f *flow) renderDOT(plan *dagExecutePlan, records map[string]*types.NodeTraceRecord) (string, error) {
	renderer := newDAGRenderer()
	return renderer.generateDOT(plan, records)
}

func (f *flow) loadRequestAndRender(ctx context.Context, requestID string) (string, error) {
	plan, _, err := f.loadPlan(ctx, requestID)
	if err != nil {
		return "", errors.Trace(err)
	}
	records, err := f.loadRecords(ctx, requestID)
	if err != nil {
		return "", errors.Trace(err)
	}
	return f.renderDOT(plan, records)
}

func newDAGRenderer() *dagRenderer {
	return &dagRenderer{nil, &strings.Builder{}}
}

type dagRenderer struct {
	records map[string]*types.NodeTraceRecord
	sb      *strings.Builder
}

func (d *dagRenderer) setRecords(records map[string]*types.NodeTraceRecord) {
	if records == nil {
		records = make(map[string]*types.NodeTraceRecord)
	}
	d.records = records
}

func (d *dagRenderer) generateDOT(plan *dagExecutePlan, records map[string]*types.NodeTraceRecord) (string, error) {
	d.setRecords(records)

	d.write("digraph D {")
	d.drawDAG(plan.Name+".", plan.Name, plan)
	d.write("}")
	return d.sb.String(), nil
}

func packToComment(r *types.NodeTraceRecord) string {
	s, _ := json.Marshal(r)
	return formatNL(addSlashes(string(s)))
}

func (d *dagRenderer) calcAttr(prefix, name string) string {
	record, exists := d.records[prefix+name]
	if !exists {
		return ""
	}

	color := ""
	switch {
	case record.StartTime.IsZero():
		color = "white"
	case record.EndTime.IsZero():
		color = "yellow"
	case record.Error != "":
		color = "red"
	default:
		color = "green"
	}
	return fmt.Sprintf(" style=\"filled\" color=\"%s\" comment=\"%s\"", color, packToComment(record))
}

func (d *dagRenderer) drawCond(prefix, name string, trueVertex, falseVertex string, dag *dagExecutePlan) {
	attr := d.calcAttr(prefix, name)
	d.write("%s [label=%s shape=\"diamond\"%s]", idString(prefix+name), quoteString(name), attr)

	vertexArr := d.getRealVertex(trueVertex, dag, false)
	for _, vertex := range vertexArr {
		d.write("%s -> %s [label=\"True\"]", idString(prefix+name), idString(prefix+vertex))
	}

	vertexArr = d.getRealVertex(falseVertex, dag, false)
	for _, vertex := range vertexArr {
		d.write("%s -> %s [label=\"False\"]", idString(prefix+name), idString(prefix+vertex))
	}
}

func (d *dagRenderer) drawNode(prefix, name string) {
	attr := d.calcAttr(prefix, name)
	d.write("%s [label=%s shape=\"record\"%s]", idString(prefix+name), quoteString(name), attr)
}

func (d *dagRenderer) drawDAG(prefix, name string, dag *dagExecutePlan) {
	for vertexName, v := range dag.Vertex {
		switch v.Type {
		case vertexNode:
			d.drawNode(prefix, vertexName)

		case vertexCond:
			d.drawCond(prefix, vertexName, v.TrueVertex, v.FalseVertex, dag)

		case vertexDAG:
			d.write("subgraph cluster_%s{", idString(prefix+vertexName))
			d.write("style=filled")
			d.write("color=lightgrey")
			d.drawDAG(prefix+vertexName+".", vertexName, v.DAG)
			d.write("}")
		}
	}
	d.drawLinks(prefix, dag)
	d.write("label=%s", quoteString(name))
}

func (d *dagRenderer) getRealVertex(vertex string, dag *dagExecutePlan, isFrom bool) []string {
	v := dag.Vertex[vertex]
	if v == nil {
		return nil
	}
	if v.Type == vertexNode || v.Type == vertexCond {
		return []string{vertex}

	}
	if v.Type == vertexDAG {
		if !isFrom {
			return []string{vertex + "." + v.DAG.StartVertex}
		}
		return v.DAG.calcEndVertex(vertex + ".")
	}
	return nil
}

func (d *dagRenderer) drawLinks(prefix string, dag *dagExecutePlan) {
	for from, to := range dag.Links {
		fromVertex := d.getRealVertex(from, dag, true)
		toVertex := d.getRealVertex(to, dag, false)

		for _, fv := range fromVertex {
			for _, tv := range toVertex {
				d.write("%s -> %s", idString(prefix+fv), idString(prefix+tv))
			}
		}
	}
}

func (d *dagRenderer) write(format string, s ...any) {
	d.sb.WriteString(fmt.Sprintf(format+"\n", s...))
}

var (
	slashesToken = []string{"\\", "\"", "'", " "}
)

func addSlashes(s string) string {
	for _, token := range slashesToken {
		s = strings.ReplaceAll(s, token, "\\"+token)
	}
	return s
}

func formatNL(s string) string {
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func quoteString(s string) string {
	return "\"" + strings.ReplaceAll(s, "\"", "\\\"") + "\""
}

var idleChars = []string{" ", "'", "\"", "(", ")", "*", "&", "^", "%", "$", "#", "@", "!", "?", "<", ">", "[", "]", "{", "}", "."}

func idString(s string) string {
	for _, ch := range idleChars {
		s = strings.ReplaceAll(s, ch, "_")
	}
	return s
}
