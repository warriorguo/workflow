package tests

import (
	"fmt"
	"testing"

	workflowengine "github.com/warriorguo/workflow"
	"github.com/warriorguo/workflow/types"
	fetypes "github.com/warriorguo/workflow/types"
	"github.com/stretchr/testify/assert"
)

func dumbNode(ctx types.Context, input types.Data) (types.Data, error) {
	return input, nil
}
func dumbCond(ctx types.Context, input types.Data) (bool, error) {
	return true, nil
}
func fatalNode(ctx types.Context, input types.Data) (types.Data, error) {
	return nil, types.NewFatalErrorf("fatal error")
}

type drawSubFlow1 struct {
	t *testing.T
}

func (d *drawSubFlow1) DAG(dag types.DAG) error {
	assert.Nil(d.t, dag.Node("node1", dumbNode))
	assert.Nil(d.t, dag.Node("node2", dumbNode))

	assert.Nil(d.t, dag.Node("node4", dumbNode))
	assert.Nil(d.t, dag.Node("node5", dumbNode))
	assert.Nil(d.t, dag.Node("node7", dumbNode))

	assert.Nil(d.t, dag.Condition("node3", "node4", "node5", dumbCond))

	assert.Nil(d.t, dag.Edge("node1", "node2"))
	assert.Nil(d.t, dag.Edge("node2", "node3"))
	assert.Nil(d.t, dag.Edge("node4", "node7"))
	assert.Nil(d.t, dag.Edge("node5", "node7"))
	return nil
}

type drawSubFlow2 struct {
	t *testing.T
}

func (d *drawSubFlow2) DAG(dag types.DAG) error {
	assert.Nil(d.t, dag.Node("node1", dumbNode))
	assert.Nil(d.t, dag.Node("node2", dumbNode))
	assert.Nil(d.t, dag.Node("node3", dumbNode))
	assert.Nil(d.t, dag.Node("node4", dumbNode))
	assert.Nil(d.t, dag.Node("node5", dumbNode))

	assert.Nil(d.t, dag.Edge("node1", "node2"))
	assert.Nil(d.t, dag.Edge("node2", "node3"))
	assert.Nil(d.t, dag.Edge("node3", "node4"))
	assert.Nil(d.t, dag.Edge("node4", "node5"))
	return nil
}

type drawFlow struct {
	t *testing.T
}

func (d *drawFlow) DAG(dag types.DAG) error {
	assert.Nil(d.t, dag.Node("node1", dumbNode))
	assert.Nil(d.t, dag.Node("node3", dumbNode))
	assert.Nil(d.t, dag.SubDAG("sub1", "sub1"))
	assert.Nil(d.t, dag.SubDAG("sub2", "sub1"))
	assert.Nil(d.t, dag.SubDAG("sub3", "sub2"))
	assert.Nil(d.t, dag.Node("node4", fatalNode))
	assert.Nil(d.t, dag.Node("node5", dumbNode))

	assert.Nil(d.t, dag.Condition("node2", "sub1", "node3", dumbCond))

	assert.Nil(d.t, dag.Edge("node1", "node2"))
	assert.Nil(d.t, dag.Edge("sub1", "sub2"))
	assert.Nil(d.t, dag.Edge("node3", "sub3"))
	assert.Nil(d.t, dag.Edge("sub3", "node4"))
	assert.Nil(d.t, dag.Edge("sub2", "node5"))
	assert.Nil(d.t, dag.Edge("node4", "node5"))
	return nil
}

func TestRendering(t *testing.T) {
	flowengine, err := workflowengine.NewFlowEngine(fetypes.EnableMemStore())
	assert.Nil(t, err)

	sub1 := &drawSubFlow1{t: t}
	sub2 := &drawSubFlow2{t: t}
	assert.Nil(t, flowengine.RegisterDAG("sub1", sub1.DAG))
	assert.Nil(t, flowengine.RegisterDAG("sub2", sub2.DAG))

	drawf := &drawFlow{t: t}
	assert.Nil(t, flowengine.RegisterDAG("complext_draw", drawf.DAG))
	s, err := flowengine.RenderDAG("complext_draw")
	assert.Nil(t, err)
	fmt.Printf("DOT:\n %s\n", s)
}
