package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/warriorguo/workflow/store/mem"
	"github.com/warriorguo/workflow/types"
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
	s := mem.NewMemStore()
	flow := newFlow(s, newOptions())

	singlef := &singleDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test1", singlef.testDAG))
	d, exists := flow.getDAG("test1")
	assert.True(t, exists)
	assert.NotNil(t, d)

	f := &condDAG{}
	f.t = t
	err := flow.RegisterDAG("test_cond", f.testDAG)
	assert.Nil(t, err)
	testCond, exists := flow.getDAG("test_cond")
	assert.True(t, exists)
	assert.NotNil(t, d)

	subf := &subDAGWithNode{}
	assert.Nil(t, flow.RegisterDAG("test2", subf.testDAG))
	subD, exists := flow.getDAG("test2")
	assert.True(t, exists)
	assert.NotNil(t, subD)

	dot, err := flow.renderDOT(&d.dagExecutePlan, nil)
	assert.Nil(t, err)
	fmt.Printf("test dag DOT: %+v\n", dot)

	dot, err = flow.renderDOT(&testCond.dagExecutePlan, nil)
	assert.Nil(t, err)
	fmt.Printf("test cond dag DOT: %+v\n", dot)

	dot, err = flow.renderDOT(&subD.dagExecutePlan, nil)
	assert.Nil(t, err)
	fmt.Printf("test cond dag DOT: %+v\n", dot)

	sf1 := &drawSubFlow1{t: t}
	sf2 := &drawSubFlow2{t: t}
	df := &drawFlow{t: t}
	assert.Nil(t, flow.RegisterDAG("sub1", sf1.DAG))
	assert.Nil(t, flow.RegisterDAG("sub2", sf2.DAG))
	assert.Nil(t, flow.RegisterDAG("draw", df.DAG))

	draw, exists := flow.getDAG("draw")
	assert.True(t, exists)
	assert.NotNil(t, draw)

	dot, err = flow.renderDOT(&draw.dagExecutePlan, nil)
	assert.Nil(t, err)
	fmt.Printf("draw dag DOT: %+v\n", dot)

	flow.asyncRun()
	input := types.Data{}
	input.Set("port", 8080)
	input.Set("domain", "www.google.com")
	assert.Nil(t, flow.RunDAG(context.Background(), "draw", "draw_request_id", input))
	time.Sleep(100 * time.Millisecond)
	dot, err = flow.RenderRequestStatus(context.Background(), "draw_request_id")
	assert.Nil(t, err)
	fmt.Printf("draw dag with status DOT: %+v\n", dot)
}
