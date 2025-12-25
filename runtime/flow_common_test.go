package runtime

import (
	"context"
	"fmt"
	"testing"

	"github.com/warriorguo/workflow/store/mem"
	"github.com/warriorguo/workflow/types"
	"github.com/warriorguo/workflow/utils"
	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"
)

func newOptions() *types.FlowOptions {
	opts := types.NewFlowOptions()
	opts.AutoStart = false
	opts.MemStore = true
	opts.TaskRunAsync = false
	return opts
}

type singleDAG struct {
	t *testing.T

	node1Trigger int
	node2Trigger int
	node3Trigger int
}

func (d *singleDAG) node1(ctx types.Context, input types.Data) (types.Data, error) {
	assert.True(d.t, len(ctx.GetRequestID()) > 0)
	s1, _ := input.Get("test_param1")
	s2, _ := input.Get("test_param2")
	fmt.Printf("in node1:%s %s\n", s1, s2)
	assert.Equal(d.t, "show me the money", s1)
	assert.Equal(d.t, "black sheep wall", s2)
	d.node1Trigger++
	input.Set("node1", "food for thought")
	return input, nil
}

func (d *singleDAG) node2(ctx types.Context, input types.Data) (types.Data, error) {
	assert.True(d.t, len(ctx.GetRequestID()) > 0)
	s1, _ := input.Get("test_param1")
	s3, _ := input.Get("node1")
	fmt.Printf("in node2:%s %s\n", s1, s3)
	assert.Equal(d.t, "show me the money", s1)
	assert.Equal(d.t, "food for thought", s3)
	d.node2Trigger++
	return input, nil
}

func (d *singleDAG) node3(ctx types.Context, input types.Data) (types.Data, error) {
	assert.True(d.t, len(ctx.GetRequestID()) > 0)
	fmt.Printf("in node3\n")
	d.node3Trigger++
	return input, nil
}

func (d *singleDAG) testDAG(dag types.DAG) error {
	if err := dag.Node("node1", d.node1); err != nil {
		return errors.Trace(err)
	}
	if err := dag.Node("node2", d.node2); err != nil {
		return errors.Trace(err)
	}
	if err := dag.Node("node3", d.node3); err != nil {
		return errors.Trace(err)
	}
	if err := dag.Edge("node1", "node2"); err != nil {
		return errors.Trace(err)
	}
	if err := dag.Edge("node2", "node3"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func TestSingleFlow(t *testing.T) {
	s := mem.NewMemStore()
	flow := newFlow(s, newOptions())

	singlef := &singleDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test", singlef.testDAG))
	d, exists := flow.GetDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)
	fmt.Printf("test dag: %+v\n", d)

	inputData := types.Data{}
	inputData.Set("test_param1", "show me the money")
	inputData.Set("test_param2", "black sheep wall")
	assert.Nil(t, flow.RunDAG(context.Background(), "test", "test-require-id", inputData))
	assert.False(t, flow.isRunningEmpty())
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 0, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)
	assert.Nil(t, flow.Close(context.Background()))
}

type condDAG struct {
	singleDAG

	condFlag    bool
	condTrigger int
}

func (d *condDAG) condNode(ctx types.Context, input types.Data) (bool, error) {
	d.condTrigger++
	fmt.Printf("in cond\n")
	return d.condFlag, nil
}

func (d *condDAG) testDAG(dag types.DAG) error {
	if err := dag.Node("node1", d.node1); err != nil {
		return errors.Trace(err)
	}
	if err := dag.Node("node2", d.node2); err != nil {
		return errors.Trace(err)
	}
	if err := dag.Node("node3", d.node3); err != nil {
		return errors.Trace(err)
	}
	if err := dag.Condition("cond1", "node2", "node3", d.condNode); err != nil {
		return errors.Trace(err)
	}
	if err := dag.Condition("cond2", "node2", "node3", d.condNode); err != nil {
		return errors.Trace(err)
	}
	// can not edge cond to other node
	assert.NotNil(d.t, dag.Edge("cond1", "node1"))

	if err := dag.Edge("node1", "cond1"); err != nil {
		return errors.Trace(err)
	}
	if err := dag.Edge("node3", "cond2"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func TestCondFlow(t *testing.T) {
	flow := newFlow(mem.NewMemStore(), newOptions())
	f := &condDAG{}
	f.t = t

	err := flow.RegisterDAG("test", f.testDAG)
	if err != nil {
		fmt.Printf("err: %+v\n", err)
	}
	assert.Nil(t, err)
	d, exists := flow.GetDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)
	fmt.Printf("test dag: %+v\n", d)

	inputData := types.Data{}
	inputData.Set("test_param1", "show me the money")
	inputData.Set("test_param2", "black sheep wall")
	assert.Nil(t, flow.RunDAG(context.Background(), "test", "testcond-require-id", inputData))
	assert.False(t, flow.isRunningEmpty())
	assert.Nil(t, flow.runOnce())
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 1, f.condTrigger)
	// redirect cond to other side
	f.condFlag = true
	assert.Nil(t, flow.runOnce())
	assert.Nil(t, flow.runOnce())
	assert.Nil(t, flow.runOnce())
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 2, f.condTrigger)
}

func TestFlowEntrypoint(t *testing.T) {
	flow := newFlow(mem.NewMemStore(), newOptions())

	singlef := &singleDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test", singlef.testDAG))
	d, exists := flow.getDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)
	fmt.Printf("test dag: %+v\n", d)

	inputData := types.Data{}
	inputData.Set("test_param1", "show me the money")
	inputData.Set("test_param2", "black sheep wall")
	inputData.Set("node1", "food for thought")

	assert.Nil(t, flow.launchDAG(context.Background(), &d.dagExecutePlan, "test-require-id", inputData, utils.NewPath("test", "node2"), nil))
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 0, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 0, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 0, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
}

func TestRerunFlow(t *testing.T) {
	s := mem.NewMemStore()
	flow := newFlow(s, newOptions())

	singlef := &singleDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test", singlef.testDAG))
	d, exists := flow.GetDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)
	fmt.Printf("test dag: %+v\n", d)

	inputData := types.Data{}
	inputData.Set("test_param1", "show me the money")
	inputData.Set("test_param2", "black sheep wall")
	assert.Nil(t, flow.RunDAG(context.Background(), "test", "test-require-id", inputData))
	assert.False(t, flow.isRunningEmpty())
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 0, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)

	// reset flow
	fmt.Printf("\n#####################\nreset flow\n#####################\n")
	flow = newFlow(s, newOptions())
	singlef = &singleDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test", singlef.testDAG))
	d, exists = flow.GetDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)

	errs, err := flow.reloadPlans(context.Background())
	for id, err := range errs {
		fmt.Printf("reload %s failed:\n%s\n", id, errors.ErrorStack(err))
	}
	assert.NotNil(t, errs)
	for id, err := range errs {
		assert.Equal(t, "test-require-id", id)
		assert.Nil(t, err)
	}
	assert.Nil(t, err)
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 0, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 0, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 0, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)

	records, err := flow.loadRecords(context.Background(), "test-require-id")
	assert.Nil(t, err)
	for _, rs := range records {
		fmt.Printf("records: %+v\n", rs)
	}
}

type subDAGWithNode struct {
	node1Trigger int
}

func (d *subDAGWithNode) node1(ctx types.Context, input types.Data) (types.Data, error) {
	d.node1Trigger++
	return input, nil
}

func (d *subDAGWithNode) testDAG(dag types.DAG) error {
	if err := dag.SubDAG("subdag", "test1"); err != nil {
		return err
	}
	if err := dag.Node("node1", d.node1); err != nil {
		return err
	}
	if err := dag.Edge("node1", "subdag"); err != nil {
		return err
	}
	return nil
}

func TestSubDAGWithNodeFlow(t *testing.T) {
	s := mem.NewMemStore()
	flow := newFlow(s, newOptions())

	singlef := &singleDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test1", singlef.testDAG))
	d, exists := flow.getDAG("test1")
	assert.True(t, exists)
	assert.NotNil(t, d)

	subf := &subDAGWithNode{}
	assert.Nil(t, flow.RegisterDAG("test2", subf.testDAG))
	d, exists = flow.getDAG("test2")
	assert.True(t, exists)
	assert.NotNil(t, d)
	fmt.Printf("test sub dag: %+v\n", d)

	inputData := types.Data{}
	inputData.Set("test_param1", "show me the money")
	inputData.Set("test_param2", "black sheep wall")
	assert.Nil(t, flow.RunDAG(context.Background(), "test2", "test-require-id", inputData))
	assert.False(t, flow.isRunningEmpty())
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 1, subf.node1Trigger)
	assert.Equal(t, 0, singlef.node1Trigger)
	assert.Equal(t, 0, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 0, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	fmt.Printf("store: %v\n", s)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)
}

type subDAG struct {
}

func (d *subDAG) testDAG(dag types.DAG) error {
	return dag.SubDAG("subdag", "test1")
}

func TestSubDAGFlow(t *testing.T) {
	flow := newFlow(mem.NewMemStore(), newOptions())

	singlef := &singleDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test1", singlef.testDAG))
	d, exists := flow.getDAG("test1")
	assert.True(t, exists)
	assert.NotNil(t, d)

	subf := &subDAG{}
	assert.Nil(t, flow.RegisterDAG("test2", subf.testDAG))
	d, exists = flow.getDAG("test2")
	assert.True(t, exists)
	assert.NotNil(t, d)
	fmt.Printf("test sub dag: %+v\n", d)

	inputData := types.Data{}
	inputData.Set("test_param1", "show me the money")
	inputData.Set("test_param2", "black sheep wall")
	assert.Nil(t, flow.RunDAG(context.Background(), "test2", "test-require-id", inputData))
	assert.False(t, flow.isRunningEmpty())
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 0, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 0, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Equal(t, 1, singlef.node2Trigger)
	assert.Equal(t, 1, singlef.node3Trigger)
}

func TestControlFlow(t *testing.T) {
	flow := newFlow(mem.NewMemStore(), newOptions())

	singlef := &singleDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test1", singlef.testDAG))
	d, exists := flow.getDAG("test1")
	assert.True(t, exists)
	assert.NotNil(t, d)

	subf := &subDAG{}
	assert.Nil(t, flow.RegisterDAG("test2", subf.testDAG))
	d, exists = flow.getDAG("test2")
	assert.True(t, exists)
	assert.NotNil(t, d)
	fmt.Printf("test sub dag: %+v\n", d)

	inputData := types.Data{}
	inputData.Set("test_param1", "show me the money")
	inputData.Set("test_param2", "black sheep wall")
	assert.Nil(t, flow.RunDAG(context.Background(), "test2", "test-require-id", inputData))
	assert.False(t, flow.isRunningEmpty())
	assert.Nil(t, flow.runOnce())
	assert.Nil(t, flow.PauseRequest(context.Background(), "test-require-id"))
	assert.Nil(t, flow.runOnce())
	status, err := flow.GetRequestStatus(context.Background(), "test-require-id")
	assert.Nil(t, err)
	assert.Equal(t, types.Paused, status.Status)
}
