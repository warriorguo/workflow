package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/warriorguo/workflow/store/mem"
	"github.com/warriorguo/workflow/types"
	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"
)

func newAsyncOptions(concurrency int) *types.FlowOptions {
	opts := types.NewFlowOptions()
	opts.AutoStart = false
	opts.MaxNodeConcurrency = concurrency
	opts.MemStore = true
	opts.TaskRunAsync = true
	return opts
}

type singleAsyncDAG struct {
	t *testing.T

	node1Trigger int
	node2Trigger int
	node3Trigger int
}

func (d *singleAsyncDAG) node1(ctx types.Context, input types.Data) (types.Data, error) {
	time.Sleep(500 * time.Millisecond)
	d.node1Trigger++
	return input, nil
}

func (d *singleAsyncDAG) node2(ctx types.Context, input types.Data) (types.Data, error) {
	time.Sleep(500 * time.Millisecond)
	d.node2Trigger++
	return input, nil
}

func (d *singleAsyncDAG) node3(ctx types.Context, input types.Data) (types.Data, error) {
	time.Sleep(500 * time.Millisecond)
	d.node3Trigger++
	return input, nil
}

func (d *singleAsyncDAG) testDAG(dag types.DAG) error {
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

func TestAsyncFlow(t *testing.T) {
	s := mem.NewMemStore()
	flow := newFlow(s, newAsyncOptions(1))

	singlef := &singleAsyncDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test", singlef.testDAG))
	d, exists := flow.GetDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)

	inputData := types.Data{}
	assert.Nil(t, flow.RunDAG(context.Background(), "test", "test-require-id", inputData))

	t1 := time.Now().Add(500 * time.Millisecond)
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 0, singlef.node1Trigger)
	assert.True(t, time.Now().Before(t1))
	time.Sleep(600 * time.Millisecond)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Nil(t, flow.Close(context.Background()))
}

func TestAsyncConcurrencyFlow(t *testing.T) {
	s := mem.NewMemStore()
	flow := newFlow(s, newAsyncOptions(1))

	singlef := &singleAsyncDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test", singlef.testDAG))
	d, exists := flow.GetDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)

	inputData := types.Data{}

	assert.Nil(t, flow.RunDAG(context.Background(), "test", "test-require-id1", inputData))
	assert.Nil(t, flow.RunDAG(context.Background(), "test", "test-require-id2", inputData))

	t1 := time.Now().Add(499 * time.Millisecond)
	assert.Nil(t, flow.runOnce())
	assert.Nil(t, flow.runOnce())
	assert.Nil(t, flow.runOnce())
	assert.Equal(t, 0, singlef.node1Trigger)
	assert.True(t, time.Now().Before(t1))
	time.Sleep(600 * time.Millisecond)
	assert.Equal(t, 1, singlef.node1Trigger)
	assert.Nil(t, flow.runOnce())
	time.Sleep(300 * time.Millisecond)
	assert.Nil(t, flow.runOnce())
	time.Sleep(300 * time.Millisecond)
	assert.Nil(t, flow.runOnce())
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, 2, singlef.node1Trigger)
	assert.Nil(t, flow.Close(context.Background()))
}

func TestAsyncFlowClose1(t *testing.T) {
	s := mem.NewMemStore()
	flow := newFlow(s, newAsyncOptions(1))

	singlef := &singleAsyncDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test", singlef.testDAG))
	d, exists := flow.GetDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)

	inputData := types.Data{}

	assert.Nil(t, flow.RunDAG(context.Background(), "test", "test-require-id1", inputData))
	assert.Nil(t, flow.Close(context.Background()))
	fmt.Printf("mem store: %s", s)
}

func TestAsyncFlowClose2(t *testing.T) {
	s := mem.NewMemStore()
	flow := newFlow(s, newAsyncOptions(1))

	singlef := &singleAsyncDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test", singlef.testDAG))
	d, exists := flow.GetDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)

	inputData := types.Data{}

	assert.Nil(t, flow.RunDAG(context.Background(), "test", "test-require-id1", inputData))
	assert.Nil(t, flow.runOnce())
	t1 := time.Now().Add(99 * time.Millisecond)
	assert.Nil(t, flow.Close(context.Background()))
	assert.True(t, time.Now().After(t1))
	fmt.Printf("mem store: %s", s)
}

func TestAsyncFlowSetStatus1(t *testing.T) {
	s := mem.NewMemStore()
	flow := newFlow(s, newAsyncOptions(1))

	singlef := &singleAsyncDAG{t: t}
	assert.Nil(t, flow.RegisterDAG("test", singlef.testDAG))
	d, exists := flow.GetDAG("test")
	assert.True(t, exists)
	assert.NotNil(t, d)

	inputData := types.Data{}

	assert.Nil(t, flow.RunDAG(context.Background(), "test", "test-require-id1", inputData))
	assert.Nil(t, flow.Close(context.Background()))
	fmt.Printf("mem store: %s", s)

}
