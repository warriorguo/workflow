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

type failRegFlow struct {
	t *testing.T
}

func (d *failRegFlow) failDAG1(dag types.DAG) error {
	return dag.SubDAG("subdag", "test1")
}
func (d *failRegFlow) failDAG2(dag types.DAG) error {
	return dag.Node("node1", nil)
}
func (d *failRegFlow) failDAG3(dag types.DAG) error {
	return dag.Edge("node1", "node2")
}
func (d *failRegFlow) failDAG4(dag types.DAG) error {
	return dag.Condition("cond", "node1", "node2", nil)
}
func (d *failRegFlow) failDAG5(dag types.DAG) error {
	return dag.Condition("cond", "node1", "node2", func(ctx types.Context, input types.Data) (bool, error) {
		return false, nil
	})
}

func (d *failRegFlow) testDAG(dag types.DAG) error {
	assert.NotNil(d.t, dag.Edge("node1", "node2"))
	assert.Nil(d.t, dag.Node("node1", func(ctx types.Context, input types.Data) (types.Data, error) {
		fmt.Printf("node 1\n")
		return input, nil
	}))
	assert.NotNil(d.t, dag.Node("node1", func(ctx types.Context, input types.Data) (types.Data, error) {
		return input, nil
	}))
	assert.NotNil(d.t, dag.Edge("node1", "node2"))
	assert.NotNil(d.t, dag.Edge("node2", "node1"))
	assert.Nil(d.t, dag.Node("node2", func(ctx types.Context, input types.Data) (types.Data, error) {
		fmt.Printf("node 2\n")
		return input, nil
	}))
	assert.NotNil(d.t, dag.Node("node2", func(ctx types.Context, input types.Data) (types.Data, error) {
		return input, nil
	}))
	assert.Nil(d.t, dag.Edge("node1", "node2"))
	assert.NotNil(d.t, dag.Edge("node1", "node2"))
	assert.NotNil(d.t, dag.Edge("node2", "node1"))

	assert.NotNil(d.t, dag.Node("node2", func(ctx types.Context, input types.Data) (types.Data, error) {
		return input, nil
	}))
	assert.NotNil(d.t, dag.Node("node2", func(ctx types.Context, input types.Data) (types.Data, error) {
		return input, nil
	}))
	assert.Nil(d.t, dag.Condition("cond", "node1", "node2", func(ctx types.Context, input types.Data) (bool, error) {
		return false, nil
	}))
	assert.Nil(d.t, dag.Node("node3", func(ctx types.Context, input types.Data) (types.Data, error) {
		fmt.Printf("node 3\n")
		return input, nil
	}))
	assert.Nil(d.t, dag.Edge("node2", "node3"))
	assert.Nil(d.t, dag.Node("node4", func(ctx types.Context, input types.Data) (types.Data, error) {
		fmt.Printf("node 3\n")
		return input, nil
	}))
	assert.Nil(d.t, dag.Edge("node3", "node4"))
	assert.Nil(d.t, dag.Node("node5", func(ctx types.Context, input types.Data) (types.Data, error) {
		fmt.Printf("node 3\n")
		return input, nil
	}))
	assert.Nil(d.t, dag.Edge("node4", "node5"))
	return nil
}

type failOnFlow struct {
	t *testing.T

	retryCount int
}

func (d *failOnFlow) testDAG(dag types.DAG) error {
	assert.Nil(d.t, dag.Node("retry", func(ctx types.Context, input types.Data) (types.Data, error) {
		fmt.Printf("retry left: %v\n", d.retryCount)
		if d.retryCount--; d.retryCount >= 0 {
			return nil, types.NewRetryError(errors.Errorf("retry"), time.Millisecond*90)
		}
		return input, nil
	}))
	assert.Nil(d.t, dag.Node("fatal", func(ctx types.Context, input types.Data) (types.Data, error) {
		fmt.Printf("trigger a fatal error\n")
		return input, types.NewFatalError(errors.Errorf("fatal"))
	}))
	assert.Nil(d.t, dag.Node("normal", func(ctx types.Context, input types.Data) (types.Data, error) {
		return input, nil
	}))
	assert.Nil(d.t, dag.Edge("retry", "fatal"))
	assert.Nil(d.t, dag.Edge("fatal", "normal"))
	return nil
}

func TestFailCondition(t *testing.T) {
	var setError error = nil
	s := mem.NewMemStoreWithErrHandler(func() error {
		return setError
	})
	flow := newFlow(s, newOptions())
	d := &failRegFlow{t: t}

	assert.NotNil(t, flow.RegisterDAG("test", d.failDAG1))
	assert.NotNil(t, flow.RegisterDAG("test", d.failDAG2))
	assert.NotNil(t, flow.RegisterDAG("test", d.failDAG3))
	assert.NotNil(t, flow.RegisterDAG("test", d.failDAG4))
	assert.NotNil(t, flow.RegisterDAG("test", d.failDAG5))
	assert.Nil(t, flow.RegisterDAG("test", d.testDAG))

	inputData := types.Data{}
	assert.Nil(t, flow.RunDAG(context.Background(), "test", "test-require-id", inputData))
	assert.Nil(t, flow.runOnce())
	setError = errors.Errorf("set error")
	assert.NotNil(t, flow.runOnce())
	assert.NotNil(t, flow.runOnce())
	assert.NotNil(t, flow.runOnce())
	assert.NotNil(t, flow.runOnce())
	setError = nil
	assert.Nil(t, flow.runOnce())

	failFlow := newFlow(s, newOptions())
	fd := &failOnFlow{t: t, retryCount: 1}
	assert.Nil(t, failFlow.RegisterDAG("test", fd.testDAG))
	assert.Nil(t, failFlow.RunDAG(context.Background(), "test", "test-require-id0", inputData))

	fmt.Printf("run once for fail flow immediately\n")
	assert.Nil(t, failFlow.runOnce())
	fmt.Printf("run once for fail flow immediately\n")
	assert.Nil(t, failFlow.runOnce())
	dot, err := failFlow.loadRequestAndRender(context.Background(), "test-require-id0")
	assert.Nil(t, err)
	fmt.Printf("dot: %s\n", dot)
	fmt.Printf("run once for fail flow 1 seconds later\n")
	time.Sleep(100 * time.Millisecond)
	assert.Nil(t, failFlow.runOnce())
	dot, err = failFlow.loadRequestAndRender(context.Background(), "test-require-id0")
	assert.Nil(t, err)
	fmt.Printf("dot: %s\n", dot)
	fmt.Printf("run once for fail flow\n")
	assert.Nil(t, failFlow.runOnce())
	fmt.Printf("run once for fail flow\n")
	assert.Nil(t, failFlow.runOnce())
	fmt.Printf("run once for fail flow\n")
	assert.Nil(t, failFlow.runOnce())
	dot, err = failFlow.loadRequestAndRender(context.Background(), "test-require-id0")
	assert.Nil(t, err)
	fmt.Printf("dot: %s\n", dot)
}
