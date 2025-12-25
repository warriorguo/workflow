package runtime

import (
	"fmt"
	"sync/atomic"

	"github.com/juju/errors"

	"github.com/warriorguo/workflow/types"
	"github.com/warriorguo/workflow/utils"
)

var (
	shouldNotReach = errors.New("should not reach here")
)

type nodeType int

const (
	node      nodeType = 1
	condition nodeType = 2
)

type nodeRuntime struct {
	utils.Concurrency
	utils.Backoff

	name     string
	path     utils.Path
	nodeType nodeType

	node struct {
		handler    types.NodeHandler
		nextRC     runContext
		nextVertex string
	}
	cond struct {
		handler     types.BooleanHandler
		trueRC      runContext
		trueVertex  string
		falseRC     runContext
		falseVertex string
	}

	runtimeData *types.NodeRuntimeData
}

func newNodeRuntime() *nodeRuntime {
	nr := &nodeRuntime{}
	nr.runtimeData = &types.NodeRuntimeData{}
	return nr
}

func (n *nodeRuntime) getPath() utils.Path {
	return n.path
}

func (n *nodeRuntime) runCond(fc *flowContext, input types.Data) (runContext, types.Data, error) {
	boolRet, err := n.cond.handler(fc, input)
	if err != nil {
		return n, nil, err
	}
	if boolRet {
		return n.cond.trueRC, input, nil
	}
	return n.cond.falseRC, input, nil
}

func (n *nodeRuntime) runNode(fc *flowContext, input types.Data) (runContext, types.Data, error) {
	output, err := n.node.handler(fc, input)
	if err != nil {
		return n, nil, err
	}
	return n.node.nextRC, output, nil
}

func (n *nodeRuntime) runHandler(fc *flowContext, input types.Data) (nextRC runContext, output types.Data, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = types.NewFatalError(fmt.Errorf("panic on %s: %v", fc.GetCurrentVertex(), r))
		}
	}()
	switch n.nodeType {
	case node:
		{
			return n.runNode(fc, input)
		}
	case condition:
		{
			return n.runCond(fc, input)
		}
	}
	return nil, nil, types.NewFatalError(shouldNotReach)
}

func (n *nodeRuntime) runOnce(fc *flowContext, input types.Data) (runContext, types.Data, error) {
	n.Acquire()
	defer n.Release()

	atomic.AddInt32(&n.runtimeData.CurrentRunning, 1)
	defer func() {
		atomic.AddInt32(&n.runtimeData.CurrentRunning, -1)
		// atomic.AddInt64(&n.runtimeData.SuccessTimes, 1)
		//

		// TODO: handle panic
	}()

	fc.enterNode(n.name)
	defer fc.exitNode(n.name)

	rc, output, err := n.runHandler(fc, input)
	if err != nil {
		atomic.AddInt64(&n.runtimeData.FailedTimes, 1)
	} else {
		atomic.AddInt64(&n.runtimeData.SuccessTimes, 1)
	}
	return rc, output, err
}
