package runtime

import (
	"context"

	"github.com/juju/errors"
	"github.com/warriorguo/workflow/store"
	"github.com/warriorguo/workflow/types"
)

type flowExecute struct {
	ctx    context.Context
	cancel context.CancelFunc

	exitCh  chan struct{}
	running bool

	store store.Store

	concurrency int
	batchRunner *batchRunner
}

func (fe *flowExecute) startExecutePlan(requestID string, dr *dagRuntime, params types.Data) error {
	return fe.batchRunner.add(requestID, newContextRunner(fe.store, requestID, dr, params))
}

func (fe *flowExecute) hasExecutePlan(requestID string) bool {
	return fe.batchRunner.exists(requestID)
}

func (fe *flowExecute) runOnce() error {
	return fe.batchRunner.runOnce(fe.ctx, fe.concurrency)
}

func (fe *flowExecute) isRunningEmpty() bool {
	return len(fe.batchRunner.runners) == 0
}

func (fe *flowExecute) setExecutePlanStatus(requestID string, newStatus types.StatusType) error {
	cr := fe.batchRunner.get(requestID)
	if cr == nil {
		return errors.NotFoundf("request ID:%s", requestID)
	}

	return cr.setNextStatus(newStatus)
}

func (fe *flowExecute) getExecutePlanStatus(requestID string) (*types.RequestStatus, error) {
	cr := fe.batchRunner.get(requestID)
	if cr == nil {
		return nil, errors.NotFoundf("request id: %s", requestID)
	}

	status, err := cr.getStatus()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return status, nil
}
