package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/warriorguo/workflow/store"
	"github.com/warriorguo/workflow/types"
	"github.com/warriorguo/workflow/utils"
)

const (
	RunContextPath = "/run_context/"
)

var (
	Termination runContext = nil
)

type runContext interface {
	runOnce(fc *flowContext, input types.Data) (runContext, types.Data, error)
	getPath() utils.Path
}

func newBatchRunner(concurrency int, asyncFlag bool) *batchRunner {
	return &batchRunner{
		wp:        workerpool.New(concurrency),
		asyncFlag: asyncFlag,
	}
}

type batchRunner struct {
	mu sync.Mutex

	wp        *workerpool.WorkerPool
	asyncFlag bool
	runners   map[string]*contextRunner
}

func (b *batchRunner) exists(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, exists := b.runners[key]
	return exists
}

func (b *batchRunner) get(key string) *contextRunner {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.runners[key]
}

func (b *batchRunner) remove(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.runners[key]; !exists {
		return
	}
	delete(b.runners, key)
}

func (b *batchRunner) add(key string, r *contextRunner) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.runners == nil {
		b.runners = make(map[string]*contextRunner)
	}
	if _, exists := b.runners[key]; exists {
		return errors.AlreadyExistsf("key: %s", key)
	}
	b.runners[key] = r
	return nil
}

func (b *batchRunner) stopWait(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.wp.StopWait()

	var retErr error
	for key, r := range b.runners {
		err := r.setStatus(ctx, types.Paused)
		if err != nil {
			retErr = errors.Wrapf(retErr, err, "failed on %s", key)
		}
	}
	return retErr
}

func (b *batchRunner) runOnce(ctx context.Context, maxRunAmount int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.runners) == 0 {
		return nil
	}

	runAmount := 0
	for key, r := range b.runners {
		if !r.canRun() {
			continue
		}
		if runAmount++; runAmount > maxRunAmount {
			break
		}

		var err error
		if b.asyncFlag {
			err = errors.Trace(r.tryAsyncRunOnce(ctx, b.wp, key))
		} else {
			err = errors.Trace(r.runOnce(ctx, key))
		}
		if err != nil {
			return errors.Trace(err)
		}
	}

	keyToRemoved := make([]string, 0, len(b.runners))
	for key, r := range b.runners {
		if r.tryCheckCanRemove() {
			keyToRemoved = append(keyToRemoved, key)
		}
	}
	for _, key := range keyToRemoved {
		delete(b.runners, key)
	}
	return nil
}

type contextRunner struct {
	mu    sync.Mutex
	store store.Store

	errMu   sync.Mutex
	errCh   chan error
	lastErr error

	runningStatus types.StatusType

	nextStatusMu sync.Mutex
	nextStatus   types.StatusType

	createTime  time.Time
	lastRunTime time.Time
	nextRunTime time.Time

	runningRC   runContext
	fc          *flowContext
	currentData types.Data
}

type flowRerunContext struct {
	Status     types.StatusType `json:",omitempty"`
	Entrypoint utils.Path       `json:",omitempty"`
	Data       types.Data       `json:",omitempty"`
}

func (r *contextRunner) exportRerunContext() *flowRerunContext {
	if r.runningRC == nil {
		return nil
	}
	return &flowRerunContext{
		Status:     r.runningStatus,
		Entrypoint: r.runningRC.getPath(),
		Data:       r.currentData,
	}
}

func (r *contextRunner) saveContext(ctx context.Context) error {
	rerunC := r.exportRerunContext()
	if rerunC == nil {
		return errors.Trace(r.store.Remove(ctx, RunContextPath, r.fc.requestID))
	}

	b, err := utils.Serialize(rerunC)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(r.store.Set(ctx, RunContextPath, r.fc.requestID, b))
}

func newContextRunner(store store.Store, requestID string, rc runContext, input types.Data) *contextRunner {
	cr := &contextRunner{}
	cr.store = store
	cr.runningStatus = types.Pending
	cr.currentData = input
	cr.runningRC = rc
	cr.createTime = time.Now()
	cr.fc = newFlowContext(store, requestID)

	return cr
}

func (r *contextRunner) setNextStatus(status types.StatusType) error {
	r.nextStatusMu.Lock()
	defer r.nextStatusMu.Unlock()

	currentStatus := r.runningStatus
	if !r.canSetStatus(currentStatus, status) {
		return errors.Forbiddenf("unsupport to set status from %v to %v",
			currentStatus, status)
	}
	r.nextStatus = status
	return nil
}

func (r *contextRunner) canSetStatus(currentStatus, status types.StatusType) bool {
	switch status {
	case types.Paused, types.Retrying:
		return currentStatus == types.Pending ||
			currentStatus == types.Retrying ||
			currentStatus == types.Paused ||
			currentStatus == types.Running

	case types.Fatal:
		return currentStatus != types.Finished

	default:
		return false
	}
}

func (r *contextRunner) assignNextStatus() {
	r.nextStatusMu.Lock()
	defer r.nextStatusMu.Unlock()

	if r.nextStatus != types.None {
		currentStatus := r.runningStatus
		if r.canSetStatus(currentStatus, r.nextStatus) {
			r.runningStatus = r.nextStatus
		} else {
			log.Errorf("%s failed to set status from %v to %v", r.fc.requestID, currentStatus, r.nextStatus)
		}
		r.nextStatus = types.None
	}
}

func (r *contextRunner) canRun() bool {
	if !r.mu.TryLock() {
		return false
	}
	defer r.mu.Unlock()

	if r.runningStatus == types.Pending ||
		r.runningStatus == types.Running ||
		r.runningStatus == types.Retrying {
		return time.Now().After(r.nextRunTime)
	}
	return false
}

func (r *contextRunner) tryCheckCanRemove() bool {
	if !r.mu.TryLock() {
		return false
	}
	defer r.mu.Unlock()

	if r.runningStatus == types.Failed ||
		r.runningStatus == types.Finished {
		return true
	}
	return false
}

func (r *contextRunner) tryAsyncRunOnce(ctx context.Context, wp *workerpool.WorkerPool, logPrefix string) error {
	r.errMu.Lock()
	defer r.errMu.Unlock()

	if r.errCh == nil {
		r.errCh = make(chan error, 1)
		wp.Submit(func() {
			r.errCh <- r.runOnce(ctx, logPrefix)
		})
	}

	select {
	case err := <-r.errCh:
		close(r.errCh)
		r.errCh = nil
		return errors.Trace(err)
	default:
		return nil
	}
}

func (r *contextRunner) setStatus(ctx context.Context, status types.StatusType) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.runningStatus = status
	return errors.Trace(r.saveContext(ctx))
}

func (r *contextRunner) runOnce(ctx context.Context, logPrefix string) error {
	if !r.canRun() {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.runningStatus = types.Running
	r.lastRunTime = time.Now()

	r.fc.startRecord(ctx, r.runningRC.getPath(), r.currentData)
	r.fc.Context = ctx
	nextRC, output, err := r.runningRC.runOnce(r.fc, r.currentData)
	r.fc.endRecord(ctx, output, err)

	if err != nil {
		return r.checkOnError(err)
	}

	r.runningRC = nextRC
	r.currentData = output

	if nextRC == Termination {
		r.runningStatus = types.Finished
	}

	r.assignNextStatus()
	return errors.Trace(r.saveContext(ctx))
}

func (r *contextRunner) checkOnError(err error) error {
	r.lastErr = err

	switch e := errors.Unwrap(err).(type) {
	case *types.FatalError:
		r.runningStatus = types.Fatal
		return nil

	case *types.RetryError:
		r.nextRunTime = time.Now().Add(e.Backoff)
		r.runningStatus = types.Retrying
		return nil

	case *types.PauseError:
		r.runningStatus = types.Paused
		return nil

	default:
		r.runningStatus = types.Failed
		return nil
	}
}

func (r *contextRunner) getStatus() (*types.RequestStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	status := &types.RequestStatus{
		Status:           r.runningStatus,
		LastVertexRecord: r.fc.rcRecord,
	}

	if r.lastErr != nil {
		status.LastError = r.lastErr.Error()
	}

	return status, nil
}
