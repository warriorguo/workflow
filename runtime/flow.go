package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/warriorguo/workflow/store"
	"github.com/warriorguo/workflow/types"
	"github.com/warriorguo/workflow/utils"
	"github.com/juju/errors"
)

func NewFlowEngine(store store.Store, opts *types.FlowOptions) types.FlowEngine {
	flow := newFlow(store, opts)
	return flow
}

type flow struct {
	flowExecute

	gl *globalVertex

	dagMu       sync.Mutex
	dagEntities map[string]*dagEntity
}

func newFlow(store store.Store, opts *types.FlowOptions) *flow {
	f := &flow{}
	f.ctx, f.cancel = context.WithCancel(opts.Ctx)
	f.store = store
	f.running = true
	f.batchRunner = newBatchRunner(opts.MaxNodeConcurrency, opts.TaskRunAsync)
	f.concurrency = opts.MaxNodeConcurrency
	f.gl = newGlobalVertex()
	f.dagEntities = make(map[string]*dagEntity)

	if opts.AutoStart {
		f.asyncRun()
	}
	return f
}

func (f *flow) asyncRun() {
	readyCh := make(chan struct{}, 1)

	go func() {
		f.exitCh = make(chan struct{})
		close(readyCh)

		for f.running {
			f.runOnce()
			time.Sleep(0)
		}
		close(f.exitCh)
	}()
	<-readyCh
}

func (f *flow) RegisterDAG(name string, handler types.DAGHandler) error {
	if !f.running {
		return errors.MethodNotAllowedf("not running")
	}
	dag := newDAGEntity(name, f)
	if err := handler(dag); err != nil {
		return errors.Trace(err)
	}

	f.dagMu.Lock()
	defer f.dagMu.Unlock()
	f.dagEntities[name] = dag
	return nil
}

func (f *flow) GetDAG(name string) (types.DAG, bool) {
	return f.getDAG(name)
}

func (f *flow) RenderDAG(name string) (string, error) {
	dag, exists := f.getDAG(name)
	if !exists {
		return "", errors.NotFoundf("DAG name: %s", name)
	}
	return f.renderDOT(&dag.dagExecutePlan, nil)
}

func (f *flow) RenderRequestStatus(ctx context.Context, requestID string) (string, error) {
	return f.loadRequestAndRender(ctx, requestID)
}

func (f *flow) GetRequestStatus(ctx context.Context, requestID string) (*types.RequestStatus, error) {
	return f.getExecutePlanStatus(requestID)
}

func (f *flow) ListDAGNames() ([]string, error) {
	names := make([]string, 0, len(f.dagEntities))
	for _, dag := range f.dagEntities {
		names = append(names, dag.Name)
	}
	return names, nil
}

func (fe *flowExecute) PauseRequest(ctx context.Context, requestID string) error {
	return fe.setExecutePlanStatus(requestID, types.Paused)
}

func (fe *flowExecute) ResumeRequest(ctx context.Context, requestID string) error {
	return fe.setExecutePlanStatus(requestID, types.Retrying)
}

func (fe *flowExecute) TerminateRequest(ctx context.Context, requestID string) error {
	return fe.setExecutePlanStatus(requestID, types.Fatal)
}

func (f *flow) getDAG(name string) (*dagEntity, bool) {
	f.dagMu.Lock()
	defer f.dagMu.Unlock()
	dag, exists := f.dagEntities[name]
	return dag, exists
}

func (f *flow) reloadPlans(ctx context.Context) (map[string]error, error) {
	errs := make(map[string]error, 0)
	err := f.store.List(ctx, RunContextPath, func(requestID string) bool {
		err := f.rerunPlan(ctx, requestID)
		errs[requestID] = errors.Trace(err)
		return true
	})
	if len(errs) == 0 {
		errs = nil
	}
	return errs, errors.Trace(err)
}

func (f *flow) rerunPlan(ctx context.Context, requestID string) error {
	if f.batchRunner.exists(requestID) {
		return errors.AlreadyExistsf("request already running: %s", requestID)
	}

	dag, reRC, err := f.loadPlan(ctx, requestID)
	if err != nil {
		return errors.Trace(err)
	}
	if reRC == nil {
		return errors.NotFoundf("rerun context: %s", requestID)
	}
	return f.launchDAG(ctx, dag, requestID, reRC.Data, reRC.Entrypoint, nil)
}

func (f *flow) RunDAG(ctx context.Context, dagName string, requestID string, params types.Data) error {
	if !f.running {
		return errors.MethodNotAllowedf("not running")
	}
	dag, exists := f.getDAG(dagName)
	if !exists {
		return errors.NotFound
	}
	if f.hasExecutePlan(requestID) {
		return errors.AlreadyExistsf("request id: %s", requestID)
	}
	err := f.launchDAG(ctx, &dag.dagExecutePlan, requestID, params, utils.NewPath(), func() error {
		return errors.Trace(f.savePlan(ctx, requestID, &dag.dagExecutePlan))
	})
	if err != nil {
		if lerr := f.removePlan(context.Background(), requestID); lerr != nil {
			err = errors.Wrapf(err, lerr, "remove plan %s failed after launch DAG", requestID)
		}
		return errors.Trace(err)
	}
	return nil
}

func (f *flow) launchDAG(ctx context.Context, dag *dagExecutePlan, requestID string, params types.Data,
	entrypoint utils.Path, preRunHandler func() error) error {
	dr, err := dag.generateRuntime(f.gl, utils.NewPath(), entrypoint)
	if err != nil {
		return errors.Trace(err)
	}
	if preRunHandler != nil {
		if err := preRunHandler(); err != nil {
			return errors.Trace(err)
		}
	}
	if err := f.startExecutePlan(requestID, dr, params); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (f *flow) Close(ctx context.Context) error {
	if !f.running {
		return nil
	}

	f.cancel()
	f.running = false

	if f.exitCh != nil {
		<-f.exitCh
	}

	return f.batchRunner.stopWait(ctx)
}

func (f *flow) RunOnce() error {
	return f.runOnce()
}
