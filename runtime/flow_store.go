package runtime

import (
	"context"

	"github.com/warriorguo/workflow/types"
	"github.com/warriorguo/workflow/utils"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

func (f *flow) savePlan(ctx context.Context, requestID string, plan *dagExecutePlan) error {
	b, err := utils.Serialize(plan)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(f.store.Set(ctx, DAGPlanPath, requestID, b))
}

func (f *flow) removePlan(ctx context.Context, requestID string) error {
	return errors.Trace(f.store.Remove(ctx, DAGPlanPath, requestID))
}

func (f *flow) loadPlan(ctx context.Context, requestID string) (*dagExecutePlan, *flowRerunContext, error) {
	b, err := f.store.Get(ctx, DAGPlanPath, requestID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if b == nil {
		return nil, nil, errors.NotFoundf("DAG plan requestID: %s", requestID)
	}

	dag := &dagExecutePlan{}
	if err := utils.Unserialize(b, dag); err != nil {
		return nil, nil, errors.Trace(err)
	}

	b, err = f.store.Get(ctx, RunContextPath, requestID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	reRC := &flowRerunContext{}
	if b == nil {
		reRC = nil
	} else if err := utils.Unserialize(b, reRC); err != nil {
		return nil, nil, errors.Trace(err)
	}
	return dag, reRC, nil
}

func (f *flow) loadRecords(ctx context.Context, requestID string) (map[string]*types.NodeTraceRecord, error) {
	records := make(map[string]*types.NodeTraceRecord)
	recordPath := recordSavePath(requestID)
	err := f.store.List(ctx, recordPath, func(node string) bool {
		b, err := f.store.Get(ctx, recordPath, node)
		if err != nil {
			log.Errorf("load %s %s from store failed: %v", recordPath, node, err)
			return true
		}
		record := &types.NodeTraceRecord{}
		if err := utils.Unserialize(b, record); err != nil {
			log.Errorf("unserialize %s %s from store:%s failed: %v", recordPath, node, string(b), err)
			return true
		}
		records[node] = record
		return true
	})
	return records, errors.Trace(err)
}

func (f *flow) ReloadRequests(ctx context.Context) (map[string]error, error) {
	return f.reloadPlans(ctx)
}
