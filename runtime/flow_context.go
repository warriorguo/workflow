package runtime

import (
	"context"
	"strings"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/warriorguo/workflow/store"
	"github.com/warriorguo/workflow/types"
	"github.com/warriorguo/workflow/utils"
)

const (
	RecordPath = "/record/"
)

const (
	maxDepth = 32
)

var (
	_ types.Context = &flowContext{}
)

type flowContext struct {
	context.Context

	store store.Store

	requestID string

	depth       int
	executePath utils.Path
	rcRecord    *types.NodeTraceRecord
}

func recordSavePath(requestID string) string {
	return RecordPath + requestID
}

func newFlowContext(store store.Store, requestID string) *flowContext {
	return &flowContext{store: store, requestID: requestID}
}

func (f *flowContext) GetRequestID() string {
	return f.requestID
}

func (f *flowContext) GetCurrentVertex() string {
	return strings.Join(f.executePath, ".")
}

func (f *flowContext) startRecord(ctx context.Context, path utils.Path, input types.Data) {
	log.Debugf("running %v", path)

	f.executePath = utils.Path{}
	f.depth = 0

	f.rcRecord = &types.NodeTraceRecord{}
	f.rcRecord.Path = path
	f.rcRecord.StartTime = time.Now()
	f.rcRecord.Input = input
}

func (f *flowContext) endRecord(ctx context.Context, output types.Data, err error) {
	f.rcRecord.Vertex = f.executePath.Export()
	f.rcRecord.EndTime = time.Now()
	if err != nil {
		f.rcRecord.Error = errors.ErrorStack(err)
	}
	f.rcRecord.Output = output
	if err := f.saveRecord(ctx); err != nil {
		log.Errorf("%s failed to save record: %v", f.requestID, err)
	}
}

func (f *flowContext) saveRecord(ctx context.Context) error {
	b, err := utils.Serialize(f.rcRecord)
	if err != nil {
		return errors.Trace(err)
	}
	if err := f.store.Set(ctx, recordSavePath(f.requestID), strings.Join(f.rcRecord.Vertex, "."), b); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (f *flowContext) enterDAG(name string) {
	if f.depth++; f.depth > maxDepth {
		panic("unexpect depth")
	}
	f.executePath = f.executePath.AddString(name)
}

func (f *flowContext) enterNode(name string) {
	if f.depth++; f.depth > maxDepth {
		panic("unexpect depth")
	}
	f.executePath = f.executePath.AddString(name)
}

func (f *flowContext) exitDAG(name string) {
	f.depth--
}

func (f *flowContext) exitNode(name string) {
	f.depth--
}
