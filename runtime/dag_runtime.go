package runtime

import (
	"github.com/warriorguo/workflow/types"
	"github.com/warriorguo/workflow/utils"
	"github.com/juju/errors"
)

var (
	nodeNotFound = errors.New("node not found")
)

type statusType int

const (
	fatal    statusType = -1
	normal   statusType = 0
	finished statusType = 1
)

type dagRuntime struct {
	name string
	path utils.Path

	runningRC  runContext
	nextRC     runContext
	nextVertex string
}

func newDAGRuntime(path utils.Path) *dagRuntime {
	dag := &dagRuntime{}
	dag.path = path
	return dag
}

func (d *dagRuntime) getPath() utils.Path {
	return d.runningRC.getPath()
}

func (d *dagRuntime) runOnce(fc *flowContext, input types.Data) (runContext, types.Data, error) {
	fc.enterDAG(d.name)
	defer fc.exitDAG(d.name)

	return d.runRC(fc, input)
}

func (d *dagRuntime) runRC(fc *flowContext, input types.Data) (runContext, types.Data, error) {
	nextRC, data, err := d.runningRC.runOnce(fc, input)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if nextRC == Termination {
		return d.nextRC, data, nil
	}
	d.runningRC = nextRC
	return d, data, nil
}
