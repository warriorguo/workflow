package types

import "time"

type NodeTraceRecord struct {
	Path []string
	// vertex maybe a DAG, so it would have sub vertex
	Vertex    []string
	StartTime time.Time
	EndTime   time.Time
	Error     string
	Input     Data
	Output    Data
}

type NodeRuntimeData struct {
	Vertex         string
	CurrentRunning int32
	SuccessTimes   int64
	FailedTimes    int64
	// leave the performance data to promethues
	// since for it's hard to record the histogram data
}

type NodeHandler func(ctx Context, input Data) (Data, error)
type BooleanHandler func(ctx Context, input Data) (bool, error)
