package types

import (
	"context"
)

type StatusType int32

const (
	None     StatusType = 0
	Pending  StatusType = 1
	Running  StatusType = 2
	Paused   StatusType = 3
	Retrying StatusType = 4
	Failed   StatusType = 5
	Fatal    StatusType = 9
	Finished StatusType = 10
)

type Version string
type Context interface {
	context.Context

	GetRequestID() string
}
