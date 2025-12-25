package types

import "context"

type FlowEngine interface {
	RegisterDAG(name string, handler DAGHandler) error
	GetDAG(name string) (DAG, bool)
	/**
	 * RenderDAG will return the DOT string that generate by the DAG given the name.
	 * the name is the same as RegisterDAG parameter.
	 * If error is not nil, then it indicates error happened.
	 */
	RenderDAG(name string) (string, error)

	ListDAGNames() ([]string, error)

	RunDAG(ctx context.Context, dagName string, requestID string, params Data) error

	GetRequestStatus(ctx context.Context, requestID string) (*RequestStatus, error)
	RenderRequestStatus(ctx context.Context, requestID string) (string, error)

	PauseRequest(ctx context.Context, requestID string) error
	ResumeRequest(ctx context.Context, requestID string) error
	TerminateRequest(ctx context.Context, requestID string) error
	/**
	 * close the flowengine, and left all ongoing requests Paused status
	 */
	Close(ctx context.Context) error
	/**
	 * caller self invoking RunOnce, FlowOption.AutoStart should be false.
	 */
	RunOnce() error
	/**
	 * ReloadRequests will load exists requests from store.
	 * Notice: if the request ID has been loaded(checked by whether request ID be found in loaded ones)
	 */
	ReloadRequests(ctx context.Context) (map[string]error, error)
}

type RequestStatus struct {
	Status    StatusType
	LastError string

	LastVertexRecord *NodeTraceRecord
}
