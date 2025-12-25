package types

type DAGStatus struct {
	Status        StatusType
	CurrentVertex string
	Result        Data
	Error         string
	NodeTraceData map[string]*NodeTraceRecord
}

type DAG interface {
	Node(vertex string, handler NodeHandler, options ...ExecutionOption) error
	SubDAG(vertex string, dagName string) error
	Condition(vertex, trueVertex, falseVertex string, handler BooleanHandler, options ...ExecutionOption) error
	Edge(from, to string) error
}

type DAGHandler func(dag DAG) error
