package postgres_test

import (
	"context"
	"fmt"
	"log"

	"github.com/warriorguo/workflow/runtime"
	"github.com/warriorguo/workflow/store/postgres"
	"github.com/warriorguo/workflow/types"
)

// Example_basicUsage demonstrates basic usage of PostgreSQL store
func Example_basicUsage() {
	// Create PostgreSQL store configuration
	config := postgres.DefaultConfig()
	config.Host = "localhost"
	config.Port = 5432
	config.User = "postgres"
	config.Password = "postgres"
	config.Database = "workflow"

	// Create the store
	store, err := postgres.NewPostgresStore(config)
	if err != nil {
		log.Fatal(err)
	}
	// Note: In production, the store should live for the lifetime of the application

	// Create workflow engine with PostgreSQL store
	flowOptions := types.NewFlowOptions()
	flowEngine := runtime.NewFlowEngine(store, flowOptions)

	// Define a simple workflow
	err = flowEngine.RegisterDAG("simple-workflow", func(dag types.DAG) error {
		dag.Node("start", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Starting workflow")
			input.Set("status", "started")
			return input, nil
		})

		dag.Node("process", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Processing workflow")
			input.Set("status", "processed")
			return input, nil
		})

		dag.Node("finish", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Finishing workflow")
			input.Set("status", "finished")
			return input, nil
		})

		dag.Edge("start", "process")
		dag.Edge("process", "finish")

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	// Run the workflow
	params := types.Data{}
	params.Set("input", "test data")

	err = flowEngine.RunDAG(context.Background(), "simple-workflow", "request-001", params)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Workflow started successfully")
}

// Example_withDSN demonstrates usage with DSN string
func Example_withDSN() {
	// Parse DSN string
	dsn := "host=localhost port=5432 user=postgres password=postgres dbname=workflow sslmode=disable"
	config, err := postgres.ParseDSN(dsn)
	if err != nil {
		log.Fatal(err)
	}

	// Create store with parsed config
	store, err := postgres.NewPostgresStore(config)
	if err != nil {
		log.Fatal(err)
	}

	// Use store with workflow engine
	flowOptions := types.NewFlowOptions()
	flowEngine := runtime.NewFlowEngine(store, flowOptions)

	fmt.Printf("Flow engine created with PostgreSQL store\n")

	// List all registered DAGs
	names, _ := flowEngine.ListDAGNames()
	fmt.Printf("Registered DAGs: %v\n", names)
}

// Example_workflowWithPersistence demonstrates workflow persistence and recovery
func Example_workflowWithPersistence() {
	config := postgres.DefaultConfig()
	store, err := postgres.NewPostgresStore(config)
	if err != nil {
		log.Fatal(err)
	}

	flowOptions := types.NewFlowOptions()
	flowEngine := runtime.NewFlowEngine(store, flowOptions)

	// Register a workflow
	err = flowEngine.RegisterDAG("pausable-workflow", func(dag types.DAG) error {
		dag.Node("step1", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Step 1 completed")
			return input, nil
		})

		dag.Node("step2", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Step 2 completed")
			return input, nil
		})

		dag.Node("step3", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Step 3 completed")
			return input, nil
		})

		dag.Edge("step1", "step2")
		dag.Edge("step2", "step3")

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	requestID := "request-pause-001"

	// Start workflow
	params := types.Data{}
	err = flowEngine.RunDAG(ctx, "pausable-workflow", requestID, params)
	if err != nil {
		log.Fatal(err)
	}

	// Pause the workflow
	err = flowEngine.PauseRequest(ctx, requestID)
	if err != nil {
		fmt.Printf("Pause request: %v\n", err)
	}

	// Later, in a new instance or after restart...
	// Reload workflows from PostgreSQL
	errors, err := flowEngine.ReloadRequests(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if len(errors) > 0 {
		fmt.Printf("Errors reloading: %v\n", errors)
	}

	// Resume the workflow
	err = flowEngine.ResumeRequest(ctx, requestID)
	if err != nil {
		fmt.Printf("Resume request: %v\n", err)
	}

	fmt.Println("Workflow can be paused and resumed with PostgreSQL persistence")
}

// Example_conditionalWorkflow demonstrates a workflow with conditional branches
func Example_conditionalWorkflow() {
	config := postgres.DefaultConfig()
	store, err := postgres.NewPostgresStore(config)
	if err != nil {
		log.Fatal(err)
	}

	flowOptions := types.NewFlowOptions()
	flowEngine := runtime.NewFlowEngine(store, flowOptions)

	// Register a conditional workflow
	err = flowEngine.RegisterDAG("conditional-workflow", func(dag types.DAG) error {
		dag.Node("init", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Initializing")
			input.Set("value", 100)
			return input, nil
		})

		dag.Node("process-high", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Processing high value")
			return input, nil
		})

		dag.Node("process-low", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Processing low value")
			return input, nil
		})

		dag.Node("finish", func(ctx types.Context, input types.Data) (types.Data, error) {
			fmt.Println("Workflow finished")
			return input, nil
		})

		// Conditional branch
		dag.Condition("check-value", "process-high", "process-low",
			func(ctx types.Context, input types.Data) (bool, error) {
				value, _ := input.GetInt("value")
				return value > 50, nil
			})

		dag.Edge("init", "check-value")
		dag.Edge("process-high", "finish")
		dag.Edge("process-low", "finish")

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	// Run the workflow
	ctx := context.Background()
	params := types.Data{}
	err = flowEngine.RunDAG(ctx, "conditional-workflow", "request-cond-001", params)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Conditional workflow with PostgreSQL persistence")
}
