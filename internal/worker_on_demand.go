package internal

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/api/workflowservice/v1"
)

type OnDemandWorkflowWorkerOptions struct {
	// Required task queue.
	TaskQueue string

	// All workflows. At least one required.
	Workflows []RegisteredWorkflow

	// All local activities.
	LocalActivities []RegisteredActivity

	// Required client options
	ClientOptions ClientOptions

	// The following options are implied:
	//   * MaxConcurrentWorkflowTaskPollers is always 1
	//   * EnableSessionWorker is always false (sessions not supported)
	//   * LocalActivityWorkerOnly is always true
	//   * DisableEagerActivities is always true
	WorkerOptions WorkerOptions
}

type RegisteredWorkflow struct {
	Workflow any
	Options  RegisterWorkflowOptions
}

type RegisteredActivity struct {
	Activity any
	Options  RegisterActivityOptions
}

type OnDemandWorkflowWorker struct {
	options    OnDemandWorkflowWorkerOptions
	rootClient Client

	loadedCapabilitiesWhenUnset     *workflowservice.GetSystemInfoResponse_Capabilities
	loadedCapabilitiesWhenUnsetLock sync.RWMutex
}

// Callers should:
//   - Call SetBinaryChecksum with a unique instance value if there is one
func NewOnDemandWorkflowWorker(options OnDemandWorkflowWorkerOptions) (*OnDemandWorkflowWorker, error) {
	if options.TaskQueue == "" {
		return nil, fmt.Errorf("missing task queue")
	} else if len(options.Workflows) == 0 {
		return nil, fmt.Errorf("at least one workflow required")
	}
	rootClient, err := NewLazyClient(options.ClientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed creating client: %w", err)
	}
	// We intentionally set the root client's server capabilities as an empty
	// struct so that they won't attempt to load on NewClientFromExisting.
	rootClient.(*WorkflowClient).capabilitiesLock.Lock()
	rootClient.(*WorkflowClient).capabilities = &workflowservice.GetSystemInfoResponse_Capabilities{}
	rootClient.(*WorkflowClient).capabilitiesLock.Unlock()
	return &OnDemandWorkflowWorker{options: options, rootClient: rootClient}, nil
}

func (o *OnDemandWorkflowWorker) newClient(
	capabilities *workflowservice.GetSystemInfoResponse_Capabilities,
) (Client, error) {
	// Create a new client from the root, and replace the capabilities if new set
	// is present. Otherwise, get or load capabilities ourselves.
	client, err := NewClientFromExisting(o.rootClient, o.options.ClientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed creating client: %w", err)
	}
	// Override client capabilities. Even if the given capabilities are nil, we
	// want to set nil here so later if we have to call loadCapabilities, it will
	// do what is needed.
	client.(*WorkflowClient).capabilitiesLock.Lock()
	client.(*WorkflowClient).capabilities = capabilities
	client.(*WorkflowClient).capabilitiesLock.Unlock()
	// Get or load capabilities when unset
	if capabilities == nil {
		o.loadedCapabilitiesWhenUnsetLock.RLock()
		capabilities = o.loadedCapabilitiesWhenUnset
		o.loadedCapabilitiesWhenUnsetLock.RUnlock()
		// If never initialized, we load _out of lock_ (we're ok duplicating work
		// in racy situations)
		if capabilities == nil {
			if capabilities, err = client.(*WorkflowClient).loadCapabilities(); err != nil {
				client.Close()
				return nil, fmt.Errorf("failed loading capabilities from server: %w", err)
			}
			// Store for the next use
			o.loadedCapabilitiesWhenUnsetLock.Lock()
			o.loadedCapabilitiesWhenUnset = capabilities
			o.loadedCapabilitiesWhenUnsetLock.Unlock()
		}
		// Set on the client
		client.(*WorkflowClient).capabilitiesLock.Lock()
		client.(*WorkflowClient).capabilities = capabilities
		client.(*WorkflowClient).capabilitiesLock.Unlock()
	}
	return client, nil
}

type OnDemandWorkflowWorkerExecuteOptions struct {
	// Server capabilities. If unset, a server call may be made to get this.
	ServerCapabilities *workflowservice.GetSystemInfoResponse_Capabilities
}

func (o *OnDemandWorkflowWorker) ExecuteSingleRun(
	ctx context.Context,
	options OnDemandWorkflowWorkerExecuteOptions,
) error {
	// Create client for this call
	client, err := o.newClient(options.ServerCapabilities)
	if err != nil {
		return err
	}
	defer client.Close()

	// Set implied options
	singleRunCtx, singleRunStop := context.WithCancel(context.Background())
	defer singleRunStop()
	workerOptions := o.options.WorkerOptions
	workerOptions.MaxConcurrentWorkflowTaskPollers = 1
	workerOptions.EnableSessionWorker = false
	workerOptions.LocalActivityWorkerOnly = true
	workerOptions.DisableEagerActivities = true
	workerOptions.singleRunMode = true
	workerOptions.singleRunStop = singleRunStop

	// Create worker and register workflows and activities
	worker := NewWorker(client, o.options.TaskQueue, workerOptions)
	for _, workflow := range o.options.Workflows {
		worker.RegisterWorkflowWithOptions(workflow.Workflow, workflow.Options)
	}
	for _, activity := range o.options.LocalActivities {
		worker.RegisterActivityWithOptions(activity.Activity, activity.Options)
	}

	// Build interrupt channel from context done
	interruptCh := make(chan any, 1)
	go func() {
		select {
		case <-ctx.Done():
		case <-singleRunCtx.Done():
		}
		interruptCh <- struct{}{}
	}()

	// Just run and return error, rest is taken care of inside worker
	err = worker.Run(interruptCh)
	// Use context error as error if interrupted for that reason
	if err != nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	return err
}

func (o *OnDemandWorkflowWorker) Close() { o.rootClient.Close() }
