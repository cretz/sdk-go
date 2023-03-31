package workflowfuzzer

import (
	"fmt"
	"math"
	"math/rand"

	"go.temporal.io/api/command/v1"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/internal"
	"go.temporal.io/sdk/internal/log"
	"go.temporal.io/sdk/workflow"
)

type FuzzWorkflowOptions struct {
	Workflow     any
	WorkflowType string
	WorkflowArgs []any
	WorkflowID   string

	// If an activity is not present here, it uses DefaultActivityCompleter.
	ActivityCompleters map[string]func(*FuzzRun, *command.ScheduleActivityTaskCommandAttributes)
	// If an activity is not present here, it uses DefaultActivityCanceller.
	ActivityCancellers map[string]func(*FuzzRun, *command.RequestCancelActivityTaskCommandAttributes)
	// Called when a signal of given name is asked to send. This may be called
	// many times for a single signal. By default, no signals are sent to the
	// workflow during fuzzing.
	SignalSenders map[string]func(*FuzzRun)

	// General worker options that affect the workflow
	Namespace          string
	DataConverter      converter.DataConverter
	FailureConverter   converter.FailureConverter
	ContextPropagators []workflow.ContextPropagator
	Interceptors       []interceptor.WorkerInterceptor

	// If unset or any fields unset that are needed, they will be set. This will
	// be mutated when starting.
	StartedEvent *history.WorkflowExecutionStartedEventAttributes

	// Advanced, probably do not need to use. Defaults to NewDefaultHandler.
	Handler Handler
}

func FuzzWorkflow(seed int64, options FuzzWorkflowOptions) error {
	run := &FuzzRun{
		FuzzWorkflowOptions: options,
		History:             &history.History{},
		State:               map[string]any{},
		registry:            internal.NewRegistry(),
		rand:                rand.New(rand.NewSource(seed)),
	}
	// Set default options
	if run.Workflow == nil {
		return fmt.Errorf("Workflow must be present")
	} else if run.WorkflowType == "" {
		run.registry.RegisterWorkflow(run.Workflow)
		run.WorkflowType, _ = internal.GetFunctionName(run.Workflow)
	} else {
		run.registry.RegisterWorkflowWithOptions(run.Workflow, workflow.RegisterOptions{Name: run.WorkflowType})
	}
	if run.WorkflowID == "" {
		run.WorkflowID = "fuzz-workflow-id"
	}
	if run.Namespace == "" {
		run.Namespace = "fuzz-namespace"
	}
	if run.DataConverter == nil {
		run.DataConverter = converter.GetDefaultDataConverter()
	}
	if run.StartedEvent == nil {
		run.StartedEvent = &history.WorkflowExecutionStartedEventAttributes{}
	}
	if run.StartedEvent.WorkflowType == nil {
		run.StartedEvent.WorkflowType = &common.WorkflowType{Name: run.WorkflowType}
	}
	if run.StartedEvent.TaskQueue == nil {
		run.StartedEvent.TaskQueue = &taskqueue.TaskQueue{Name: "FuzzTaskQueue"}
	}
	if run.StartedEvent.OriginalExecutionRunId == "" {
		run.StartedEvent.OriginalExecutionRunId = "fuzz-run-id"
	}
	if run.StartedEvent.FirstExecutionRunId == "" {
		run.StartedEvent.FirstExecutionRunId = run.StartedEvent.OriginalExecutionRunId
	}
	if run.StartedEvent.Attempt == 0 {
		run.StartedEvent.Attempt = 1
	}
	if len(run.WorkflowArgs) > 0 {
		var err error
		if run.StartedEvent.Input, err = run.DataConverter.ToPayloads(run.WorkflowArgs...); err != nil {
			return fmt.Errorf("failed converting input: %w", err)
		}
	}
	if err := run.run(); err != nil {
		return ErrFuzzRun{error: err, FuzzRun: run}
	}
	return nil
}

type ErrFuzzRun struct {
	error
	*FuzzRun
}

type FuzzRun struct {
	FuzzWorkflowOptions
	// This is the full history, not necessarily the current task
	History               *history.History
	State                 map[string]any
	CurrentPageStartIndex int
	LastTaskFailure       *workflowservice.RespondWorkflowTaskFailedRequest

	registry *internal.Registry
	rand     *rand.Rand
}

func (f *FuzzRun) run(args ...any) error {
	// Create task handler
	handler := internal.NewWorkflowTaskHandlerWithAllCapabilities(
		internal.WorkerExecutionParameters{
			Namespace:          f.Namespace,
			TaskQueue:          f.StartedEvent.TaskQueue.Name,
			Identity:           "fuzz-worker-id",
			Logger:             log.NewNopLogger(),
			DataConverter:      f.DataConverter,
			FailureConverter:   f.FailureConverter,
			ContextPropagators: f.ContextPropagators,
		},
		f.registry,
	)

	// Mark started
	f.Handler.OnStart()
	for {
		// Prepare the task response
		history := &history.History{Events: f.History.Events[f.CurrentPageStartIndex:]}
		resp, _, err := handler.ProcessWorkflowTask(
			internal.NewWorkflowTask(
				// TODO(cretz): Sometimes add a query
				&workflowservice.PollWorkflowTaskQueueResponse{
					Attempt:      1,
					TaskToken:    []byte("ReplayTaskToken"),
					WorkflowType: f.StartedEvent.WorkflowType,
					WorkflowExecution: &common.WorkflowExecution{
						WorkflowId: f.WorkflowID,
						RunId:      f.StartedEvent.OriginalExecutionRunId,
					},
					History:                history,
					PreviousStartedEventId: math.MaxInt64,
					// Messages:               inferMessages(history.Events),
				},
				&internal.FixedHistoryIterator{History: history},
			),
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed processing task: %w", err)
		} else if taskFail, _ := resp.(*workflowservice.RespondWorkflowTaskFailedRequest); taskFail != nil {
			f.LastTaskFailure = taskFail
			// TODO(cretz): Convert this error?
			return fmt.Errorf("failed processing task: %v", taskFail.GetFailure().GetMessage())
		} else if _, queryResp := resp.(*workflowservice.RespondQueryTaskCompletedRequest); queryResp {
			// Ignore query response
			// TODO(cretz): What to do with query fail
			continue
		}
		// On task complete we process each command
		taskComplete := resp.(*workflowservice.RespondWorkflowTaskCompletedRequest)
		// TODO(cretz): OnCommand
	}

}

func (f *FuzzRun) PermBool() bool { return f.PermIntn(2) == 0 }

func (f *FuzzRun) PermIntn(n int) int { return f.rand.Intn(n) }

func (*FuzzRun) EventByID(eventID int64) *history.HistoryEvent {
	panic("TODO")
}

// Event ID is automatically added
func (*FuzzRun) AddHistoryEvent(*history.HistoryEvent) {
	// TODO(cretz): Sometimes add a task boundary
	// TODO(cretz): Sometimes add signal
	panic("TODO")
}

// func (*FuzzRun) AddHistoryEventWithoutPossibleTaskBoundary(*history.HistoryEvent) {
// 	panic("TODO")
// }

// Empty string means no group ID. Two calls on the same group ID will be
// mutually exclusive with one another when reached. Adding a new call after the
// previous one of the same group ID has already been executed is a no-op.
func (*FuzzRun) AddMaybeInFuture(mutexGroupID string, f func()) {
	panic("TODO")
}

func (*FuzzRun) AddTaskBoundary() {
	panic("TODO")
}

func (*FuzzRun) AddActivityScheduled(cmd *command.ScheduleActivityTaskCommandAttributes) *history.HistoryEvent {
	panic("TODO")
}

func (*FuzzRun) AddActivityStarted(cmd *command.ScheduleActivityTaskCommandAttributes) *history.HistoryEvent {
	panic("TODO")
}

func (*FuzzRun) AddActivitySucceeded(cmd *command.ScheduleActivityTaskCommandAttributes, value any) *history.HistoryEvent {
	panic("TODO")
}

func (*FuzzRun) AddActivityFailed(cmd *command.ScheduleActivityTaskCommandAttributes, err error) *history.HistoryEvent {
	panic("TODO")
}

func (*FuzzRun) AddActivityTimedOut(cmd *command.ScheduleActivityTaskCommandAttributes, err error) *history.HistoryEvent {
	panic("TODO")
}

func (*FuzzRun) AddActivityCancelRequested(cmd *command.RequestCancelActivityTaskCommandAttributes) *history.HistoryEvent {
	panic("TODO")
}

func (*FuzzRun) AddActivityCancelled(cmd *command.RequestCancelActivityTaskCommandAttributes, details ...any) *history.HistoryEvent {
	panic("TODO")
}
