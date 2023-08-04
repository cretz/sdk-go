package invertedworker_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker/invertedworker"
	"go.temporal.io/sdk/workflow"
)

/*TODO(cretz): Tests needed:

* Workflow sticky enabled but worker restart
* Durable store and no sticky
* General worker crash/restart
 */

func SimpleActivity(ctx context.Context, str1, str2 string) (string, error) {
	return str1 + str2, nil
}

func SimpleWorkflow(ctx workflow.Context, str1, str2 string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var ret string
	err := workflow.ExecuteActivity(ctx, SimpleActivity, str1, str2).Get(ctx, &ret)
	return ret, err
}

func TestInvertedWorkerSimpleWorkflow(t *testing.T) {
	h := newHarness(t)
	defer h.Close()

	w := h.startInvertedWorker(invertedworker.Options{
		TaskQueue:  "my-task-queue",
		Workflows:  []*invertedworker.Workflow{{Definition: SimpleWorkflow}},
		Activities: []*invertedworker.Activity{{Definition: SimpleActivity}},
	})
	defer w.Close()

	h.startInverter("my-task-queue", w)

	run, err := h.server.Client().ExecuteWorkflow(
		h.ctx,
		client.StartWorkflowOptions{TaskQueue: "my-task-queue"},
		SimpleWorkflow,
		"Hello, ", "World!",
	)
	h.NoError(err)
	var ret string
	h.NoError(run.Get(h.ctx, &ret))
	h.Equal("Hello, World!", ret)
}

type harness struct {
	*require.Assertions
	ctx    context.Context
	cancel context.CancelFunc
	server *testsuite.DevServer

	runningInverters map[string]bool
}

func newHarness(t *testing.T) *harness {
	h := &harness{
		Assertions:       require.New(t),
		runningInverters: map[string]bool{},
	}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	var err error
	h.server, err = testsuite.StartDevServer(h.ctx, testsuite.DevServerOptions{LogLevel: "warn"})
	h.NoError(err)
	return h
}

// This sets system info and client port and such. Caller is expected to close
// this when done.
func (h *harness) startInvertedWorker(options invertedworker.Options) *invertedworker.InvertedWorker {
	var err error
	options.SystemInfo, err = h.server.Client().WorkflowService().GetSystemInfo(
		h.ctx, &workflowservice.GetSystemInfoRequest{})
	h.NoError(err)
	options.Client.HostPort = h.server.FrontendHostPort()
	if options.Worker.OnFatalError == nil {
		options.Worker.OnFatalError = func(err error) {
			log.Printf("Worker fatal error: %v", err)
		}
	}
	worker, err := invertedworker.New(options)
	h.NoError(err)
	return worker
}

func (h *harness) startInverter(taskQueue string, worker *invertedworker.InvertedWorker) {
	// First make sure there isn't already an inverter for this queue
	h.False(h.runningInverters[taskQueue])
	h.runningInverters[taskQueue] = true

	// Build options
	inverterErrCh := make(chan error, 2)
	options := invertedworker.InverterOptions{
		Client: client.Options{HostPort: h.server.FrontendHostPort()},
		// Just accept all jobs immediately and handle sync
		WorkflowJobHandler: func(
			ctx context.Context,
			job *workflowservice.PollWorkflowTaskQueueResponse,
			callback func(*invertedworker.WorkflowJobResponse),
		) error {
			resp, err := worker.HandleWorkflowJob(ctx, job)
			if err == nil {
				callback(resp)
			}
			return err
		},
		ActivityJobHandler: func(
			ctx context.Context,
			job *workflowservice.PollActivityTaskQueueResponse,
			callback func(*invertedworker.ActivityJobResponse),
		) error {
			resp, err := worker.HandleActivityJob(ctx, job)
			if err == nil {
				callback(resp)
			}
			return err
		},
		TaskQueue: taskQueue,
		OnStarted: func() { inverterErrCh <- nil },
	}

	// Start inverter
	go func() {
		err := invertedworker.RunInverter(h.ctx, options)
		if err != nil {
			log.Printf("Inverter error: %v", err)
			inverterErrCh <- err
		}
	}()

	// Wait for inverter start
	h.NoError(<-inverterErrCh)
}

func (h *harness) Close() {
	h.cancel()
	h.server.Stop()
}
