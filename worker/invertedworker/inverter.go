package invertedworker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/internal/log"
)

type InverterOptions struct {
	Client             client.Options
	WorkflowJobHandler func(
		context.Context,
		*workflowservice.PollWorkflowTaskQueueResponse,
		// Can be called inline
		func(*WorkflowJobResponse),
	) error
	ActivityJobHandler func(
		context.Context,
		*workflowservice.PollActivityTaskQueueResponse,
		// Can be called inline
		func(*ActivityJobResponse),
	) error
	TaskQueue                 string
	WorkerVersionCapabilities *common.WorkerVersionCapabilities
	OnStarted                 func()
}

// Always returns error, but may be context error. This is for testing only.
func RunInverter(ctx context.Context, options InverterOptions) error {
	if options.TaskQueue == "" {
		return fmt.Errorf("task queue required")
	} else if options.WorkflowJobHandler == nil && options.ActivityJobHandler == nil {
		return fmt.Errorf("both workflow and activity job handlers are missing")
	}

	// Default client identity
	if options.Client.Identity == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		options.Client.Identity = fmt.Sprintf("%d@%s@%s", os.Getpid(), hostname, options.TaskQueue)
	}
	if options.Client.Namespace == "" {
		options.Client.Namespace = "default"
	}
	if options.Client.Logger == nil {
		options.Client.Logger = log.NewDefaultLogger()
	}

	// Connect client and get system info
	i := &inverter{options: &options}
	var err error
	if i.client, err = client.Dial(options.Client); err != nil {
		return fmt.Errorf("failed creating client: %w", err)
	}
	defer i.client.Close()
	i.systemInfo, err = i.client.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	if err != nil {
		return fmt.Errorf("failed getting system info: %w", err)
	}

	return i.run(ctx)
}

type inverter struct {
	ctx        context.Context
	options    *InverterOptions
	client     client.Client
	systemInfo *workflowservice.GetSystemInfoResponse
	errCh      chan error

	pollErrCount int
	pollErrLast  time.Time
	pollErrLock  sync.Mutex

	seenStickyQueue     *taskqueue.TaskQueue
	seenStickyQueueLock sync.RWMutex
}

func (i *inverter) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	i.ctx = ctx

	// Run each. Give a channel buffer of 1 just in case an error is sent before
	// we start listening below
	i.errCh = make(chan error, 1)
	if i.options.WorkflowJobHandler != nil {
		i.options.Client.Logger.Info("Starting workflow poller")
		go i.runWorkflowPoller()
	}
	if i.options.ActivityJobHandler != nil {
		i.options.Client.Logger.Info("Starting activity poller")
		go i.runActivityPoller()
	}

	if i.options.OnStarted != nil {
		i.options.OnStarted()
	}

	// Wait until done or error
	var err error
	select {
	case <-ctx.Done():
	case err = <-i.errCh:
	}

	// If context complete, use that error always even if inverter failed
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return err
}

func (i *inverter) pollErr(err error) {
	// If we haven't had any errors in the last 30 seconds, reset. Otherwise
	// increment count and update time, and fatal if over a certain count.
	// TODO(cretz): This is an intentionally naive/lazy impl for POC. A proper
	// impl would have exponential backoff, non-retryable, etc
	now := time.Now()
	timeAgo := now.Add(-30 * time.Second)
	i.pollErrLock.Lock()
	if i.pollErrLast.Before(timeAgo) {
		i.pollErrCount = 1
	} else {
		i.pollErrCount++
	}
	i.pollErrLast = now
	pollErrCount := i.pollErrCount
	i.pollErrLock.Unlock()

	// If it's over the threshold, break
	i.options.Client.Logger.Error("Inverter got error", "Error", err)
	if pollErrCount > 10 {
		i.options.Client.Logger.Error("Got enough inverter errors, failing inverter")
		// Just try to set on channel if room
		select {
		case i.errCh <- err:
		default:
		}
	} else {
		// Otherwise sleep
		// TODO(cretz): Yes this is naive and may not be sleeping where user
		// expects...this is a POC impl
		time.Sleep(2 * time.Second)
	}
}

func (i *inverter) runWorkflowPoller() {
	i.runWorkflowPollerForTaskQueue(&taskqueue.TaskQueue{
		Name: i.options.TaskQueue,
		Kind: enums.TASK_QUEUE_KIND_NORMAL,
	})
}

func (i *inverter) runWorkflowPollerForTaskQueue(taskQueue *taskqueue.TaskQueue) {
	// Poll in a loop. Handler can apply backpressure as it chooses.
	req := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:                 i.options.Client.Namespace,
		TaskQueue:                 taskQueue,
		Identity:                  i.options.Client.Identity,
		WorkerVersionCapabilities: i.options.WorkerVersionCapabilities,
	}
	for {
		ctx, cancel := context.WithTimeout(i.ctx, 70*time.Second)
		resp, err := i.client.WorkflowService().PollWorkflowTaskQueue(ctx, req)
		cancel()
		if err != nil {
			i.pollErr(err)
			return
		}
		i.options.Client.Logger.Debug(
			"Got workflow job",
			"TaskQueue", taskQueue,
			"WorkflowID", resp.WorkflowExecution.WorkflowId,
			"RunID", resp.WorkflowExecution.RunId,
			"TaskToken", resp.TaskToken,
		)
		err = i.options.WorkflowJobHandler(i.ctx, resp, i.handleWorkflowResponse)
		if err != nil {
			i.pollErr(err)
			return
		}
	}
}

func (i *inverter) handleWorkflowResponse(resp *WorkflowJobResponse) {
	var err error
	switch {
	case resp.Completed != nil:
		i.options.Client.Logger.Debug("Got workflow job completed", "TaskToken", resp.Completed.TaskToken)
		// If the response has a sticky task queue, it must match the known one or
		// if this is the first we've seen, start the poller for that one too
		if stickyQueue := resp.Completed.StickyAttributes.GetWorkerTaskQueue(); stickyQueue != nil {
			i.seenStickyQueueLock.RLock()
			seenStickyQueue := i.seenStickyQueue
			i.seenStickyQueueLock.RUnlock()
			if seenStickyQueue == nil {
				// Write again under lock and start poller if it wasn't set
				i.seenStickyQueueLock.Lock()
				if i.seenStickyQueue == nil {
					i.seenStickyQueue = stickyQueue
					go i.runWorkflowPollerForTaskQueue(stickyQueue)
				}
				seenStickyQueue = i.seenStickyQueue
				i.seenStickyQueueLock.Unlock()
			}
			if seenStickyQueue.Name != stickyQueue.Name {
				err = fmt.Errorf("sticky queue %v returned but already polling for %v", stickyQueue.Name, seenStickyQueue.Name)
			}
		}

		// Try to send
		if resp.Failed != nil {
			err = fmt.Errorf("response had both completed and failed")
		} else if r, rErr := i.client.WorkflowService().RespondWorkflowTaskCompleted(i.ctx, resp.Completed); rErr != nil {
			err = rErr
		} else if r.WorkflowTask != nil {
			err = fmt.Errorf("inverter does not support creating new workflow tasks")
		} else if len(r.ActivityTasks) != 0 {
			err = fmt.Errorf("inverter does not support eager activity execution")
		}
	case resp.Failed != nil:
		i.options.Client.Logger.Debug("Got workflow job failed", "TaskToken", resp.Failed.TaskToken)
		_, err = i.client.WorkflowService().RespondWorkflowTaskFailed(i.ctx, resp.Failed)
	default:
		err = fmt.Errorf("neither completed nor failed given")
	}
	if err != nil {
		i.pollErr(err)
	}
}

func (i *inverter) runActivityPoller() {
	// Poll in a loop. Handler can apply backpressure as it chooses.
	req := &workflowservice.PollActivityTaskQueueRequest{
		Namespace: i.options.Client.Namespace,
		TaskQueue: &taskqueue.TaskQueue{
			Name: i.options.TaskQueue,
			Kind: enums.TASK_QUEUE_KIND_NORMAL,
		},
		Identity:                  i.options.Client.Identity,
		WorkerVersionCapabilities: i.options.WorkerVersionCapabilities,
	}
	for {
		ctx, cancel := context.WithTimeout(i.ctx, 70*time.Second)
		resp, err := i.client.WorkflowService().PollActivityTaskQueue(ctx, req)
		cancel()
		if err != nil {
			i.pollErr(err)
			return
		}
		i.options.Client.Logger.Debug(
			"Got activity job",
			"TaskQueue", i.options.TaskQueue,
			"WorkflowID", resp.WorkflowExecution.WorkflowId,
			"RunID", resp.WorkflowExecution.RunId,
			"TaskToken", resp.TaskToken,
		)
		err = i.options.ActivityJobHandler(i.ctx, resp, i.handleActivityResponse)
		if err != nil {
			i.pollErr(err)
			return
		}
	}
}

func (i *inverter) handleActivityResponse(resp *ActivityJobResponse) {
	var err error
	switch {
	case resp.Completed != nil:
		i.options.Client.Logger.Debug("Got activity job completed", "TaskToken", resp.Completed.TaskToken)
		// We intentionally are not overwriting identity or task token or any of
		// that. We count on handler to do its part.
		if resp.Failed != nil || resp.Canceled != nil {
			err = fmt.Errorf("response had both completed and failed/canceled")
		} else {
			_, err = i.client.WorkflowService().RespondActivityTaskCompleted(i.ctx, resp.Completed)
		}
	case resp.Failed != nil:
		i.options.Client.Logger.Debug("Got activity job failed", "TaskToken", resp.Failed.TaskToken)
		if resp.Canceled != nil {
			err = fmt.Errorf("response had both failed and canceled")
		} else {
			// TODO(cretz): Log the failure to fail which comes on success response of
			// this gRPC call
			_, err = i.client.WorkflowService().RespondActivityTaskFailed(i.ctx, resp.Failed)
		}
	case resp.Canceled != nil:
		i.options.Client.Logger.Debug("Got activity job canceled", "TaskToken", resp.Canceled.TaskToken)
		_, err = i.client.WorkflowService().RespondActivityTaskCanceled(i.ctx, resp.Canceled)
	default:
		err = fmt.Errorf("neither completed nor failed not canceled given")
	}
	if err != nil {
		i.pollErr(err)
	}
}
