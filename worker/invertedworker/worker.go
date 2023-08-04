package invertedworker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/internal"
	internalcommon "go.temporal.io/sdk/internal/common"
	internallog "go.temporal.io/sdk/internal/log"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
)

const (
	workflowJobExpiry       = 1 * time.Minute
	activityJobExpiryLeeway = 1 * time.Minute
	staleJobPurgeFreq       = 1 * time.Minute
)

var errWorkerClosed = errors.New("worker closed")
var errJobExpired = errors.New("job expired")

type Options struct {
	TaskQueue  string
	SystemInfo *workflowservice.GetSystemInfoResponse
	Client     client.Options
	// Users should set OnFatalError here
	Worker worker.Options
	// If using a durable store and it is unlikely this process will be reused for
	// existing workflows, consider calling worker.SetStickyWorkflowCacheSize(0)
	// before creating any workers so the durable store will always be used to
	// rebuild history and memory won't be used to cache unnecessarily.
	DurableStore DurableStore
	Workflows    []*Workflow
	Activities   []*Activity
}

type Workflow struct {
	Definition interface{}
	Options    workflow.RegisterOptions
}

type Activity struct {
	Definition interface{}
	Options    activity.RegisterOptions
}

type InvertedWorker struct {
	systemInfo *workflowservice.GetSystemInfoResponse
	realClient client.Client
	worker     worker.Worker

	log    log.Logger
	stopCh chan struct{}

	pendingWorkflowJobs     chan *pendingWorkflowJob
	runningWorkflowJobs     map[string]*pendingWorkflowJob
	runningWorkflowJobsLock sync.Mutex

	pendingActivityJobs     chan *pendingActivityJob
	runningActivityJobs     map[string]*pendingActivityJob
	runningActivityJobsLock sync.Mutex
}

func New(options Options) (*InvertedWorker, error) {
	if options.TaskQueue == "" {
		return nil, fmt.Errorf("task queue required")
	} else if options.SystemInfo == nil {
		return nil, fmt.Errorf("system info required")
	} else if len(options.Workflows) == 0 && len(options.Activities) == 0 {
		return nil, fmt.Errorf("must have at least one workflow and at least one activity")
	}

	i := &InvertedWorker{
		log:                 options.Client.Logger,
		stopCh:              make(chan struct{}),
		pendingWorkflowJobs: make(chan *pendingWorkflowJob),
		runningWorkflowJobs: make(map[string]*pendingWorkflowJob),
		pendingActivityJobs: make(chan *pendingActivityJob),
		runningActivityJobs: make(map[string]*pendingActivityJob),
	}
	if i.log == nil {
		i.log = internallog.NewDefaultLogger()
	}
	success := false

	// Create real client w/ options
	var err error
	if i.realClient, err = client.NewLazyClient(options.Client); err != nil {
		return nil, fmt.Errorf("failed creating client: %w", err)
	}
	defer func() {
		if !success {
			i.realClient.Close()
		}
	}()

	// Override service and disable dialing in options then create wrapped client
	internal.OverrideWorkflowService(&options.Client, newWorkflowService(i))
	options.Client.ConnectionOptions.DialOptions = append(
		options.Client.ConnectionOptions.DialOptions,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return nil, fmt.Errorf("dialing disabled")
		}),
	)
	wrappedClient, err := client.NewLazyClient(options.Client)
	if err != nil {
		return nil, fmt.Errorf("failed creating wrapped client: %w", err)
	}

	// Fix worker options
	// TODO(cretz): How to handle max concurrent? There is no backpressure built
	// into the handle calls. We need to expose slot reservation.
	// TODO(cretz): Ensure create-new-workflow-task is disabled?
	options.Worker.DisableWorkflowWorker = len(options.Workflows) == 0
	options.Worker.LocalActivityWorkerOnly = len(options.Activities) == 0
	options.Worker.DisableEagerActivities = true
	options.Worker.DisableRegistrationAliasing = true

	// Create worker
	i.worker = worker.New(wrappedClient, options.TaskQueue, options.Worker)
	for _, workflow := range options.Workflows {
		i.worker.RegisterWorkflowWithOptions(workflow.Definition, workflow.Options)
	}
	for _, activity := range options.Activities {
		i.worker.RegisterActivityWithOptions(activity.Definition, activity.Options)
	}

	// Start worker and stop it on channel close
	if err := i.worker.Start(); err != nil {
		return nil, fmt.Errorf("failed starting worker: %w", err)
	}
	go func() {
		<-i.stopCh
		i.worker.Stop()
	}()

	// Run job purger in background and return
	go i.runJobPurger()
	success = true
	return i, nil
}

// All fields here are mutually exclusive
type WorkflowJobResponse struct {
	// If this has sticky polling, it must always use the same sticky queue name
	Completed *workflowservice.RespondWorkflowTaskCompletedRequest
	Failed    *workflowservice.RespondWorkflowTaskFailedRequest
}

func (i *InvertedWorker) HandleWorkflowJob(
	ctx context.Context,
	job *workflowservice.PollWorkflowTaskQueueResponse,
) (*WorkflowJobResponse, error) {
	// Send pending job
	respCh := make(chan *WorkflowJobResponse)
	errCh := make(chan error)
	select {
	case <-i.stopCh:
		return nil, errWorkerClosed
	case i.pendingWorkflowJobs <- &pendingWorkflowJob{
		key:          string(job.TaskToken),
		job:          job,
		expiresAfter: time.Now().Add(workflowJobExpiry),
		respCh:       respCh,
		errCh:        errCh,
	}:
	}

	// Wait for result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-i.stopCh:
		return nil, errWorkerClosed
	case err := <-errCh:
		return nil, err
	case resp := <-respCh:
		return resp, nil
	}
}

type ActivityJobResponse struct {
	Completed *workflowservice.RespondActivityTaskCompletedRequest
	Failed    *workflowservice.RespondActivityTaskFailedRequest
	Canceled  *workflowservice.RespondActivityTaskCanceledRequest
}

func (i *InvertedWorker) HandleActivityJob(
	ctx context.Context,
	job *workflowservice.PollActivityTaskQueueResponse,
) (*ActivityJobResponse, error) {
	// Send pending job
	respCh := make(chan *ActivityJobResponse)
	errCh := make(chan error)
	select {
	case <-i.stopCh:
		return nil, errWorkerClosed
	case i.pendingActivityJobs <- &pendingActivityJob{
		key: string(job.TaskToken),
		job: job,
		expiresAfter: internal.CalculateActivityDeadline(
			internalcommon.TimeValue(job.GetScheduledTime()),
			internalcommon.TimeValue(job.GetStartedTime()),
			internalcommon.DurationValue(job.GetScheduleToCloseTimeout()),
			internalcommon.DurationValue(job.GetStartToCloseTimeout()),
		).Add(activityJobExpiryLeeway),
		respCh: respCh,
		errCh:  errCh,
	}:
	}

	// Wait for result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-i.stopCh:
		return nil, errWorkerClosed
	case err := <-errCh:
		return nil, err
	case resp := <-respCh:
		return resp, nil
	}
}

func (i *InvertedWorker) Close() { close(i.stopCh) }

func (i *InvertedWorker) runJobPurger() {
	t := time.NewTicker(staleJobPurgeFreq)
	defer t.Stop()
	for {
		select {
		case <-i.stopCh:
			return
		case <-t.C:
			var errChs []chan<- error
			// Remove expired
			now := time.Now()
			i.runningWorkflowJobsLock.Lock()
			for k, v := range i.runningWorkflowJobs {
				if now.After(v.expiresAfter) {
					delete(i.runningWorkflowJobs, k)
					errChs = append(errChs, v.errCh)
				}
			}
			i.runningWorkflowJobsLock.Unlock()
			i.runningActivityJobsLock.Lock()
			for k, v := range i.runningActivityJobs {
				if now.After(v.expiresAfter) {
					delete(i.runningActivityJobs, k)
					errChs = append(errChs, v.errCh)
				}
			}
			i.runningActivityJobsLock.Unlock()

			// Try to send errors to expired
			for _, errCh := range errChs {
				select {
				case errCh <- errJobExpired:
				default:
				}
			}
		}
	}
}

type pendingWorkflowJob struct {
	key string
	job *workflowservice.PollWorkflowTaskQueueResponse
	// TODO(cretz): Update on workflow task heartbeat or disable local activities
	// or something
	expiresAfter time.Time
	// Note, these channels may not have listeners. Do non-blocking sends.
	respCh chan<- *WorkflowJobResponse
	errCh  chan<- error
}

type pendingActivityJob struct {
	key          string
	job          *workflowservice.PollActivityTaskQueueResponse
	expiresAfter time.Time
	// Note, these channels may not have listeners. Do non-blocking sends.
	respCh chan<- *ActivityJobResponse
	errCh  chan<- error
}

type workflowService struct {
	// Embed and just let all non-impl'd calls panic
	workflowservice.WorkflowServiceClient
	worker *InvertedWorker
}

func newWorkflowService(worker *InvertedWorker) *workflowService {
	return &workflowService{worker: worker}
}

func (w *workflowService) GetSystemInfo(
	context.Context,
	*workflowservice.GetSystemInfoRequest,
	...grpc.CallOption,
) (*workflowservice.GetSystemInfoResponse, error) {
	return w.worker.systemInfo, nil
}

func (w *workflowService) DescribeNamespace(
	ctx context.Context,
	in *workflowservice.DescribeNamespaceRequest,
	opts ...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	// Just give basic response, worker is just using this to make sure it doesn't
	// error
	return &workflowservice.DescribeNamespaceResponse{
		NamespaceInfo: &namespace.NamespaceInfo{Name: in.Namespace},
	}, nil
}

func (w *workflowService) PollWorkflowTaskQueue(
	ctx context.Context,
	req *workflowservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.worker.stopCh:
		return nil, errWorkerClosed
	case job := <-w.worker.pendingWorkflowJobs:
		w.worker.runningWorkflowJobsLock.Lock()
		w.worker.runningWorkflowJobs[job.key] = job
		w.worker.runningWorkflowJobsLock.Unlock()
		return job.job, nil
	}
}

func (w *workflowService) respondWorkflowJob(taskToken []byte, resp *WorkflowJobResponse) error {
	w.worker.runningWorkflowJobsLock.Lock()
	// Key is optimized: https://github.com/golang/go/issues/3512
	job := w.worker.runningWorkflowJobs[string(taskToken)]
	if job != nil {
		delete(w.worker.runningWorkflowJobs, job.key)
	}
	w.worker.runningWorkflowJobsLock.Unlock()
	if job == nil {
		return errJobExpired
	}
	select {
	case job.respCh <- resp:
		return nil
	default:
		return errJobExpired
	}
}

func (w *workflowService) RespondWorkflowTaskCompleted(
	ctx context.Context,
	req *workflowservice.RespondWorkflowTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	return &workflowservice.RespondWorkflowTaskCompletedResponse{},
		w.respondWorkflowJob(req.TaskToken, &WorkflowJobResponse{Completed: req})
}

func (w *workflowService) RespondWorkflowTaskFailed(
	ctx context.Context,
	req *workflowservice.RespondWorkflowTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondWorkflowTaskFailedResponse, error) {
	return &workflowservice.RespondWorkflowTaskFailedResponse{},
		w.respondWorkflowJob(req.TaskToken, &WorkflowJobResponse{Failed: req})
}

func (w *workflowService) PollActivityTaskQueue(
	ctx context.Context,
	req *workflowservice.PollActivityTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollActivityTaskQueueResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.worker.stopCh:
		return nil, errWorkerClosed
	case job := <-w.worker.pendingActivityJobs:
		w.worker.runningActivityJobsLock.Lock()
		w.worker.runningActivityJobs[job.key] = job
		w.worker.runningActivityJobsLock.Unlock()
		return job.job, nil
	}
}

func (w *workflowService) RecordActivityTaskHeartbeat(
	ctx context.Context,
	req *workflowservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	panic("TODO")
}

func (w *workflowService) respondActivityJob(taskToken []byte, resp *ActivityJobResponse) error {
	w.worker.runningActivityJobsLock.Lock()
	// Key is optimized: https://github.com/golang/go/issues/3512
	job := w.worker.runningActivityJobs[string(taskToken)]
	if job != nil {
		delete(w.worker.runningActivityJobs, job.key)
	}
	w.worker.runningActivityJobsLock.Unlock()
	if job == nil {
		return errJobExpired
	}
	select {
	case job.respCh <- resp:
		return nil
	default:
		return errJobExpired
	}
}

func (w *workflowService) RespondActivityTaskCompleted(
	ctx context.Context,
	req *workflowservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	return &workflowservice.RespondActivityTaskCompletedResponse{},
		w.respondActivityJob(req.TaskToken, &ActivityJobResponse{Completed: req})
}

func (w *workflowService) RespondActivityTaskFailed(
	ctx context.Context,
	req *workflowservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	return &workflowservice.RespondActivityTaskFailedResponse{},
		w.respondActivityJob(req.TaskToken, &ActivityJobResponse{Failed: req})
}

func (w *workflowService) RespondActivityTaskCanceled(
	ctx context.Context,
	req *workflowservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption,
) (*workflowservice.RespondActivityTaskCanceledResponse, error) {
	return &workflowservice.RespondActivityTaskCanceledResponse{},
		w.respondActivityJob(req.TaskToken, &ActivityJobResponse{Canceled: req})
}
