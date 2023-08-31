package internal_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
)

func OnDemandTestWorkflow(ctx workflow.Context, name string) (string, error) {
	return "Hello, " + name + "!", nil
}

func TestOnDemandWorkflowWorkerEmptyPollResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv := startTestOnDemandGRPCServer()
	defer srv.Stop()

	var pollCount int
	var pollReq *workflowservice.PollWorkflowTaskQueueRequest
	srv.pollCallback = func(
		req *workflowservice.PollWorkflowTaskQueueRequest,
	) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
		pollCount++
		pollReq = req
		// Empty response (no task token)
		return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
	}

	// Start on-demand workflow worker run
	errCh := make(chan error, 1)
	go func() {
		w, err := worker.NewOnDemandWorkflowWorker(worker.OnDemandWorkflowWorkerOptions{
			TaskQueue:     "tq-test",
			Workflows:     []worker.RegisteredWorkflow{{Workflow: OnDemandTestWorkflow}},
			ClientOptions: client.Options{HostPort: srv.addr},
		})
		if err == nil {
			defer w.Close()
			err = w.ExecuteSingleRun(ctx, worker.OnDemandWorkflowWorkerExecuteOptions{
				ServerCapabilities: &workflowservice.GetSystemInfoResponse_Capabilities{},
			})
		}
		errCh <- err
	}()

	// Confirm single-run ends
	select {
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	case err := <-errCh:
		require.NoError(t, err)
		require.Equal(t, 1, pollCount)
		require.Equal(t, enums.TASK_QUEUE_KIND_NORMAL, pollReq.TaskQueue.Kind)
	}
}

func TestOnDemandWorkflowWorkerErrorPollResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv := startTestOnDemandGRPCServer()
	defer srv.Stop()

	var pollCount int
	srv.pollCallback = func(
		req *workflowservice.PollWorkflowTaskQueueRequest,
	) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
		pollCount++
		return nil, fmt.Errorf("some error")
	}

	// Start on-demand workflow worker run
	errCh := make(chan error, 1)
	go func() {
		w, err := worker.NewOnDemandWorkflowWorker(worker.OnDemandWorkflowWorkerOptions{
			TaskQueue:     "tq-test",
			Workflows:     []worker.RegisteredWorkflow{{Workflow: OnDemandTestWorkflow}},
			ClientOptions: client.Options{HostPort: srv.addr},
		})
		if err == nil {
			defer w.Close()
			err = w.ExecuteSingleRun(ctx, worker.OnDemandWorkflowWorkerExecuteOptions{
				ServerCapabilities: &workflowservice.GetSystemInfoResponse_Capabilities{},
			})
		}
		errCh <- err
	}()

	// Confirm single-run ends
	select {
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	case err := <-errCh:
		require.ErrorContains(t, err, "some error")
		require.Equal(t, 1, pollCount)
	}
}

type testOnDemandGRPCServer struct {
	workflowservice.UnimplementedWorkflowServiceServer
	*grpc.Server
	addr string

	pollCallback func(*workflowservice.PollWorkflowTaskQueueRequest) (*workflowservice.PollWorkflowTaskQueueResponse, error)

	// Locks all fields below
	reqRespLock sync.Mutex

	lastRespondWorkflowTaskCompletedRequest  *workflowservice.RespondWorkflowTaskCompletedRequest
	nextRespondWorkflowTaskCompletedResponse *workflowservice.RespondWorkflowTaskCompletedResponse
	nextRespondWorkflowTaskCompletedErr      error

	lastRespondWorkflowTaskFailedRequest  *workflowservice.RespondWorkflowTaskFailedRequest
	nextRespondWorkflowTaskFailedResponse *workflowservice.RespondWorkflowTaskFailedResponse
	nextRespondWorkflowTaskFailedErr      error
}

func startTestOnDemandGRPCServer() *testOnDemandGRPCServer {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	t := &testOnDemandGRPCServer{
		Server: grpc.NewServer(),
		addr:   l.Addr().String(),
	}
	workflowservice.RegisterWorkflowServiceServer(t.Server, t)
	go func() {
		if err := t.Serve(l); err != nil {
			log.Fatal(err)
		}
	}()
	return t
}

func (t *testOnDemandGRPCServer) PollWorkflowTaskQueue(
	ctx context.Context,
	req *workflowservice.PollWorkflowTaskQueueRequest,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	if t.pollCallback == nil {
		return nil, fmt.Errorf("not implemented")
	}
	return t.pollCallback(req)
}

func (t *testOnDemandGRPCServer) RespondWorkflowTaskCompleted(
	ctx context.Context,
	req *workflowservice.RespondWorkflowTaskCompletedRequest,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	t.reqRespLock.Lock()
	defer t.reqRespLock.Unlock()
	t.lastRespondWorkflowTaskCompletedRequest = req
	return t.nextRespondWorkflowTaskCompletedResponse, t.nextRespondWorkflowTaskCompletedErr
}

func (t *testOnDemandGRPCServer) RespondWorkflowTaskFailed(
	ctx context.Context,
	req *workflowservice.RespondWorkflowTaskFailedRequest,
) (*workflowservice.RespondWorkflowTaskFailedResponse, error) {
	t.reqRespLock.Lock()
	defer t.reqRespLock.Unlock()
	t.lastRespondWorkflowTaskFailedRequest = req
	return t.nextRespondWorkflowTaskFailedResponse, t.nextRespondWorkflowTaskFailedErr
}
