// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../LICENSE -package client -source client.go -destination client_mock.go

// Package client is used by external programs to communicate with Temporal service.
// NOTE: DO NOT USE THIS API INSIDE OF ANY WORKFLOW CODE!!!
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/activityutil"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/internal/common/serializer"
	"go.temporal.io/sdk/internal/common/util"
	"go.temporal.io/sdk/internal/converterutil"
	"go.temporal.io/sdk/internal/funcutil"
	"go.temporal.io/sdk/internal/headerutil"
	ilog "go.temporal.io/sdk/internal/log"
	"go.temporal.io/sdk/temporal"
)

type client struct {
	options          Options
	service          workflowservice.WorkflowServiceClient
	connectionCloser io.Closer
	// namespace          string
	// logger             log.Logger
	// metricsHandler     MetricsHandler
	// identity           string
	// dataConverter      converter.DataConverter
	// contextPropagators []ContextPropagator
	interceptor OutboundInterceptor
}

// NewClient creates an instance of a workflow client
func NewClient(options Options) (Client, error) {
	if options.Namespace == "" {
		options.Namespace = DefaultNamespace
	}

	if options.Identity == "" {
		options.Identity = defaultIdentity()
	}

	if options.DataConverter == nil {
		options.DataConverter = converter.GetDefaultDataConverter()
	}

	// Initialize root tags
	if options.MetricsHandler == nil {
		options.MetricsHandler = MetricsNopHandler
	}
	options.MetricsHandler = options.MetricsHandler.WithTags(metrics.RootTags(options.Namespace))

	if options.HostPort == "" {
		options.HostPort = DefaultHostPort
	}

	if options.Logger == nil {
		options.Logger = ilog.NewDefaultLogger()
		options.Logger.Info("No logger configured for temporal client. Created default one.")
	}

	connection, err := dial(newDialParameters(&options))
	if err != nil {
		return nil, err
	}

	if err = checkHealth(connection, options.ConnectionOptions); err != nil {
		if err := connection.Close(); err != nil {
			options.Logger.Warn("Unable to close connection on health check failure.", "error", err)
		}
		return nil, err
	}

	c := ClientFromService(workflowservice.NewWorkflowServiceClient(connection), options)
	c.(*client).connectionCloser = connection
	return c, nil
}

// Does not check health, close is a noop
func ClientFromService(service workflowservice.WorkflowServiceClient, options Options) Client {
	if options.Namespace == "" {
		options.Namespace = DefaultNamespace
	}

	if options.Identity == "" {
		options.Identity = defaultIdentity()
	}

	if options.DataConverter == nil {
		options.DataConverter = converter.GetDefaultDataConverter()
	}

	if options.MetricsHandler == nil {
		options.MetricsHandler = MetricsNopHandler
	}

	client := &client{
		options: options,
		service: service,
	}

	// Create outbound interceptor by wrapping backwards through chain
	client.interceptor = &interceptor{client: client}
	for i := len(options.Interceptors) - 1; i >= 0; i-- {
		client.interceptor = options.Interceptors[i].InterceptClient(client.interceptor)
	}

	return client
}

func defaultIdentity() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "Unknown"
	}
	return fmt.Sprintf("%d@%s@", os.Getpid(), hostname)
}

func (c *client) ExecuteWorkflow(ctx context.Context, options StartWorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error) {
	// Default workflow ID
	if options.ID == "" {
		options.ID = uuid.New()
	}

	// Validate function and get name
	if err := funcutil.ValidateFunctionArgs(workflow, args, true); err != nil {
		return nil, err
	}
	workflowType, err := getWorkflowFunctionName(workflow)
	if err != nil {
		return nil, err
	}

	// Set header before interceptor run
	ctx = headerutil.ContextWithNewHeader(ctx)

	// Run via interceptor
	return c.interceptor.ExecuteWorkflow(ctx, &ClientExecuteWorkflowInput{
		Options:      &options,
		WorkflowType: workflowType,
		Args:         args,
	})
}

func getWorkflowFunctionName(workflowFunc interface{}) (string, error) {
	fType := reflect.TypeOf(workflowFunc)
	if fType != nil {
		if fType.Kind() == reflect.String {
			return reflect.ValueOf(workflowFunc).String(), nil
		} else if fType.Kind() == reflect.Func {
			fnName, _ := funcutil.GetFunctionName(workflowFunc)
			return fnName, nil
		}
	}
	return "", fmt.Errorf("invalid type 'workflowFunc' parameter provided, it can be either worker function or function name: %v", workflowFunc)
}
func (c *client) GetWorkflow(ctx context.Context, workflowID string, runID string) WorkflowRun {
	iterFn := func(fnCtx context.Context, fnRunID string) HistoryEventIterator {
		return c.GetWorkflowHistory(fnCtx, workflowID, fnRunID, true, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
	}

	// The ID may not actually have been set - if not, we have to (lazily) ask the server for info about the workflow
	// execution and extract run id from there. This is definitely less efficient than it could be if there was a more
	// specific rpc method for this, or if there were more granular history filters - in which case it could be
	// extracted from the `iterFn` inside of `workflowRunImpl`
	var runIDCell util.OnceCell
	if runID == "" {
		fetcher := func() string {
			execData, _ := c.DescribeWorkflowExecution(ctx, workflowID, runID)
			wei := execData.GetWorkflowExecutionInfo()
			if wei != nil {
				execution := wei.GetExecution()
				if execution != nil {
					return execution.RunId
				}
			}
			return ""
		}
		runIDCell = util.LazyOnceCell(fetcher)
	} else {
		runIDCell = util.PopulatedOnceCell(runID)
	}

	return &workflowRunImpl{
		workflowID:    workflowID,
		firstRunID:    runID,
		currentRunID:  &runIDCell,
		iterFn:        iterFn,
		dataConverter: c.options.DataConverter,
	}
}

func (c *client) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	// Set header before interceptor run
	ctx = headerutil.ContextWithNewHeader(ctx)

	return c.interceptor.SignalWorkflow(ctx, &ClientSignalWorkflowInput{
		WorkflowID: workflowID,
		RunID:      runID,
		SignalName: signalName,
		Arg:        arg,
	})
}

func (c *client) SignalWithStartWorkflow(ctx context.Context, workflowID string, signalName string, signalArg interface{},
	options StartWorkflowOptions, workflowFunc interface{}, workflowArgs ...interface{}) (WorkflowRun, error) {

	// Due to the ambiguous way to provide workflow IDs, if options contains an
	// ID, it must match the parameter
	if options.ID != "" && options.ID != workflowID {
		return nil, fmt.Errorf("workflow ID from options not used, must be unset or match workflow ID parameter")
	}

	// Default workflow ID to UUID
	options.ID = workflowID
	if options.ID == "" {
		options.ID = uuid.New()
	}

	// Validate function and get name
	if err := funcutil.ValidateFunctionArgs(workflowFunc, workflowArgs, true); err != nil {
		return nil, err
	}
	workflowType, err := getWorkflowFunctionName(workflowFunc)
	if err != nil {
		return nil, err
	}

	// Set header before interceptor run
	ctx = headerutil.ContextWithNewHeader(ctx)

	// Run via interceptor
	return c.interceptor.SignalWithStartWorkflow(ctx, &ClientSignalWithStartWorkflowInput{
		SignalName:   signalName,
		SignalArg:    signalArg,
		Options:      &options,
		WorkflowType: workflowType,
		Args:         workflowArgs,
	})
}

func (c *client) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	return c.interceptor.CancelWorkflow(ctx, &ClientCancelWorkflowInput{WorkflowID: workflowID, RunID: runID})
}

func (c *client) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details ...interface{}) error {
	return c.interceptor.TerminateWorkflow(ctx, &ClientTerminateWorkflowInput{
		WorkflowID: workflowID,
		RunID:      runID,
		Reason:     reason,
		Details:    details,
	})
}

func (c *client) GetWorkflowHistory(
	ctx context.Context,
	workflowID string,
	runID string,
	isLongPoll bool,
	filterType enumspb.HistoryEventFilterType,
) HistoryEventIterator {
	return c.getWorkflowHistory(ctx, workflowID, runID, isLongPoll, filterType, c.options.MetricsHandler)
}

func (c *client) getWorkflowHistory(
	ctx context.Context,
	workflowID string,
	runID string,
	isLongPoll bool,
	filterType enumspb.HistoryEventFilterType,
	rpcMetricsHandler MetricsHandler,
) HistoryEventIterator {
	paginate := func(nextToken []byte) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
		request := &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: c.options.Namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			WaitNewEvent:           isLongPoll,
			HistoryEventFilterType: filterType,
			NextPageToken:          nextToken,
			SkipArchival:           isLongPoll,
		}

		var response *workflowservice.GetWorkflowExecutionHistoryResponse
		var err error
	Loop:
		for {
			response, err = c.getWorkflowExecutionHistory(ctx, rpcMetricsHandler, isLongPoll, request, filterType)
			if err != nil {
				return nil, err
			}
			if isLongPoll && len(response.History.Events) == 0 && len(response.NextPageToken) != 0 {
				request.NextPageToken = response.NextPageToken
				continue Loop
			}
			break Loop
		}
		return response, nil
	}

	return &historyEventIteratorImpl{
		paginate: paginate,
	}
}

const defaultGetHistoryTimeout = 65 * time.Second

func (c *client) getWorkflowExecutionHistory(
	ctx context.Context,
	rpcMetricsHandler MetricsHandler,
	isLongPoll bool,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	filterType enumspb.HistoryEventFilterType,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	grpcCtxOpts := GRPCContextOptions{
		MetricsHandler:     rpcMetricsHandler,
		LongPoll:           isLongPoll,
		DefaultRetryPolicy: true,
	}
	if isLongPoll {
		grpcCtxOpts.Timeout = defaultGetHistoryTimeout
	}
	grpcCtx, cancel := NewGRPCContext(ctx, grpcCtxOpts)

	defer cancel()
	response, err := c.service.GetWorkflowExecutionHistory(grpcCtx, request)

	if err != nil {
		return nil, err
	}

	if response.RawHistory != nil {
		history, err := serializer.DeserializeBlobDataToHistoryEvents(response.RawHistory, filterType)
		if err != nil {
			return nil, err
		}
		response.History = history
	}
	return response, err
}

func (c *client) CompleteActivity(ctx context.Context, taskToken []byte, result interface{}, err error) error {
	if taskToken == nil {
		return errors.New("invalid task token provided")
	}

	dataConverter := headerutil.WithContext(ctx, c.options.DataConverter)
	var data *commonpb.Payloads
	if result != nil {
		var err0 error
		data, err0 = dataConverter.ToPayloads(result)
		if err0 != nil {
			return err0
		}
	}

	request := activityutil.ConvertActivityResultToRespondRequest(c.options.Identity, taskToken, data, err,
		c.options.DataConverter, c.options.Namespace)

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{
		MetricsHandler:     c.options.MetricsHandler,
		DefaultRetryPolicy: true,
	})
	defer cancel()
	var retErr error
	switch request := request.(type) {
	case *workflowservice.RespondActivityTaskCanceledRequest:
		_, retErr = c.service.RespondActivityTaskCanceled(ctx, request)
	case *workflowservice.RespondActivityTaskFailedRequest:
		_, retErr = c.service.RespondActivityTaskFailed(ctx, request)
	case *workflowservice.RespondActivityTaskCompletedRequest:
		_, retErr = c.service.RespondActivityTaskCompleted(ctx, request)
	}
	return retErr
}

func (c *client) CompleteActivityByID(ctx context.Context, namespace, workflowID, runID, activityID string,
	result interface{}, err error) error {

	if activityID == "" || workflowID == "" || namespace == "" {
		return errors.New("empty activity or workflow id or namespace")
	}

	dataConverter := headerutil.WithContext(ctx, c.options.DataConverter)
	var data *commonpb.Payloads
	if result != nil {
		var err0 error
		data, err0 = dataConverter.ToPayloads(result)
		if err0 != nil {
			return err0
		}
	}

	request := activityutil.ConvertActivityResultToRespondRequestByID(c.options.Identity, namespace,
		workflowID, runID, activityID, data, err, c.options.DataConverter)

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{
		MetricsHandler:     c.options.MetricsHandler,
		DefaultRetryPolicy: true,
	})
	defer cancel()

	var retErr error
	switch request := request.(type) {
	case *workflowservice.RespondActivityTaskCanceledByIdRequest:
		_, retErr = c.service.RespondActivityTaskCanceledById(ctx, request)
	case *workflowservice.RespondActivityTaskFailedByIdRequest:
		_, retErr = c.service.RespondActivityTaskFailedById(ctx, request)
	case *workflowservice.RespondActivityTaskCompletedByIdRequest:
		_, retErr = c.service.RespondActivityTaskCompletedById(ctx, request)
	}
	return retErr
}

func (c *client) RecordActivityHeartbeat(ctx context.Context, taskToken []byte, details ...interface{}) error {
	dataConverter := headerutil.WithContext(ctx, c.options.DataConverter)
	data, err := dataConverter.ToPayloads(details...)
	if err != nil {
		return err
	}

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{
		MetricsHandler:     c.options.MetricsHandler,
		DefaultRetryPolicy: true,
	})
	defer cancel()

	var namespace string
	if activity.IsActivityContext(ctx) {
		namespace = activity.GetInfo(ctx).WorkflowNamespace
	}

	resp, err := c.service.RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
		TaskToken: taskToken,
		Details:   data,
		Identity:  c.options.Identity,
		Namespace: namespace,
	})
	if err == nil && resp != nil && resp.GetCancelRequested() {
		return temporal.NewCanceledError()
	}
	return err
}

func (c *client) RecordActivityHeartbeatByID(
	ctx context.Context,
	namespace string,
	workflowID string,
	runID string,
	activityID string,
	details ...interface{},
) error {
	dataConverter := headerutil.WithContext(ctx, c.options.DataConverter)
	data, err := dataConverter.ToPayloads(details...)
	if err != nil {
		return err
	}

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{
		MetricsHandler:     c.options.MetricsHandler,
		DefaultRetryPolicy: true,
	})
	defer cancel()

	resp, err := c.service.RecordActivityTaskHeartbeatById(ctx, &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
		Namespace:  namespace,
		WorkflowId: workflowID,
		RunId:      runID,
		ActivityId: activityID,
		Details:    data,
		Identity:   c.options.Identity,
	})
	if err == nil && resp != nil && resp.GetCancelRequested() {
		return temporal.NewCanceledError()
	}
	return err
}

func (c *client) ListClosedWorkflow(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	if request.GetNamespace() == "" {
		request.Namespace = c.options.Namespace
	}
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	return c.service.ListClosedWorkflowExecutions(ctx, request)
}

func (c *client) ListOpenWorkflow(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	if request.GetNamespace() == "" {
		request.Namespace = c.options.Namespace
	}
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	return c.service.ListOpenWorkflowExecutions(ctx, request)
}

func (c *client) ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if request.GetNamespace() == "" {
		request.Namespace = c.options.Namespace
	}
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	return c.service.ListWorkflowExecutions(ctx, request)
}

const maxListArchivedWorkflowTimeout = 3 * time.Minute

func (c *client) ListArchivedWorkflow(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	if request.GetNamespace() == "" {
		request.Namespace = c.options.Namespace
	}
	timeout := maxListArchivedWorkflowTimeout
	now := time.Now()
	if ctx != nil {
		if expiration, ok := ctx.Deadline(); ok && expiration.After(now) {
			timeout = expiration.Sub(now)
			if timeout > maxListArchivedWorkflowTimeout {
				timeout = maxListArchivedWorkflowTimeout
			} else if timeout < minRPCTimeout {
				timeout = minRPCTimeout
			}
		}
	}
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{Timeout: timeout, DefaultRetryPolicy: true})
	defer cancel()
	return c.service.ListArchivedWorkflowExecutions(ctx, request)
}

func (c *client) ScanWorkflow(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	if request.GetNamespace() == "" {
		request.Namespace = c.options.Namespace
	}
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	return c.service.ScanWorkflowExecutions(ctx, request)
}

func (c *client) CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	if request.GetNamespace() == "" {
		request.Namespace = c.options.Namespace
	}
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	return c.service.CountWorkflowExecutions(ctx, request)
}

func (c *client) GetSearchAttributes(ctx context.Context) (*workflowservice.GetSearchAttributesResponse, error) {
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	return c.service.GetSearchAttributes(ctx, &workflowservice.GetSearchAttributesRequest{})
}

func (c *client) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	request := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: c.options.Namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	return c.service.DescribeWorkflowExecution(ctx, request)
}

func (c *client) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	// Set header before interceptor run
	ctx = headerutil.ContextWithNewHeader(ctx)

	return c.interceptor.QueryWorkflow(ctx, &ClientQueryWorkflowInput{
		WorkflowID: workflowID,
		RunID:      runID,
		QueryType:  queryType,
		Args:       args,
	})
}

func (c *client) QueryWorkflowWithOptions(ctx context.Context, request *QueryWorkflowWithOptionsRequest) (*QueryWorkflowWithOptionsResponse, error) {
	var input *commonpb.Payloads
	if len(request.Args) > 0 {
		var err error
		if input, err = c.options.DataConverter.ToPayloads(request.Args...); err != nil {
			return nil, err
		}
	}
	req := &workflowservice.QueryWorkflowRequest{
		Namespace: c.options.Namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: request.WorkflowID,
			RunId:      request.RunID,
		},
		Query: &querypb.WorkflowQuery{
			QueryType: request.QueryType,
			QueryArgs: input,
			Header:    request.Header,
		},
		QueryRejectCondition: request.QueryRejectCondition,
	}

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	resp, err := c.service.QueryWorkflow(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.QueryRejected != nil {
		return &QueryWorkflowWithOptionsResponse{
			QueryRejected: resp.QueryRejected,
			QueryResult:   nil,
		}, nil
	}
	return &QueryWorkflowWithOptionsResponse{
		QueryRejected: nil,
		QueryResult:   converterutil.NewEncodedValue(resp.QueryResult, c.options.DataConverter),
	}, nil
}

func (c *client) DescribeTaskQueue(ctx context.Context, taskQueue string, taskQueueType enumspb.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	request := &workflowservice.DescribeTaskQueueRequest{
		Namespace:     c.options.Namespace,
		TaskQueue:     &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: taskQueueType,
	}

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	return c.service.DescribeTaskQueue(ctx, request)
}

func (c *client) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	if request != nil && request.GetRequestId() == "" {
		request.RequestId = uuid.New()
	}

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	return c.service.ResetWorkflowExecution(ctx, request)
}

func (c *client) WorkflowService() workflowservice.WorkflowServiceClient {
	return c.service
}

func (c *client) Close() {
	if c.connectionCloser == nil {
		return
	}
	if err := c.connectionCloser.Close(); err != nil {
		c.options.Logger.Warn("unable to close connection", ilog.TagError, err)
	}
}
