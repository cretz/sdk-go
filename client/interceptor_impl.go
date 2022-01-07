package client

import (
	"context"
	"fmt"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/clientutil"
	"go.temporal.io/sdk/internal/common/metrics"
	"go.temporal.io/sdk/internal/common/util"
	"go.temporal.io/sdk/internal/headerutil"
	"go.temporal.io/sdk/internal/workflowutil"
)

// Root outbound interceptor
type interceptor struct {
	client *client
}

func (w *interceptor) ExecuteWorkflow(
	ctx context.Context,
	in *ClientExecuteWorkflowInput,
) (WorkflowRun, error) {
	// This is always set before interceptor is invoked
	workflowID := in.Options.ID
	if workflowID == "" {
		return nil, fmt.Errorf("no workflow ID in options")
	}

	executionTimeout := in.Options.WorkflowExecutionTimeout
	runTimeout := in.Options.WorkflowRunTimeout
	workflowTaskTimeout := in.Options.WorkflowTaskTimeout

	dataConverter := headerutil.WithContext(ctx, w.client.options.DataConverter)
	if dataConverter == nil {
		dataConverter = converter.GetDefaultDataConverter()
	}

	// Encode input
	input, err := dataConverter.ToPayloads(in.Args...)
	if err != nil {
		return nil, err
	}

	memo, err := workflowutil.GetWorkflowMemo(in.Options.Memo, dataConverter)
	if err != nil {
		return nil, err
	}

	searchAttr, err := workflowutil.SerializeSearchAttributes(in.Options.SearchAttributes)
	if err != nil {
		return nil, err
	}

	// get workflow headers from the context
	header, err := headerutil.HeaderPropagated(ctx, w.client.options.ContextPropagators)
	if err != nil {
		return nil, err
	}

	// run propagators to extract information about tracing and other stuff, store in headers field
	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                w.client.options.Namespace,
		RequestId:                uuid.New(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: in.WorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: in.Options.TaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:                    input,
		WorkflowExecutionTimeout: &executionTimeout,
		WorkflowRunTimeout:       &runTimeout,
		WorkflowTaskTimeout:      &workflowTaskTimeout,
		Identity:                 w.client.options.Identity,
		WorkflowIdReusePolicy:    in.Options.WorkflowIDReusePolicy,
		RetryPolicy:              clientutil.RetryPolicyToProto(in.Options.RetryPolicy),
		CronSchedule:             in.Options.CronSchedule,
		Memo:                     memo,
		SearchAttributes:         searchAttr,
		Header:                   header,
	}

	var response *workflowservice.StartWorkflowExecutionResponse
	metricsHandler := w.client.options.MetricsHandler.WithTags(
		metrics.RPCTags(in.WorkflowType, metrics.NoneTagValue, in.Options.TaskQueue))

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{
		MetricsHandler:     metricsHandler,
		DefaultRetryPolicy: true,
	})
	defer cancel()

	response, err = w.client.service.StartWorkflowExecution(ctx, startRequest)

	// Allow already-started error
	var runID string
	if e, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); ok && !in.Options.WorkflowExecutionErrorWhenAlreadyStarted {
		runID = e.RunId
	} else if err != nil {
		return nil, err
	} else {
		runID = response.RunId
	}

	iterFn := func(fnCtx context.Context, fnRunID string) HistoryEventIterator {
		return w.client.getWorkflowHistory(fnCtx, workflowID, fnRunID, true,
			enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT, metricsHandler)
	}

	curRunIDCell := util.PopulatedOnceCell(runID)
	return &workflowRunImpl{
		workflowType:  in.WorkflowType,
		workflowID:    workflowID,
		firstRunID:    runID,
		currentRunID:  &curRunIDCell,
		iterFn:        iterFn,
		dataConverter: w.client.options.DataConverter,
	}, nil
}

func (w *interceptor) SignalWorkflow(ctx context.Context, in *ClientSignalWorkflowInput) error {
	dataConverter := headerutil.WithContext(ctx, w.client.options.DataConverter)
	input, err := dataConverter.ToPayloads(in.Arg)
	if err != nil {
		return err
	}

	// get workflow headers from the context
	header, err := headerutil.HeaderPropagated(ctx, w.client.options.ContextPropagators)
	if err != nil {
		return err
	}

	request := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: w.client.options.Namespace,
		RequestId: uuid.New(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: in.WorkflowID,
			RunId:      in.RunID,
		},
		SignalName: in.SignalName,
		Input:      input,
		Identity:   w.client.options.Identity,
		Header:     header,
	}

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	_, err = w.client.service.SignalWorkflowExecution(ctx, request)
	return err
}

func (w *interceptor) SignalWithStartWorkflow(
	ctx context.Context,
	in *ClientSignalWithStartWorkflowInput,
) (WorkflowRun, error) {

	dataConverter := headerutil.WithContext(ctx, w.client.options.DataConverter)
	signalInput, err := dataConverter.ToPayloads(in.SignalArg)
	if err != nil {
		return nil, err
	}

	executionTimeout := in.Options.WorkflowExecutionTimeout
	runTimeout := in.Options.WorkflowRunTimeout
	taskTimeout := in.Options.WorkflowTaskTimeout

	// Encode input
	input, err := dataConverter.ToPayloads(in.Args...)
	if err != nil {
		return nil, err
	}

	memo, err := workflowutil.GetWorkflowMemo(in.Options.Memo, dataConverter)
	if err != nil {
		return nil, err
	}

	searchAttr, err := workflowutil.SerializeSearchAttributes(in.Options.SearchAttributes)
	if err != nil {
		return nil, err
	}

	// get workflow headers from the context
	header, err := headerutil.HeaderPropagated(ctx, w.client.options.ContextPropagators)
	if err != nil {
		return nil, err
	}

	signalWithStartRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:                w.client.options.Namespace,
		RequestId:                uuid.New(),
		WorkflowId:               in.Options.ID,
		WorkflowType:             &commonpb.WorkflowType{Name: in.WorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: in.Options.TaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:                    input,
		WorkflowExecutionTimeout: &executionTimeout,
		WorkflowRunTimeout:       &runTimeout,
		WorkflowTaskTimeout:      &taskTimeout,
		SignalName:               in.SignalName,
		SignalInput:              signalInput,
		Identity:                 w.client.options.Identity,
		RetryPolicy:              clientutil.RetryPolicyToProto(in.Options.RetryPolicy),
		CronSchedule:             in.Options.CronSchedule,
		Memo:                     memo,
		SearchAttributes:         searchAttr,
		WorkflowIdReusePolicy:    in.Options.WorkflowIDReusePolicy,
		Header:                   header,
	}

	var response *workflowservice.SignalWithStartWorkflowExecutionResponse

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()

	response, err = w.client.service.SignalWithStartWorkflowExecution(ctx, signalWithStartRequest)
	if err != nil {
		return nil, err
	}

	iterFn := func(fnCtx context.Context, fnRunID string) HistoryEventIterator {
		metricsHandler := w.client.options.MetricsHandler.WithTags(metrics.RPCTags(in.WorkflowType,
			metrics.NoneTagValue, in.Options.TaskQueue))
		return w.client.getWorkflowHistory(fnCtx, in.Options.ID, fnRunID, true,
			enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT, metricsHandler)
	}

	curRunIDCell := util.PopulatedOnceCell(response.GetRunId())
	return &workflowRunImpl{
		workflowType:  in.WorkflowType,
		workflowID:    in.Options.ID,
		firstRunID:    response.GetRunId(),
		currentRunID:  &curRunIDCell,
		iterFn:        iterFn,
		dataConverter: w.client.options.DataConverter,
	}, nil
}

func (w *interceptor) CancelWorkflow(ctx context.Context, in *ClientCancelWorkflowInput) error {
	request := &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: w.client.options.Namespace,
		RequestId: uuid.New(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: in.WorkflowID,
			RunId:      in.RunID,
		},
		Identity: w.client.options.Identity,
	}
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	_, err := w.client.service.RequestCancelWorkflowExecution(ctx, request)
	return err
}

func (w *interceptor) TerminateWorkflow(ctx context.Context, in *ClientTerminateWorkflowInput) error {
	datailsPayload, err := w.client.options.DataConverter.ToPayloads(in.Details...)
	if err != nil {
		return err
	}

	request := &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: w.client.options.Namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: in.WorkflowID,
			RunId:      in.RunID,
		},
		Reason:   in.Reason,
		Identity: w.client.options.Identity,
		Details:  datailsPayload,
	}

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	_, err = w.client.service.TerminateWorkflowExecution(ctx, request)
	return err
}

func (w *interceptor) QueryWorkflow(
	ctx context.Context,
	in *ClientQueryWorkflowInput,
) (converter.EncodedValue, error) {
	// get workflow headers from the context
	header, err := headerutil.HeaderPropagated(ctx, w.client.options.ContextPropagators)
	if err != nil {
		return nil, err
	}

	result, err := w.client.QueryWorkflowWithOptions(ctx, &QueryWorkflowWithOptionsRequest{
		WorkflowID: in.WorkflowID,
		RunID:      in.RunID,
		QueryType:  in.QueryType,
		Args:       in.Args,
		Header:     header,
	})
	if err != nil {
		return nil, err
	}
	return result.QueryResult, nil
}

// Required to implement ClientOutboundInterceptor
func (*interceptor) mustEmbedClientOutboundInterceptorBase() {}
