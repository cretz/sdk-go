package client

import (
	"context"

	"go.temporal.io/sdk/converter"
)

// Interceptor for providing an OutboundInterceptor to intercept certain
// workflow-specific client calls from the SDK. See documentation in the
// interceptor package for more details.
type Interceptor interface {
	// This is called on client creation if set via client options
	InterceptClient(next OutboundInterceptor) OutboundInterceptor

	mustEmbedClientInterceptorBase()
}

// OutboundInterceptor is an interface for certain workflow-specific calls
// originating from the SDK. See documentation in the interceptor package for
// more details.
type OutboundInterceptor interface {
	// ExecuteWorkflow intercepts client.Client.ExecuteWorkflow.
	// interceptor.Header will return a non-nil map for this context.
	ExecuteWorkflow(context.Context, *ClientExecuteWorkflowInput) (WorkflowRun, error)

	// SignalWorkflow intercepts client.Client.SignalWorkflow.
	// interceptor.Header will return a non-nil map for this context.
	SignalWorkflow(context.Context, *ClientSignalWorkflowInput) error

	// SignalWithStartWorkflow intercepts client.Client.SignalWithStartWorkflow.
	// interceptor.Header will return a non-nil map for this context.
	SignalWithStartWorkflow(context.Context, *ClientSignalWithStartWorkflowInput) (WorkflowRun, error)

	// CancelWorkflow intercepts client.Client.CancelWorkflow.
	CancelWorkflow(context.Context, *ClientCancelWorkflowInput) error

	// TerminateWorkflow intercepts client.Client.TerminateWorkflow.
	TerminateWorkflow(context.Context, *ClientTerminateWorkflowInput) error

	// QueryWorkflow intercepts client.Client.QueryWorkflow.
	// interceptor.Header will return a non-nil map for this context.
	QueryWorkflow(context.Context, *ClientQueryWorkflowInput) (converter.EncodedValue, error)

	mustEmbedClientOutboundInterceptorBase()
}

// ClientExecuteWorkflowInput is the input to
// ClientOutboundInterceptor.ExecuteWorkflow.
type ClientExecuteWorkflowInput struct {
	Options      *StartWorkflowOptions
	WorkflowType string
	Args         []interface{}
}

// ClientSignalWorkflowInput is the input to
// ClientOutboundInterceptor.SignalWorkflow.
type ClientSignalWorkflowInput struct {
	WorkflowID string
	RunID      string
	SignalName string
	Arg        interface{}
}

// ClientSignalWithStartWorkflowInput is the input to
// ClientOutboundInterceptor.SignalWithStartWorkflow.
type ClientSignalWithStartWorkflowInput struct {
	SignalName   string
	SignalArg    interface{}
	Options      *StartWorkflowOptions
	WorkflowType string
	Args         []interface{}
}

// ClientCancelWorkflowInput is the input to
// ClientOutboundInterceptor.CancelWorkflow.
type ClientCancelWorkflowInput struct {
	WorkflowID string
	RunID      string
}

// ClientTerminateWorkflowInput is the input to
// ClientOutboundInterceptor.TerminateWorkflow.
type ClientTerminateWorkflowInput struct {
	WorkflowID string
	RunID      string
	Reason     string
	Details    []interface{}
}

// ClientQueryWorkflowInput is the input to
// ClientOutboundInterceptor.QueryWorkflow.
type ClientQueryWorkflowInput struct {
	WorkflowID string
	RunID      string
	QueryType  string
	Args       []interface{}
}
