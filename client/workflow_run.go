package client

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/util"
	"go.temporal.io/sdk/internal/converterutil"
	"go.temporal.io/sdk/internal/errorutil"
	"go.temporal.io/sdk/temporal"
)

// WorkflowRun represents a started non child workflow
type WorkflowRun interface {
	// GetID return workflow ID, which will be same as StartWorkflowOptions.ID if provided.
	GetID() string

	// GetRunID return the first started workflow run ID (please see below) - empty string if no such run
	GetRunID() string

	// Get will fill the workflow execution result to valuePtr,
	// if workflow execution is a success, or return corresponding,
	// error. This is a blocking API.
	Get(ctx context.Context, valuePtr interface{}) error

	// NOTE: if the started workflow return ContinueAsNewError during the workflow execution, the
	// return result of GetRunID() will be the started workflow run ID, not the new run ID caused by ContinueAsNewError,
	// however, Get(ctx context.Context, valuePtr interface{}) will return result from the run which did not return ContinueAsNewError.
	// Say ExecuteWorkflow started a workflow, in its first run, has run ID "run ID 1", and returned ContinueAsNewError,
	// the second run has run ID "run ID 2" and return some result other than ContinueAsNewError:
	// GetRunID() will always return "run ID 1" and  Get(ctx context.Context, valuePtr interface{}) will return the result of second run.
	// NOTE: DO NOT USE client.ExecuteWorkflow API INSIDE A WORKFLOW, USE workflow.ExecuteChildWorkflow instead
}

type workflowRunImpl struct {
	workflowType  string
	workflowID    string
	firstRunID    string
	currentRunID  *util.OnceCell
	iterFn        func(ctx context.Context, runID string) HistoryEventIterator
	dataConverter converter.DataConverter
}

func (workflowRun *workflowRunImpl) GetRunID() string {
	return workflowRun.currentRunID.Get()
}

func (workflowRun *workflowRunImpl) GetID() string {
	return workflowRun.workflowID
}

func (workflowRun *workflowRunImpl) Get(ctx context.Context, valuePtr interface{}) error {

	iter := workflowRun.iterFn(ctx, workflowRun.currentRunID.Get())
	if !iter.HasNext() {
		panic("could not get last history event for workflow")
	}
	closeEvent, err := iter.Next()
	if err != nil {
		return err
	}

	switch closeEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		attributes := closeEvent.GetWorkflowExecutionCompletedEventAttributes()
		if attributes.NewExecutionRunId != "" {
			return workflowRun.follow(ctx, valuePtr, attributes.NewExecutionRunId)
		}
		if valuePtr == nil || attributes.Result == nil {
			return nil
		}
		rf := reflect.ValueOf(valuePtr)
		if rf.Type().Kind() != reflect.Ptr {
			return errors.New("value parameter is not a pointer")
		}
		return workflowRun.dataConverter.FromPayloads(attributes.Result, valuePtr)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attributes := closeEvent.GetWorkflowExecutionFailedEventAttributes()
		if attributes.NewExecutionRunId != "" {
			return workflowRun.follow(ctx, valuePtr, attributes.NewExecutionRunId)
		}
		err = errorutil.ConvertFailureToError(attributes.GetFailure(), workflowRun.dataConverter)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		attributes := closeEvent.GetWorkflowExecutionCanceledEventAttributes()
		details := converterutil.NewEncodedValues(attributes.Details, workflowRun.dataConverter)
		err = temporal.NewCanceledError(details)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		err = errorutil.NewTerminatedError()
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		attributes := closeEvent.GetWorkflowExecutionTimedOutEventAttributes()
		if attributes.NewExecutionRunId != "" {
			return workflowRun.follow(ctx, valuePtr, attributes.NewExecutionRunId)
		}
		err = errorutil.NewTimeoutError("Workflow timeout", enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		attributes := closeEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
		return workflowRun.follow(ctx, valuePtr, attributes.NewExecutionRunId)
	default:
		return fmt.Errorf("unexpected event type %s when handling workflow execution result", closeEvent.GetEventType())
	}

	err = errorutil.NewWorkflowExecutionError(
		workflowRun.workflowID,
		workflowRun.currentRunID.Get(),
		workflowRun.workflowType,
		err)

	return err
}

// follow is used by Get to follow a chain of executions linked by NewExecutionRunId, so that Get
// doesn't return until the chain finishes. These can be ContinuedAsNew events, Completed events
// (for workflows with a cron schedule), or Failed or TimedOut events (for workflows with a retry
// policy or cron schedule).
func (workflowRun *workflowRunImpl) follow(ctx context.Context, valuePtr interface{}, newRunID string) error {
	curRunID := util.PopulatedOnceCell(newRunID)
	workflowRun.currentRunID = &curRunID
	return workflowRun.Get(ctx, valuePtr)
}
