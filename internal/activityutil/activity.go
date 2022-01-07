package activityutil

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/errorutil"
)

func ConvertActivityResultToRespondRequest(identity string, taskToken []byte, result *commonpb.Payloads, err error,
	dataConverter converter.DataConverter, namespace string) interface{} {
	if err == errorutil.ErrActivityResultPending {
		// activity result is pending and will be completed asynchronously.
		// nothing to report at this point
		return errorutil.ErrActivityResultPending
	}

	if err == nil {
		return &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    result,
			Identity:  identity,
			Namespace: namespace}
	}

	var canceledErr *errorutil.CanceledError
	if errors.As(err, &canceledErr) {
		return &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Details:   errorutil.ConvertErrDetailsToPayloads(errorutil.RawCanceledErrorDetails(canceledErr), dataConverter),
			Identity:  identity,
			Namespace: namespace}
	}
	if errors.Is(err, context.Canceled) {
		return &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Namespace: namespace}
	}

	return &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Failure:   errorutil.ConvertErrorToFailure(err, dataConverter),
		Identity:  identity,
		Namespace: namespace}
}

func ConvertActivityResultToRespondRequestByID(identity, namespace, workflowID, runID, activityID string,
	result *commonpb.Payloads, err error, dataConverter converter.DataConverter) interface{} {
	if err == errorutil.ErrActivityResultPending {
		// activity result is pending and will be completed asynchronously.
		// nothing to report at this point
		return nil
	}

	if err == nil {
		return &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  namespace,
			WorkflowId: workflowID,
			RunId:      runID,
			ActivityId: activityID,
			Result:     result,
			Identity:   identity}
	}

	var canceledErr *errorutil.CanceledError
	if errors.As(err, &canceledErr) {
		return &workflowservice.RespondActivityTaskCanceledByIdRequest{
			Namespace:  namespace,
			WorkflowId: workflowID,
			RunId:      runID,
			ActivityId: activityID,
			Details:    errorutil.ConvertErrDetailsToPayloads(errorutil.RawCanceledErrorDetails(canceledErr), dataConverter),
			Identity:   identity}
	}

	if errors.Is(err, context.Canceled) {
		return &workflowservice.RespondActivityTaskCanceledByIdRequest{
			Namespace:  namespace,
			WorkflowId: workflowID,
			RunId:      runID,
			ActivityId: activityID,
			Identity:   identity}
	}

	return &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace:  namespace,
		WorkflowId: workflowID,
		RunId:      runID,
		ActivityId: activityID,
		Failure:    errorutil.ConvertErrorToFailure(err, dataConverter),
		Identity:   identity}
}
