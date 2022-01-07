package clientutil

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/temporal"
)

func RetryPolicyToProto(retryPolicy *temporal.RetryPolicy) *commonpb.RetryPolicy {
	if retryPolicy == nil {
		return nil
	}

	return &commonpb.RetryPolicy{
		MaximumInterval:        &retryPolicy.MaximumInterval,
		InitialInterval:        &retryPolicy.InitialInterval,
		BackoffCoefficient:     retryPolicy.BackoffCoefficient,
		MaximumAttempts:        retryPolicy.MaximumAttempts,
		NonRetryableErrorTypes: retryPolicy.NonRetryableErrorTypes,
	}
}

func RetryPolicyFromProto(retryPolicy *commonpb.RetryPolicy) *temporal.RetryPolicy {
	if retryPolicy == nil {
		return nil
	}

	p := temporal.RetryPolicy{
		BackoffCoefficient:     retryPolicy.BackoffCoefficient,
		MaximumAttempts:        retryPolicy.MaximumAttempts,
		NonRetryableErrorTypes: retryPolicy.NonRetryableErrorTypes,
	}

	// Avoid nil pointer dereferences
	if v := retryPolicy.MaximumInterval; v != nil {
		p.MaximumInterval = *v
	}
	if v := retryPolicy.InitialInterval; v != nil {
		p.InitialInterval = *v
	}

	return &p
}
