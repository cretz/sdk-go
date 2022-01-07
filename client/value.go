package client

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/converterutil"
)

// NewValue creates a new converter.EncodedValue which can be used to decode binary data returned by Temporal.  For example:
// User had Activity.RecordHeartbeat(ctx, "my-heartbeat") and then got response from calling Client.DescribeWorkflowExecution.
// The response contains binary field PendingActivityInfo.HeartbeatDetails,
// which can be decoded by using:
//   var result string // This need to be same type as the one passed to RecordHeartbeat
//   NewValue(data).Get(&result)
func NewValue(data *commonpb.Payloads) converter.EncodedValue {
	return converterutil.NewEncodedValue(data, nil)
}

// NewValues creates a new converter.EncodedValues which can be used to decode binary data returned by Temporal. For example:
// User had Activity.RecordHeartbeat(ctx, "my-heartbeat", 123) and then got response from calling Client.DescribeWorkflowExecution.
// The response contains binary field PendingActivityInfo.HeartbeatDetails,
// which can be decoded by using:
//   var result1 string
//   var result2 int // These need to be same type as those arguments passed to RecordHeartbeat
//   NewValues(data).Get(&result1, &result2)
func NewValues(data *commonpb.Payloads) converter.EncodedValues {
	return converterutil.NewEncodedValues(data, nil)
}
