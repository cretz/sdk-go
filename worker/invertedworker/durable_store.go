package invertedworker

import (
	"context"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/history/v1"
)

type DurableStore interface {
	AppendPartialHistory(context.Context, *common.WorkflowExecution, *history.History) error
	GetHistory(context.Context, *common.WorkflowExecution) (*history.History, error)
	PurgeHistory(context.Context, *common.WorkflowExecution) error
}
