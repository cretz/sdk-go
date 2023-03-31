package internal

import (
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
)

type Registry = registry

func NewRegistry() *Registry { return newRegistry() }

func GetFunctionName(i interface{}) (name string, isMethod bool) { return getFunctionName(i) }

type FixedHistoryIterator struct{ *history.History }

func (f *FixedHistoryIterator) GetNextPage() (*history.History, error) { return f.History, nil }

func (*FixedHistoryIterator) Reset() { panic("Not supported") }

func (*FixedHistoryIterator) HasNextPage() bool { return false }

func NewWorkflowTask(task *workflowservice.PollWorkflowTaskQueueResponse, histIter HistoryIterator) *workflowTask {
	return &workflowTask{task: task, historyIterator: histIter}
}

type WorkerExecutionParameters = workerExecutionParameters

func NewWorkflowTaskHandlerWithAllCapabilities(
	params WorkerExecutionParameters,
	registry *registry,
) WorkflowTaskHandler {
	params.capabilities = &workflowservice.GetSystemInfoResponse_Capabilities{
		SignalAndQueryHeader:            true,
		InternalErrorDifferentiation:    true,
		ActivityFailureIncludeHeartbeat: true,
		SupportsSchedules:               true,
		EncodedFailureAttributes:        true,
		UpsertMemo:                      true,
		EagerWorkflowStart:              true,
		SdkMetadata:                     true,
	}
	params.cache = NewWorkerCache()
	return newWorkflowTaskHandler(params, nil, registry)
}
