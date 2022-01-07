package workflowutil

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

func GetWorkflowMemo(input map[string]interface{}, dc converter.DataConverter) (*commonpb.Memo, error) {
	if input == nil {
		return nil, nil
	}

	memo := make(map[string]*commonpb.Payload)
	for k, v := range input {
		// TODO (shtin): use dc here???
		memoBytes, err := converter.GetDefaultDataConverter().ToPayload(v)
		if err != nil {
			return nil, fmt.Errorf("encode workflow memo error: %v", err.Error())
		}
		memo[k] = memoBytes
	}
	return &commonpb.Memo{Fields: memo}, nil
}

func SerializeSearchAttributes(input map[string]interface{}) (*commonpb.SearchAttributes, error) {
	if input == nil {
		return nil, nil
	}

	attr := make(map[string]*commonpb.Payload)
	for k, v := range input {
		attrBytes, err := converter.GetDefaultDataConverter().ToPayload(v)
		if err != nil {
			return nil, fmt.Errorf("encode search attribute [%s] error: %v", k, err)
		}
		attr[k] = attrBytes
	}
	return &commonpb.SearchAttributes{IndexedFields: attr}, nil
}
