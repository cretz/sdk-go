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

package headerutil

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

// ContextAware is an optional interface that can be implemented alongside DataConverter.
// This interface allows Temporal to pass Workflow/Activity contexts to the DataConverter
// so that it may tailor it's behaviour.
type ContextAware interface {
	WithWorkflowContext(ctx workflow.Context) converter.DataConverter
	WithContext(ctx context.Context) converter.DataConverter
}

type reader struct {
	header *commonpb.Header
}

func (hr *reader) ForEachKey(handler func(string, *commonpb.Payload) error) error {
	if hr.header == nil {
		return nil
	}
	for key, value := range hr.header.Fields {
		if err := handler(key, value); err != nil {
			return err
		}
	}
	return nil
}

func (hr *reader) Get(key string) (*commonpb.Payload, bool) {
	if hr.header == nil {
		return nil, false
	}
	payload, ok := hr.header.Fields[key]
	return payload, ok
}

// NewReader returns a header reader interface
func NewReader(header *commonpb.Header) workflow.HeaderReader {
	return &reader{header: header}
}

type writer struct {
	header *commonpb.Header
}

func (hw *writer) Set(key string, value *commonpb.Payload) {
	if hw.header == nil {
		return
	}
	hw.header.Fields[key] = value
}

// NewWriter returns a header writer interface
func NewWriter(header *commonpb.Header) workflow.HeaderWriter {
	if header != nil && header.Fields == nil {
		header.Fields = make(map[string]*commonpb.Payload)
	}
	return &writer{header: header}
}

// WithWorkflowContext returns a new DataConverter tailored to the passed Workflow context if
// the DataConverter implements the ContextAware interface. Otherwise the DataConverter is returned
// as-is.
func WithWorkflowContext(ctx workflow.Context, dc converter.DataConverter) converter.DataConverter {
	if d, ok := dc.(ContextAware); ok {
		return d.WithWorkflowContext(ctx)
	}
	return dc
}

// WithContext returns a new DataConverter tailored to the passed Workflow/Activity context if
// the DataConverter implements the ContextAware interface. Otherwise the DataConverter is returned
// as-is. This is generally used for Activity context but can be context for a Workflow if we're
// not yet executing the workflow so do not have a workflow.Context.
func WithContext(ctx context.Context, dc converter.DataConverter) converter.DataConverter {
	if d, ok := dc.(ContextAware); ok {
		return d.WithContext(ctx)
	}
	return dc
}
