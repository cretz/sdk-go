package client

import (
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// HistoryEventIterator represents the interface for
// history event iterator
type HistoryEventIterator interface {
	// HasNext return whether this iterator has next value
	HasNext() bool
	// Next returns the next history events and error
	// The errors it can return:
	//	- serviceerror.NotFound
	//	- serviceerror.InvalidArgument
	//	- serviceerror.Internal
	//	- serviceerror.Unavailable
	Next() (*historypb.HistoryEvent, error)
}

// historyEventIteratorImpl is the implementation of HistoryEventIterator
type historyEventIteratorImpl struct {
	// whether this iterator is initialized
	initialized bool
	// local cached history events and corresponding consuming index
	nextEventIndex int
	events         []*historypb.HistoryEvent
	// token to get next page of history events
	nexttoken []byte
	// err when getting next page of history events
	err error
	// func which use a next token to get next page of history events
	paginate func(nexttoken []byte) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)
}

func (iter *historyEventIteratorImpl) HasNext() bool {
	if iter.nextEventIndex < len(iter.events) || iter.err != nil {
		return true
	} else if !iter.initialized || len(iter.nexttoken) != 0 {
		iter.initialized = true
		response, err := iter.paginate(iter.nexttoken)
		iter.nextEventIndex = 0
		if err == nil {
			iter.events = response.History.Events
			iter.nexttoken = response.NextPageToken
			iter.err = nil
		} else {
			iter.events = nil
			iter.nexttoken = nil
			iter.err = err
		}

		if iter.nextEventIndex < len(iter.events) || iter.err != nil {
			return true
		}
		return false
	}

	return false
}

func (iter *historyEventIteratorImpl) Next() (*historypb.HistoryEvent, error) {
	// if caller call the Next() when iteration is over, just return nil, nil
	if !iter.HasNext() {
		panic("HistoryEventIterator Next() called without checking HasNext()")
	}

	// we have cached events
	if iter.nextEventIndex < len(iter.events) {
		index := iter.nextEventIndex
		iter.nextEventIndex++
		return iter.events[index], nil
	} else if iter.err != nil {
		// we have err, clear that iter.err and return err
		err := iter.err
		iter.err = nil
		return nil, err
	}

	panic("HistoryEventIterator Next() should return either a history event or a err")
}
