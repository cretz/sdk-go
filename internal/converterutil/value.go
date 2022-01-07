package converterutil

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

// EncodedValue is type used to encapsulate/extract encoded result from workflow/activity.
type EncodedValue struct {
	value         *commonpb.Payloads
	dataConverter converter.DataConverter
}

func NewEncodedValue(value *commonpb.Payloads, dc converter.DataConverter) converter.EncodedValue {
	if dc == nil {
		dc = converter.GetDefaultDataConverter()
	}
	return &EncodedValue{value, dc}
}

// Get extract data from encoded data to desired value type. valuePtr is pointer to the actual value type.
func (b EncodedValue) Get(valuePtr interface{}) error {
	if !b.HasValue() {
		return temporal.ErrNoData
	}
	return b.dataConverter.FromPayloads(b.value, valuePtr)
}

// HasValue return whether there is value
func (b EncodedValue) HasValue() bool {
	return b.value != nil
}

// EncodedValues is a type alias used to encapsulate/extract encoded arguments from workflow/activity.
type EncodedValues struct {
	Values        *commonpb.Payloads
	dataConverter converter.DataConverter
}

func NewEncodedValues(values *commonpb.Payloads, dc converter.DataConverter) converter.EncodedValues {
	if dc == nil {
		dc = converter.GetDefaultDataConverter()
	}
	return &EncodedValues{values, dc}
}

// Get extract data from encoded data to desired value type. valuePtr is pointer to the actual value type.
func (b EncodedValues) Get(valuePtr ...interface{}) error {
	if !b.HasValues() {
		return temporal.ErrNoData
	}
	return b.dataConverter.FromPayloads(b.Values, valuePtr...)
}

// HasValues return whether there are values
func (b EncodedValues) HasValues() bool {
	return b.Values != nil
}
