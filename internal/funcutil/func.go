package funcutil

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"go.temporal.io/sdk/workflow"
)

func getKind(fType reflect.Type) reflect.Kind {
	if fType == nil {
		return reflect.Invalid
	}
	return fType.Kind()
}

func ValidateFunctionArgs(fn interface{}, args []interface{}, workflow bool) error {
	fType := reflect.TypeOf(fn)
	switch getKind(fType) {
	case reflect.String:
		// We can't validate function passed as string.
		return nil
	case reflect.Func:
	default:
		return fmt.Errorf(
			"invalid type 'workflowFunc' parameter provided, it can be either worker function or function name: %v",
			fn)
	}

	fnName, _ := GetFunctionName(fn)
	fnArgIndex := 0
	// Skip Context function argument.
	if fType.NumIn() > 0 {
		if workflow && isWorkflowContext(fType.In(0)) {
			fnArgIndex++
		}
		if !workflow && isActivityContext(fType.In(0)) {
			fnArgIndex++
		}
	}

	// Validate provided args match with function order match.
	if fType.NumIn()-fnArgIndex != len(args) {
		return fmt.Errorf(
			"expected %d args for function: %v but found %v",
			fType.NumIn()-fnArgIndex, fnName, len(args))
	}

	for i := 0; fnArgIndex < fType.NumIn(); fnArgIndex, i = fnArgIndex+1, i+1 {
		fnArgType := fType.In(fnArgIndex)
		argType := reflect.TypeOf(args[i])
		if argType != nil && !argType.AssignableTo(fnArgType) {
			return fmt.Errorf(
				"cannot assign function argument: %d from type: %s to type: %s",
				fnArgIndex+1, argType, fnArgType,
			)
		}
	}

	return nil
}

func GetFunctionName(i interface{}) (name string, isMethod bool) {
	if fullName, ok := i.(string); ok {
		return fullName, false
	}
	fullName := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	// Full function name that has a struct pointer receiver has the following format
	// <prefix>.(*<type>).<function>
	isMethod = strings.ContainsAny(fullName, "*")
	elements := strings.Split(fullName, ".")
	shortName := elements[len(elements)-1]
	// This allows to call activities by method pointer
	// Compiler adds -fm suffix to a function name which has a receiver
	// Note that this works even if struct pointer used to get the function is nil
	// It is possible because nil receivers are allowed.
	// For example:
	// var a *Activities
	// ExecuteActivity(ctx, a.Foo)
	// will call this function which is going to return "Foo"
	return strings.TrimSuffix(shortName, "-fm"), isMethod
}

func isWorkflowContext(inType reflect.Type) bool {
	// NOTE: We don't expect any one to derive from workflow context.
	return inType == reflect.TypeOf((*workflow.Context)(nil)).Elem()
}

func isActivityContext(inType reflect.Type) bool {
	contextElem := reflect.TypeOf((*context.Context)(nil)).Elem()
	return inType != nil && inType.Implements(contextElem)
}
