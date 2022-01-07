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

package client

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/status"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"go.temporal.io/api/serviceerror"
	internal_backoff "go.temporal.io/sdk/internal/common/backoff"
	"go.temporal.io/sdk/internal/common/retry"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

type GRPCContextOptions struct {
	Timeout            time.Duration
	MetricsHandler     MetricsHandler
	Headers            metadata.MD
	LongPoll           bool
	DefaultRetryPolicy bool
}

func NewGRPCContext(ctx context.Context, options GRPCContextOptions) (context.Context, context.CancelFunc) {
	if options.Timeout == 0 {
		options.Timeout = defaultRPCTimeout
		// Set rpc timeout less than context timeout to allow for retries when call gets lost
		now := time.Now()
		if deadline, ok := ctx.Deadline(); ok && deadline.After(now) {
			options.Timeout = deadline.Sub(now) / 2
			// Make sure to not set rpc timeout lower than minRPCTimeout
			if options.Timeout < minRPCTimeout {
				options.Timeout = minRPCTimeout
			} else if options.Timeout > maxRPCTimeout {
				options.Timeout = maxRPCTimeout
			}
		}
	}

	if options.Headers == nil {
		options.Headers = metadata.MD{}
	}
	options.Headers.Set(clientNameHeaderName, clientNameHeaderValue)
	options.Headers.Set(clientVersionHeaderName, temporal.SDKVersion)
	options.Headers.Set(supportedServerVersionsHeaderName, supportedServerVersions)
	ctx = metadata.NewOutgoingContext(ctx, options.Headers)

	if options.MetricsHandler != nil {
		ctx = context.WithValue(ctx, metricsHandlerContextKey{}, options.MetricsHandler)
	}

	ctx = context.WithValue(ctx, metricsLongPollContextKey{}, options.LongPoll)

	if options.DefaultRetryPolicy {
		ctx = context.WithValue(ctx, retry.ConfigKey, createDynamicServiceRetryPolicy(ctx).GrpcRetryConfig())
	}

	return context.WithTimeout(ctx, options.Timeout)
}

// dialParameters are passed to GRPCDialer and must be used to create gRPC connection.
type dialParameters struct {
	HostPort              string
	UserConnectionOptions ConnectionOptions
	RequiredInterceptors  []grpc.UnaryClientInterceptor
	DefaultServiceConfig  string
}

func newDialParameters(options *Options) dialParameters {
	return dialParameters{
		UserConnectionOptions: options.ConnectionOptions,
		HostPort:              options.HostPort,
		RequiredInterceptors:  requiredInterceptors(options.MetricsHandler, options.HeadersProvider, options.TrafficController),
		DefaultServiceConfig:  defaultServiceConfig,
	}
}

const (
	// defaultServiceConfig is a default gRPC connection service config which enables DNS round-robin between IPs.
	defaultServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`

	// minConnectTimeout is the minimum amount of time we are willing to give a connection to complete.
	minConnectTimeout = 20 * time.Second

	// attemptSuffix is a suffix added to the metric name for individual call attempts made to the server, which includes retries.
	attemptSuffix = "_attempt"
	// mb is a number of bytes in a megabyte
	mb = 1024 * 1024
	// defaultMaxPayloadSize is a maximum size of the payload that grpc client would allow.
	defaultMaxPayloadSize = 64 * mb

	retryPollOperationInitialInterval = 200 * time.Millisecond
	retryPollOperationMaxInterval     = 10 * time.Second

	clientNameHeaderName              = "client-name"
	clientNameHeaderValue             = "temporal-go"
	clientVersionHeaderName           = "client-version"
	supportedServerVersionsHeaderName = "supported-server-versions"

	// defaultRPCTimeout is the default gRPC call timeout.
	defaultRPCTimeout = 10 * time.Second
	// minRPCTimeout is minimum gRPC call timeout allowed.
	minRPCTimeout = 1 * time.Second
	// maxRPCTimeout is maximum gRPC call timeout allowed (should not be less than defaultRPCTimeout).
	maxRPCTimeout = 10 * time.Second

	// supportedServerVersions is a semver rages (https://github.com/blang/semver#ranges) of server versions that
	// are supported by this Temporal SDK.
	// Server validates if its version fits into SupportedServerVersions range and rejects request if it doesn't.
	supportedServerVersions = ">=1.0.0 <2.0.0"
)

func dial(params dialParameters) (*grpc.ClientConn, error) {
	var securityOptions []grpc.DialOption
	if params.UserConnectionOptions.TLS != nil {
		securityOptions = []grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(params.UserConnectionOptions.TLS)),
		}
	} else {
		securityOptions = []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithAuthority(params.UserConnectionOptions.Authority),
		}
	}

	maxPayloadSize := defaultMaxPayloadSize
	if params.UserConnectionOptions.MaxPayloadSize != 0 {
		maxPayloadSize = params.UserConnectionOptions.MaxPayloadSize
	}

	// gRPC maintains connection pool inside grpc.ClientConn.
	// This connection pool has auto reconnect feature.
	// If connection goes down, gRPC will try to reconnect using exponential backoff strategy:
	// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md.
	// Default MaxDelay is 120 seconds which is too high.
	// Setting it to retryPollOperationMaxInterval here will correlate with poll reconnect interval.
	var cp = grpc.ConnectParams{
		Backoff:           backoff.DefaultConfig,
		MinConnectTimeout: minConnectTimeout,
	}
	cp.Backoff.BaseDelay = retryPollOperationInitialInterval
	cp.Backoff.MaxDelay = retryPollOperationMaxInterval
	opts := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(params.RequiredInterceptors...),
		grpc.WithDefaultServiceConfig(params.DefaultServiceConfig),
		grpc.WithConnectParams(cp),
	}

	opts = append(opts, securityOptions...)
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxPayloadSize)))
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxPayloadSize)))

	if params.UserConnectionOptions.EnableKeepAliveCheck {
		// gRPC utilizes keep alive mechanism to detect dead connections in case if server didn't close them
		// gracefully. Client would ping the server periodically and expect replies withing the specified timeout.
		// Learn more by reading https://github.com/grpc/grpc/blob/master/doc/keepalive.md
		var kap = keepalive.ClientParameters{
			Time:                params.UserConnectionOptions.KeepAliveTime,
			Timeout:             params.UserConnectionOptions.KeepAliveTimeout,
			PermitWithoutStream: params.UserConnectionOptions.KeepAlivePermitWithoutStream,
		}
		opts = append(opts, grpc.WithKeepaliveParams(kap))
	}

	// Append any user-supplied options
	opts = append(opts, params.UserConnectionOptions.DialOptions...)

	return grpc.Dial(params.HostPort, opts...)
}

func requiredInterceptors(
	metricsHandler MetricsHandler,
	headersProvider HeadersProvider,
	controller TrafficController,
) []grpc.UnaryClientInterceptor {
	interceptors := []grpc.UnaryClientInterceptor{
		errorInterceptor,
		// Report aggregated metrics for the call, this is done outside of the retry loop.
		newMetricsGRPCInterceptor(metricsHandler, ""),
		// By default the grpc retry interceptor *is disabled*, preventing accidental use of retries.
		// We add call options for retry configuration based on the values present in the context.
		retry.NewRetryOptionsInterceptor(),
		// Performs retries *IF* retry options are set for the call.
		grpc_retry.UnaryClientInterceptor(),
		// Report metrics for every call made to the server.
		newMetricsGRPCInterceptor(metricsHandler, attemptSuffix),
	}
	if headersProvider != nil {
		interceptors = append(interceptors, headersProviderInterceptor(headersProvider))
	}
	if controller != nil {
		interceptors = append(interceptors, trafficControllerInterceptor(controller))
	}
	return interceptors
}

func trafficControllerInterceptor(controller TrafficController) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := controller.CheckCallAllowed(ctx, method, req, reply)
		// Break execution chain and return an error without sending actual request to the server.
		if err != nil {
			return err
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func headersProviderInterceptor(headersProvider HeadersProvider) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		headers, err := headersProvider.GetHeaders(ctx)
		if err != nil {
			return err
		}
		for k, v := range headers {
			ctx = metadata.AppendToOutgoingContext(ctx, k, v)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func errorInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	err = serviceerror.FromStatus(status.Convert(err))
	return err
}

const (
	retryServiceOperationInitialInterval    = 200 * time.Millisecond
	retryServiceOperationExpirationInterval = 60 * time.Second
	retryServiceOperationBackoff            = 2
)

// Creates a retry policy which allows appropriate retries for the deadline passed in as context.
// It uses the context deadline to set MaxInterval as 1/10th of context timeout
// MaxInterval = Max(context_timeout/10, 20ms)
// defaults to ExpirationInterval of 60 seconds, or uses context deadline as expiration interval
func createDynamicServiceRetryPolicy(ctx context.Context) internal_backoff.RetryPolicy {
	timeout := retryServiceOperationExpirationInterval
	if ctx != nil {
		now := time.Now()
		if expiration, ok := ctx.Deadline(); ok && expiration.After(now) {
			timeout = expiration.Sub(now)
		}
	}
	initialInterval := retryServiceOperationInitialInterval
	maximumInterval := timeout / 10
	if maximumInterval < retryServiceOperationInitialInterval {
		maximumInterval = retryServiceOperationInitialInterval
	}

	policy := internal_backoff.NewExponentialRetryPolicy(initialInterval)
	policy.SetBackoffCoefficient(retryServiceOperationBackoff)
	policy.SetMaximumInterval(maximumInterval)
	policy.SetExpirationInterval(timeout)
	return policy
}

const (
	healthCheckServiceName           = "temporal.api.workflowservice.v1.WorkflowService"
	defaultHealthCheckAttemptTimeout = 5 * time.Second
	defaultHealthCheckTimeout        = 10 * time.Second
)

// checkHealth checks service health using gRPC health check:
// https://github.com/grpc/grpc/blob/master/doc/health-checking.md
func checkHealth(connection grpc.ClientConnInterface, options ConnectionOptions) error {
	if options.DisableHealthCheck {
		return nil
	}

	healthClient := healthpb.NewHealthClient(connection)

	request := &healthpb.HealthCheckRequest{
		Service: healthCheckServiceName,
	}

	attemptTimeout := options.HealthCheckAttemptTimeout
	if attemptTimeout == 0 {
		attemptTimeout = defaultHealthCheckAttemptTimeout
	}
	timeout := options.HealthCheckTimeout
	if timeout == 0 {
		timeout = defaultHealthCheckTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	policy := createDynamicServiceRetryPolicy(ctx)
	// TODO: refactor using grpc retry interceptor
	return internal_backoff.Retry(ctx, func() error {
		healthCheckCtx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		defer cancel()
		resp, err := healthClient.Check(healthCheckCtx, request)
		if err != nil {
			return fmt.Errorf("health check error: %w", err)
		}
		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("health check returned unhealthy status: %v", resp.Status)
		}
		return nil
	}, policy, nil)
}
