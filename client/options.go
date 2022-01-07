package client

import (
	"context"
	"crypto/tls"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
)

const (
	// DefaultHostPort is the host:port which is used if not passed with options.
	DefaultHostPort = "localhost:7233"

	// DefaultNamespace is the namespace name which is used if not passed with options.
	DefaultNamespace = "default"
)

// Options are optional parameters for Client creation.
type Options struct {
	// Optional: To set the host:port for this client to connect to.
	// default: localhost:7233
	//
	// This is a gRPC address and therefore can also support a special-formatted address of "<resolver>:///<value>" that
	// will use a registered resolver. By default all hosts returned from the resolver will be used in a round-robin
	// fashion.
	//
	// The "dns" resolver is registered by default. Using a "dns:///" prefixed address will periodically resolve all IPs
	// for DNS address given and round robin amongst them.
	//
	// A custom resolver can be created to provide multiple hosts in other ways. For example, to manually provide
	// multiple IPs to round-robin across, a google.golang.org/grpc/resolver/manual resolver can be created and
	// registered with google.golang.org/grpc/resolver with a custom scheme:
	//		builder := manual.NewBuilderWithScheme("myresolver")
	//		builder.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: "1.2.3.4:1234"}, {Addr: "2.3.4.5:2345"}}})
	//		resolver.Register(builder)
	//		c, err := client.NewClient(client.Options{HostPort: "myresolver:///ignoredvalue"})
	// Other more advanced resolvers can also be registered.
	HostPort string

	// Optional: To set the namespace name for this client to work with.
	// default: default
	Namespace string

	// Optional: Logger framework can use to log.
	// default: default logger provided.
	Logger log.Logger

	// Optional: Metrics handler for reporting metrics.
	// default: no metrics.
	MetricsHandler MetricsHandler

	// Optional: Sets an identify that can be used to track this host for debugging.
	// default: default identity that include hostname, groupName and process ID.
	Identity string

	// Optional: Sets DataConverter to customize serialization/deserialization of arguments in Temporal
	// default: defaultDataConverter, an combination of google protobuf converter, gogo protobuf converter and json converter
	DataConverter converter.DataConverter

	// Optional: Sets ContextPropagators that allows users to control the context information passed through a workflow
	// default: nil
	ContextPropagators []workflow.ContextPropagator

	// Optional: Sets options for server connection that allow users to control features of connections such as TLS settings.
	// default: no extra options
	ConnectionOptions ConnectionOptions

	// Optional: HeadersProvider will be invoked on every outgoing gRPC request and gives user ability to
	// set custom request headers. This can be used to set auth headers for example.
	HeadersProvider HeadersProvider

	// Optional parameter that is designed to be used *in tests*. It gets invoked last in
	// the gRPC interceptor chain and can be used to induce artificial failures in test scenarios.
	TrafficController TrafficController

	// Interceptors to apply to some calls of the client. Earlier interceptors
	// wrap later interceptors.
	//
	// Any interceptors that also implement Interceptor (meaning they implement
	// WorkerInterceptor in addition to ClientInterceptor) will be used for
	// worker interception as well. When worker interceptors are here and in
	// worker options, the ones here wrap the ones in worker options. The same
	// interceptor should not be set here and in worker options.
	Interceptors []Interceptor
}

// ConnectionOptions are optional parameters that can be specified in Options.
type ConnectionOptions struct {
	// TLS configures connection level security credentials.
	TLS *tls.Config

	// Authority specifies the value to be used as the :authority pseudo-header.
	// This value only used when TLS is nil.
	Authority string

	// By default, after gRPC connection to the Server is created, client will make a request to
	// health check endpoint to make sure that the Server is accessible.
	// Set DisableHealthCheck to true to disable it.
	DisableHealthCheck bool

	// HealthCheckAttemptTimeout specifies how to long to wait for service response on each health check attempt.
	// Default: 5s.
	HealthCheckAttemptTimeout time.Duration

	// HealthCheckTimeout defines how long client should be sending health check requests to the server before concluding
	// that it is unavailable. Defaults to 10s, once this timeout is reached error will be propagated to the client.
	HealthCheckTimeout time.Duration

	// Enables keep alive ping from client to the server, which can help detect abruptly closed connections faster.
	EnableKeepAliveCheck bool

	// After a duration of this time if the client doesn't see any activity it
	// pings the server to see if the transport is still alive.
	// If set below 10s, a minimum value of 10s will be used instead.
	KeepAliveTime time.Duration

	// After having pinged for keepalive check, the client waits for a duration
	// of Timeout and if no activity is seen even after that the connection is
	// closed.
	KeepAliveTimeout time.Duration

	// If true, client sends keepalive pings even with no active RPCs. If false,
	// when there are no active RPCs, Time and Timeout will be ignored and no
	// keepalive pings will be sent.
	KeepAlivePermitWithoutStream bool

	// MaxPayloadSize is a number of bytes that gRPC would allow to travel to and from server. Defaults to 64 MB.
	MaxPayloadSize int

	// Advanced dial options for gRPC connections. These are applied after the internal default dial options are
	// applied. Therefore any dial options here may override internal ones.
	//
	// For gRPC interceptors, internal interceptors such as error handling, metrics, and retrying are done via
	// grpc.WithChainUnaryInterceptor. Therefore to add inner interceptors that are wrapped by those, a
	// grpc.WithChainUnaryInterceptor can be added as an option here. To add a single outer interceptor, a
	// grpc.WithUnaryInterceptor option can be added since grpc.WithUnaryInterceptor is prepended to chains set with
	// grpc.WithChainUnaryInterceptor.
	DialOptions []grpc.DialOption
}

// HeadersProvider returns a map of gRPC headers that should be used on every request.
type HeadersProvider interface {
	GetHeaders(ctx context.Context) (map[string]string, error)
}

// TrafficController is getting called in the interceptor chain with API invocation parameters.
// Result is either nil if API call is allowed or an error, in which case request would be interrupted and
// the error will be propagated back through the interceptor chain.
type TrafficController interface {
	CheckCallAllowed(ctx context.Context, method string, req, reply interface{}) error
}
