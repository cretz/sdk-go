package client

import (
	"context"
	"io"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/internal/common/metrics"
	ilog "go.temporal.io/sdk/internal/log"
	"go.temporal.io/sdk/log"
)

// NamespaceClient is the client for managing operations on the namespace.
// CLI, tools, ... can use this layer to manager operations on namespace.
type NamespaceClient interface {
	// Register a namespace with temporal server
	// The errors it can throw:
	//	- NamespaceAlreadyExistsError
	//	- serviceerror.InvalidArgument
	//	- serviceerror.Internal
	//	- serviceerror.Unavailable
	Register(ctx context.Context, request *workflowservice.RegisterNamespaceRequest) error

	// Describe a namespace. The namespace has 3 part of information
	// NamespaceInfo - Which has Name, Status, Description, Owner Email
	// NamespaceConfiguration - Configuration like Workflow Execution Retention Period In Days, Whether to emit metrics.
	// ReplicationConfiguration - replication config like clusters and active cluster name
	// The errors it can throw:
	//	- serviceerror.NotFound
	//	- serviceerror.InvalidArgument
	//	- serviceerror.Internal
	//	- serviceerror.Unavailable
	Describe(ctx context.Context, name string) (*workflowservice.DescribeNamespaceResponse, error)

	// Update a namespace.
	// The errors it can throw:
	//	- serviceerror.NotFound
	//	- serviceerror.InvalidArgument
	//	- serviceerror.Internal
	//	- serviceerror.Unavailable
	Update(ctx context.Context, request *workflowservice.UpdateNamespaceRequest) error

	// Close client and clean up underlying resources.
	Close()
}

// NewNamespaceClient creates an instance of a namespace client, to manage lifecycle of namespaces.
func NewNamespaceClient(options Options) (NamespaceClient, error) {
	// 	// Initialize root tags
	if options.MetricsHandler == nil {
		options.MetricsHandler = MetricsNopHandler
	}
	options.MetricsHandler = options.MetricsHandler.WithTags(metrics.RootTags(metrics.NoneTagValue))

	if options.HostPort == "" {
		options.HostPort = DefaultHostPort
	}

	if options.Identity == "" {
		options.Identity = defaultIdentity()
	}

	if options.Logger == nil {
		options.Logger = ilog.NewDefaultLogger()
	}

	connection, err := dial(newDialParameters(&options))
	if err != nil {
		return nil, err
	}

	if err = checkHealth(connection, options.ConnectionOptions); err != nil {
		if err := connection.Close(); err != nil {
			options.Logger.Warn("Unable to close connection on health check failure.", "error", err)
		}
		return nil, err
	}

	return &namespaceClient{
		service:          workflowservice.NewWorkflowServiceClient(connection),
		connectionCloser: connection,
		metricsHandler:   options.MetricsHandler,
		logger:           options.Logger,
		identity:         options.Identity,
	}, nil
}

type namespaceClient struct {
	service          workflowservice.WorkflowServiceClient
	connectionCloser io.Closer
	metricsHandler   MetricsHandler
	logger           log.Logger
	identity         string
}

func (c *namespaceClient) Register(ctx context.Context, request *workflowservice.RegisterNamespaceRequest) error {
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	var err error
	_, err = c.service.RegisterNamespace(ctx, request)
	return err
}

func (c *namespaceClient) Describe(ctx context.Context, namespace string) (*workflowservice.DescribeNamespaceResponse, error) {
	request := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}

	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	response, err := c.service.DescribeNamespace(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (nc *namespaceClient) Update(ctx context.Context, request *workflowservice.UpdateNamespaceRequest) error {
	ctx, cancel := NewGRPCContext(ctx, GRPCContextOptions{DefaultRetryPolicy: true})
	defer cancel()
	_, err := nc.service.UpdateNamespace(ctx, request)
	return err
}

// Close client and clean up underlying resources.
func (c *namespaceClient) Close() {
	if c.connectionCloser == nil {
		return
	}
	if err := c.connectionCloser.Close(); err != nil {
		c.logger.Warn("unable to close connection", ilog.TagError, err)
	}
}
