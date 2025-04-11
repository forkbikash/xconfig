package config

import (
	"context"
)

type Client struct {
	Environment string
	Namespace   string

	configService ConfigService
}

type ClientOption func(*Client)

func NewZkClient(zkServers []string, opts ...ClientOption) (*Client, error) {
	// Default config service options
	serviceOpts := []Option{
		WithBasePath("/config"),
		WithDelimiter("::"),
	}

	configService, err := NewConfigService(zkServers, serviceOpts...)
	if err != nil {
		return nil, FormatError(ErrCodeConnection, err, "failed to create config service")
	}

	client := &Client{
		configService: configService,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

func WithEnvironment(env string) ClientOption {
	return func(c *Client) {
		c.Environment = env
	}
}

func WithNamespace(namespace string) ClientOption {
	return func(c *Client) {
		c.Namespace = namespace
	}
}

func (c *Client) LoadConfigW(ctx context.Context, configStruct any, callback func(any)) error {
	_, err := c.configService.BindStructWithCallback(ctx, "", c.Environment, c.Namespace, configStruct, callback)
	if err != nil {
		return FormatError(ErrCodeInternal, err, "failed to bind config struct")
	}

	return nil
}

func (c *Client) SetConfig(ctx context.Context, configStruct any) error {
	return c.configService.SetFromStruct(ctx, "", c.Environment, c.Namespace, configStruct)
}

func (c *Client) Close() error {
	err := c.configService.Close()
	return err
}
