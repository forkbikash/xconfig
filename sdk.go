package config

import (
	"context"
	"fmt"
)

type Client struct {
	Environment string
	Namespace   string

	configService ConfigService
}

type ClientOption func(*Client)

func NewZkClient(zkServers []string, opts ...ClientOption) *Client {
	configService, err := NewConfigService(zkServers, "/config")
	if err != nil {
		panic(err)
	}

	client := &Client{
		configService: configService,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client
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

func (c *Client) LoadConfigWithWatch(ctx context.Context, configStruct any, callback func(any)) error {
	_, err := c.configService.BindStructWithCallback(ctx, "", c.Environment, c.Namespace, configStruct, callback)
	if err != nil {
		return fmt.Errorf("failed to bind config struct: %w", err)
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
