package config

import (
	"context"
	"sync"
	"time"
)

type ConfigValue struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

type ConfigChangeEvent struct {
	Path       string      `json:"path"`
	OldValue   ConfigValue `json:"old_value,omitempty"`
	NewValue   ConfigValue `json:"new_value,omitempty"`
	ChangeType string      `json:"change_type"`
	Timestamp  time.Time   `json:"timestamp"`
}

type ConfigChangeHandler func(event ConfigChangeEvent)

type ConfigStructBinding struct {
	Path           string
	StructPtr      any
	UpdateCallback func(any)
	subscriptionID string
	mu             sync.RWMutex
}

type ConfigService interface {
	Get(ctx context.Context, path string, watch ...bool) (ConfigValue, error)
	Set(ctx context.Context, path string, value any, watch ...bool) error
	Delete(ctx context.Context, path string) error
	Exists(ctx context.Context, path string) (bool, error)
	List(ctx context.Context, path string) ([]string, error)

	Subscribe(ctx context.Context, path string, handler ConfigChangeHandler) (string, error)
	Unsubscribe(subscriptionID string) error

	GetEffective(ctx context.Context, path string) (ConfigValue, error)

	SetBatch(ctx context.Context, configs map[string]any, watch ...bool) error
	GetBatch(ctx context.Context, paths []string, watch ...bool) (map[string]ConfigValue, error)

	Export(ctx context.Context, rootPath string) (map[string]ConfigValue, error)
	Import(ctx context.Context, configs map[string]ConfigValue, watch ...bool) error

	BindStructWithCallback(ctx context.Context, path string, structPtr any, callback func(any)) (*ConfigStructBinding, error)
	UnbindStruct(binding *ConfigStructBinding) error
	ReloadStruct(ctx context.Context, binding *ConfigStructBinding) error

	SetFromStruct(ctx context.Context, path string, structPtr any) error

	Close() error
}
