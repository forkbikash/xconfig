package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/zk"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

var v1 = viper.NewWithOptions(viper.KeyDelimiter("::"))

func init() {
	v1.SetConfigType("json")
}

// configServiceImpl implements the ConfigService interface
type configServiceImpl struct {
	zkConn           *zk.Conn
	baseZkPath       string
	subscriptions    map[string]subscription
	subscriptionsMu  sync.RWMutex
	structBindings   map[*ConfigStructBinding]bool
	structBindingsMu sync.RWMutex
	notificationChan chan ConfigChangeEvent
	closeChan        chan struct{}
	wg               sync.WaitGroup
}

type subscription struct {
	path    string
	handler ConfigChangeHandler
}

// NewConfigService creates a new configuration service
func NewConfigService(zkServers []string, basePath string) (ConfigService, error) {
	conn, _, err := zk.Connect(zkServers, time.Second*60)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ZooKeeper: %w", err)
	}

	// Ensure base path exists
	exists, _, err := conn.Exists(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to check base path: %w", err)
	}

	if !exists {
		_, err = conn.Create(basePath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return nil, fmt.Errorf("failed to create base path: %w", err)
		}
	}

	service := &configServiceImpl{
		zkConn:           conn,
		baseZkPath:       basePath,
		subscriptions:    make(map[string]subscription),
		structBindings:   make(map[*ConfigStructBinding]bool),
		notificationChan: make(chan ConfigChangeEvent, 100),
		closeChan:        make(chan struct{}),
	}

	// Start the notification dispatcher
	service.wg.Add(1)
	go service.notificationDispatcher()

	return service, nil
}

// notificationDispatcher distributes config change events to subscribers
func (c *configServiceImpl) notificationDispatcher() {
	defer c.wg.Done()

	for {
		select {
		case <-c.closeChan:
			return
		case event := <-c.notificationChan:
			c.dispatchEvent(event)
		}
	}
}

// dispatchEvent sends notifications to relevant subscribers
func (c *configServiceImpl) dispatchEvent(event ConfigChangeEvent) {
	c.subscriptionsMu.RLock()
	defer c.subscriptionsMu.RUnlock()

	for _, sub := range c.subscriptions {
		// Check if subscription path is a prefix of or equal to the event path
		if strings.HasPrefix(event.Path, sub.path) {
			sub.handler(event)
		}
	}
}

// getZkPath converts a logical path to ZooKeeper path
func (c *configServiceImpl) getZkPath(logicalPath string) string {
	return path.Join(c.baseZkPath, logicalPath)
}

// watchZkNodeRecurse sets up a watcher on a ZooKeeper node
func (c *configServiceImpl) watchZkNodeRecurse(ctx context.Context, path string) error {
	zkPath := c.getZkPath(path)
	log.Println("Watching ZK node", zkPath)

	// Set up a watch on the node
	go func() {
		ch, err := c.zkConn.AddWatchCtx(ctx, zkPath, true)
		if err != nil {
			return
		}

		for e := range ch {
			// zk.Event {Type: EventNodeDataChanged (3), State: 3, Path: "/config/ecommerce::prod::database::pool_size", Err: error nil, Server: ""}
			// zk.Event {Type: EventNodeCreated (1), State: 3, Path: "/config/ecommerce::prod::database::pool_size", Err: error nil, Server: ""}
			// zk.Event {Type: EventNodeDeleted (2), State: 3, Path: "/config/ecommerce::prod::database::pool_size", Err: error nil, Server: ""}
			switch e.Type {
			case zk.EventNodeCreated:
				log.Println("ZK node created", e.Path)
				// Node was created, notify subscribers
				data, _, err := c.zkConn.Get(e.Path)
				if err == nil {
					var value ConfigValue
					if err := json.Unmarshal(data, &value); err == nil {
						c.notificationChan <- ConfigChangeEvent{
							Path:       strings.TrimPrefix(e.Path, "/config"),
							NewValue:   value,
							ChangeType: Created,
							Timestamp:  time.Now(),
						}
					}
				}

			case zk.EventNodeDataChanged:
				log.Println("ZK node data changed", e.Path)
				// Data changed, notify subscribers
				newData, _, err := c.zkConn.Get(e.Path)
				if err == nil {
					var newValue ConfigValue
					if err := json.Unmarshal(newData, &newValue); err == nil {
						c.notificationChan <- ConfigChangeEvent{
							Path: strings.TrimPrefix(e.Path, "/config"),
							// OldValue:   oldValue,
							NewValue:   newValue,
							ChangeType: Updated,
							Timestamp:  time.Now(),
						}
					}
				}
			}
		}
	}()

	return nil
}

// watchZkNode sets up a watcher on a ZooKeeper node
func (c *configServiceImpl) watchZkNode(path string) error {
	zkPath := c.getZkPath(path)
	log.Println("Watching ZK node", zkPath)

	// Set up a watch on the node
	go func() {
		for {
			exists, _, ch, err := c.zkConn.ExistsW(zkPath)
			if err != nil {
				// Log error and retry after a delay
				continue
			}

			if !exists {
				// If node doesn't exist, watch for creation
				event := <-ch
				if event.Type == zk.EventNodeCreated {
					// Node was created, notify subscribers
					data, _, err := c.zkConn.Get(zkPath)
					if err == nil {
						var value ConfigValue
						if err := json.Unmarshal(data, &value); err == nil {
							c.notificationChan <- ConfigChangeEvent{
								Path:       path,
								NewValue:   value,
								ChangeType: Created,
								Timestamp:  time.Now(),
							}
						}
					}
				}
				continue
			}

			// Node exists, watch for changes or deletion
			data, _, ch, err := c.zkConn.GetW(zkPath)
			if err != nil {
				// Log error and retry
				continue
			}

			var oldValue ConfigValue
			if err := json.Unmarshal(data, &oldValue); err != nil {
				// Log error but continue watching
			}

			event := <-ch
			switch event.Type {
			case zk.EventNodeDataChanged:
				// Data changed, notify subscribers
				newData, _, err := c.zkConn.Get(zkPath)
				if err == nil {
					var newValue ConfigValue
					if err := json.Unmarshal(newData, &newValue); err == nil {
						c.notificationChan <- ConfigChangeEvent{
							Path:       path,
							OldValue:   oldValue,
							NewValue:   newValue,
							ChangeType: Updated,
							Timestamp:  time.Now(),
						}
					}
				}
			case zk.EventNodeDeleted:
				// Node deleted, notify subscribers
				c.notificationChan <- ConfigChangeEvent{
					Path:       path,
					OldValue:   oldValue,
					ChangeType: Deleted,
					Timestamp:  time.Now(),
				}
				// Exit this watch goroutine since node is deleted
				return

			case zk.EventNodeChildrenChanged:
			}
		}
	}()

	return nil
}

// Get retrieves a configuration value by path
func (c *configServiceImpl) Get(ctx context.Context, path string, watch ...bool) (ConfigValue, error) {
	zkPath := c.getZkPath(path)

	data, _, err := c.zkConn.Get(zkPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return ConfigValue{}, fmt.Errorf("config not found at path: %s", path)
		}
		return ConfigValue{}, fmt.Errorf("failed to get config: %w", err)
	}

	var value ConfigValue
	if err := json.Unmarshal(data, &value); err != nil {
		return ConfigValue{}, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if len(watch) > 0 && watch[0] {
		_ = c.watchZkNodeRecurse(ctx, path)
	}

	return value, nil
}

// Set creates or updates a configuration value
func (c *configServiceImpl) Set(ctx context.Context, path string, value any, watch ...bool) error {
	zkPath := c.getZkPath(path)

	// Determine value type
	valueType := fmt.Sprintf("%T", value)

	configValue := ConfigValue{
		Type:  valueType,
		Value: value,
	}

	data, err := json.Marshal(configValue)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	exists, stat, err := c.zkConn.Exists(zkPath)
	if err != nil {
		return fmt.Errorf("failed to check config existence: %w", err)
	}

	if exists {
		_, err = c.zkConn.Set(zkPath, data, stat.Version)
	} else {
		// Create parent nodes if they don't exist
		err = c.createParentNodes(path)
		if err != nil {
			return err
		}

		_, err = c.zkConn.Create(zkPath, data, 0, zk.WorldACL(zk.PermAll))
	}

	if err != nil {
		return fmt.Errorf("failed to set config: %w", err)
	}

	if len(watch) > 0 && watch[0] {
		// Set up watch on this node if it doesn't exist yet
		_ = c.watchZkNodeRecurse(ctx, path)
	}

	return nil
}

// createParentNodes ensures all parent nodes in a path exist
func (c *configServiceImpl) createParentNodes(configPath string) error {
	parts := strings.Split(configPath, "/")
	if len(parts) <= 1 {
		return nil // No parent needed
	}

	// Build parent path
	current := ""
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "" {
			continue
		}
		current = path.Join(current, parts[i])
		zkPath := c.getZkPath(current)

		exists, _, err := c.zkConn.Exists(zkPath)
		if err != nil {
			return fmt.Errorf("failed to check parent path: %w", err)
		}

		if !exists {
			_, err = c.zkConn.Create(zkPath, nil, 0, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists {
				return fmt.Errorf("failed to create parent node: %w", err)
			}
		}
	}

	return nil
}

// Delete removes a configuration
func (c *configServiceImpl) Delete(ctx context.Context, path string) error {
	zkPath := c.getZkPath(path)

	// Check for children
	children, _, err := c.zkConn.Children(zkPath)
	if err != nil && err != zk.ErrNoNode {
		return fmt.Errorf("failed to check children: %w", err)
	}

	if len(children) > 0 {
		return fmt.Errorf("cannot delete node with children, delete children first")
	}

	exists, stat, err := c.zkConn.Exists(zkPath)
	if err != nil {
		return fmt.Errorf("failed to check config existence: %w", err)
	}

	if !exists {
		return nil // Already deleted
	}

	err = c.zkConn.Delete(zkPath, stat.Version)
	if err != nil {
		return fmt.Errorf("failed to delete config: %w", err)
	}

	return nil
}

// Exists checks if a configuration exists
func (c *configServiceImpl) Exists(ctx context.Context, path string) (bool, error) {
	zkPath := c.getZkPath(path)

	exists, _, err := c.zkConn.Exists(zkPath)
	if err != nil {
		return false, fmt.Errorf("failed to check config existence: %w", err)
	}

	return exists, nil
}

// List gets all child nodes under a path
func (c *configServiceImpl) List(ctx context.Context, path string) ([]string, error) {
	zkPath := c.getZkPath(path)

	children, _, err := c.zkConn.Children(zkPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, fmt.Errorf("path does not exist: %s", path)
		}
		return nil, fmt.Errorf("failed to list children: %w", err)
	}

	return children, nil
}

// Subscribe adds a subscriber for config changes
func (c *configServiceImpl) Subscribe(ctx context.Context, path string, handler ConfigChangeHandler) (string, error) {
	if handler == nil {
		return "", errors.New("handler cannot be nil")
	}

	// Generate subscription ID
	subscriptionID := fmt.Sprintf("sub_%d", time.Now().UnixNano())

	c.subscriptionsMu.Lock()
	c.subscriptions[subscriptionID] = subscription{
		path:    path,
		handler: handler,
	}
	c.subscriptionsMu.Unlock()

	// Set up watch for this path and its children
	err := c.watchZkNodeRecurse(ctx, path)
	if err != nil {
		c.subscriptionsMu.Lock()
		delete(c.subscriptions, subscriptionID)
		c.subscriptionsMu.Unlock()
		return "", fmt.Errorf("failed to set up watch: %w", err)
	}

	return subscriptionID, nil
}

// Unsubscribe removes a subscriber
func (c *configServiceImpl) Unsubscribe(subscriptionID string) error {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()

	if _, exists := c.subscriptions[subscriptionID]; !exists {
		return fmt.Errorf("subscription ID not found: %s", subscriptionID)
	}

	delete(c.subscriptions, subscriptionID)
	return nil
}

// GetEffective gets a config value considering the hierarchy
func (c *configServiceImpl) GetEffective(ctx context.Context, path string, env string, namespace string) (ConfigValue, error) {
	pathWithoutFirstSlash := strings.TrimPrefix(path, "/")
	pathWithoutFirstSlash = strings.TrimPrefix(pathWithoutFirstSlash, namespace+"::")
	pathWithoutFirstSlash = strings.TrimPrefix(pathWithoutFirstSlash, env+"::")
	pathWithoutFirstSlash = strings.TrimPrefix(pathWithoutFirstSlash, "global::")

	// Try in order: namespace-env-specific, env-specific, global
	pathsToTry := []string{
		fmt.Sprintf("/%s::%s::%s", namespace, env, pathWithoutFirstSlash),
		fmt.Sprintf("/%s::%s", env, pathWithoutFirstSlash),
		fmt.Sprintf("/%s::%s", "global", pathWithoutFirstSlash),
	}

	var lastErr error
	for _, configPath := range pathsToTry {
		value, err := c.Get(ctx, configPath)
		if err == nil {
			return value, nil
		}
		lastErr = err
	}

	return ConfigValue{}, fmt.Errorf("config not found in hierarchy: %w", lastErr)
}

// SetBatch sets multiple configs in a batch operation
func (c *configServiceImpl) SetBatch(ctx context.Context, configs map[string]any, watch ...bool) error {
	for path, value := range configs {
		err := c.Set(ctx, path, value, watch...)
		if err != nil {
			return fmt.Errorf("failed to set config at %s: %w", path, err)
		}
	}
	return nil
}

// GetBatch gets multiple configs in a batch operation
func (c *configServiceImpl) GetBatch(ctx context.Context, paths []string, watch ...bool) (map[string]ConfigValue, error) {
	result := make(map[string]ConfigValue)
	var firstErr error

	for _, path := range paths {
		value, err := c.Get(ctx, path, watch...)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		result[path] = value
	}

	if len(result) == 0 && firstErr != nil {
		return nil, firstErr
	}

	return result, nil
}

// Export exports all configs under a path
func (c *configServiceImpl) Export(ctx context.Context, rootPath string, env string, namespace string) (map[string]ConfigValue, error) {
	result := make(map[string]ConfigValue)
	err := c.exportRecursive(ctx, rootPath, env, namespace, result)
	return result, err
}

// exportRecursive recursively exports configs
func (c *configServiceImpl) exportRecursive(ctx context.Context, path string, env string, namespace string, result map[string]ConfigValue) error {
	if path != "" {
		// Get current node value
		value, err := c.GetEffective(ctx, path, env, namespace)
		if err == nil {
			result[path] = value
		}
	}

	// Get children
	children, err := c.List(ctx, path)
	if err != nil {
		// If not found, assume it's not a directory
		if strings.Contains(err.Error(), "not exist") {
			return nil
		}
		return err
	}

	// Process children
	for _, child := range children {
		childPath := path
		if path == "" || path == "/" {
			childPath = "/" + child
		} else {
			childPath = path + "/" + child
		}

		err := c.exportRecursive(ctx, childPath, env, namespace, result)
		if err != nil {
			return err
		}
	}

	return nil
}

// Import imports configs
func (c *configServiceImpl) Import(ctx context.Context, configs map[string]ConfigValue, watch ...bool) error {
	for path, value := range configs {
		zkPath := c.getZkPath(path)

		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal config: %w", err)
		}

		exists, stat, err := c.zkConn.Exists(zkPath)
		if err != nil {
			return fmt.Errorf("failed to check config existence: %w", err)
		}

		// Create parent nodes
		err = c.createParentNodes(path)
		if err != nil {
			return err
		}

		if exists {
			_, err = c.zkConn.Set(zkPath, data, stat.Version)
		} else {
			_, err = c.zkConn.Create(zkPath, data, 0, zk.WorldACL(zk.PermAll))
		}

		if err != nil {
			return fmt.Errorf("failed to import config at %s: %w", path, err)
		}

		if len(watch) > 0 && watch[0] {
			// Set up watch
			_ = c.watchZkNodeRecurse(ctx, path)
		}
	}

	return nil
}

// BindStruct binds a struct to a configuration path with real-time updates
func (c *configServiceImpl) BindStruct(ctx context.Context, path string, env string, namespace string, structPtr any) (*ConfigStructBinding, error) {
	return c.BindStructWithCallback(ctx, path, env, namespace, structPtr, nil)
}

// BindStructWithCallback binds a struct to a configuration path with a callback for updates
func (c *configServiceImpl) BindStructWithCallback(ctx context.Context, path string, env string, namespace string, structPtr any, callback func(any)) (*ConfigStructBinding, error) {
	// Validate the struct pointer
	if structPtr == nil {
		return nil, errors.New("structPtr cannot be nil")
	}

	v := reflect.ValueOf(structPtr)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return nil, errors.New("structPtr must be a pointer to a struct")
	}

	binding := &ConfigStructBinding{
		Path:           path,
		StructPtr:      structPtr,
		UpdateCallback: callback,
	}

	// Do the initial loading
	err := c.loadStructFromZk(ctx, binding.Path, env, namespace, binding)
	if err != nil {
		return nil, fmt.Errorf("failed to load initial configuration: %w", err)
	}

	// Subscribe to changes
	handler := func(event ConfigChangeEvent) {
		err := c.loadStructFromZk(ctx, event.Path, env, namespace, binding)
		if err != nil {
			// Log error but don't fail the subscription
			fmt.Printf("Error updating struct: %v\n", err)
		} else if binding.UpdateCallback != nil {
			binding.UpdateCallback(binding.StructPtr)
		}
	}

	subscriptionID, err := c.Subscribe(ctx, path, handler)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to config changes: %w", err)
	}
	binding.subscriptionID = subscriptionID

	// Track the binding
	c.structBindingsMu.Lock()
	c.structBindings[binding] = true
	c.structBindingsMu.Unlock()

	return binding, nil
}

// UnbindStruct removes a struct binding
func (c *configServiceImpl) UnbindStruct(binding *ConfigStructBinding) error {
	if binding == nil {
		return errors.New("binding cannot be nil")
	}

	if binding.subscriptionID != "" {
		err := c.Unsubscribe(binding.subscriptionID)
		if err != nil {
			return fmt.Errorf("failed to unsubscribe: %w", err)
		}
	}

	c.structBindingsMu.Lock()
	delete(c.structBindings, binding)
	c.structBindingsMu.Unlock()

	return nil
}

// ReloadStruct manually reloads a struct from the current configuration
func (c *configServiceImpl) ReloadStruct(ctx context.Context, env string, namespace string, binding *ConfigStructBinding) error {
	if binding == nil {
		return errors.New("binding cannot be nil")
	}

	err := c.loadStructFromZk(ctx, binding.Path, env, namespace, binding)
	if err != nil {
		return fmt.Errorf("failed to reload struct: %w", err)
	}

	if binding.UpdateCallback != nil {
		binding.UpdateCallback(binding.StructPtr)
	}

	return nil
}

// loadStructFromZk loads configuration data from ZooKeeper into a struct
func (c *configServiceImpl) loadStructFromZk(ctx context.Context, path string, env string, namespace string, binding *ConfigStructBinding) error {
	binding.mu.Lock()
	defer binding.mu.Unlock()

	// Export all configs under the path
	configs, err := c.Export(ctx, path, env, namespace)
	if err != nil {
		return fmt.Errorf("failed to export configs: %w", err)
	}

	// Convert to a format that Viper can use
	configMap := make(map[string]any)
	for configPath, configValue := range configs {
		// Remove the binding path prefix to get the relative path
		relPath := strings.TrimPrefix(configPath, binding.Path)
		relPath = strings.TrimPrefix(relPath, "/")
		relPath = strings.TrimPrefix(relPath, namespace+"::")
		relPath = strings.TrimPrefix(relPath, env+"::")
		relPath = strings.TrimPrefix(relPath, "global::")

		// Handle empty path (root config)
		if relPath == "" {
			configMap["value"] = configValue.Value
		} else {
			configMap[relPath] = configValue.Value
		}
	}

	// Convert the map to JSON and load it into Viper
	jsonData, err := json.Marshal(configMap)
	if err != nil {
		return fmt.Errorf("failed to convert config to JSON: %w", err)
	}

	// Use Viper to load the config into the struct
	err = v1.ReadConfig(bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	// Unmarshal into the struct
	err = v1.Unmarshal(binding.StructPtr, func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "json"
	})
	if err != nil {
		return fmt.Errorf("failed to unmarshal config into struct: %w", err)
	}

	return nil
}

// Close also cleans up struct bindings
func (c *configServiceImpl) Close() error {
	// Clean up all struct bindings
	c.structBindingsMu.Lock()
	for binding := range c.structBindings {
		if binding.subscriptionID != "" {
			c.Unsubscribe(binding.subscriptionID)
		}
	}
	c.structBindings = nil
	c.structBindingsMu.Unlock()

	// Continue with regular close
	close(c.closeChan)
	c.wg.Wait()
	c.zkConn.Close()
	return nil
}

// SetFromStruct sets configuration values from a struct.
func (c *configServiceImpl) SetFromStruct(ctx context.Context, path string, env string, namespace string, structPtr any) error {
	// Validate the struct pointer
	if structPtr == nil {
		return errors.New("structPtr cannot be nil")
	}

	v := reflect.ValueOf(structPtr)
	if v.Kind() != reflect.Ptr {
		return errors.New("structPtr must be a pointer")
	}

	// Dereference pointer if it's a pointer to a pointer
	if v.Elem().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Elem().Kind() != reflect.Struct {
		return errors.New("structPtr must point to a struct")
	}

	// Convert struct to JSON
	jsonData, err := json.Marshal(structPtr)
	if err != nil {
		return fmt.Errorf("failed to marshal struct to JSON: %w", err)
	}

	// Unmarshal JSON into a map
	var settings map[string]any
	if err := json.Unmarshal(jsonData, &settings); err != nil {
		return fmt.Errorf("failed to unmarshal JSON into map: %w", err)
	}

	// Flatten the settings map with custom delimiter "::" and base path.
	zkConfigs := make(map[string]any)
	flattenMap("", settings, zkConfigs, path, env, namespace)

	// Use SetBatch to set all configurations.
	return c.SetBatch(ctx, zkConfigs)
}

// flattenMap recursively flattens the nested map with proper path prefixing
func flattenMap(prefix string, input map[string]any, output map[string]any, basePath string, env string, namespace string) {
	for k, v := range input {
		key := k
		if prefix != "" {
			key = prefix + "::" + k
		}

		fullPath := key
		if basePath != "" && basePath != "/" {
			fullPath = basePath + "::" + key
		}

		if subMap, isMap := v.(map[string]any); isMap {
			flattenMap(key, subMap, output, basePath, env, namespace)
		} else {
			output[namespace+"::"+env+"::"+fullPath] = v
		}
	}
}
