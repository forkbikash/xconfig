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

	"github.com/go-zookeeper/zk"
	"github.com/spf13/viper"
)

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
	conn, _, err := zk.Connect(zkServers, time.Second*10)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ZooKeeper: %w", err)
	}

	// Ensure base path exists
	exists, _, err := conn.Exists(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to check base path: %w", err)
	}

	if !exists {
		_, err = conn.Create(basePath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return nil, fmt.Errorf("failed to create base path: %w", err)
		}
	}

	service := &configServiceImpl{
		zkConn:           conn,
		baseZkPath:       basePath,
		subscriptions:    make(map[string]subscription),
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
func (c *configServiceImpl) Get(ctx context.Context, path string) (ConfigValue, error) {
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

	return value, nil
}

// Set creates or updates a configuration value
func (c *configServiceImpl) Set(ctx context.Context, path string, value any) error {
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

	// Set up watch on this node if it doesn't exist yet
	c.watchZkNode(path)

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
			_, err = c.zkConn.Create(zkPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
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
func (c *configServiceImpl) Subscribe(path string, handler ConfigChangeHandler) (string, error) {
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
	err := c.watchZkNode(path)
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
	// Try in order: namespace-specific, env-specific, global
	pathsToTry := []string{
		fmt.Sprintf("%s::%s::%s", namespace, env, path),
		fmt.Sprintf("%s::%s", env, path),
		fmt.Sprintf("%s::%s", "global", path),
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
func (c *configServiceImpl) SetBatch(ctx context.Context, configs map[string]any) error {
	for path, value := range configs {
		err := c.Set(ctx, path, value)
		if err != nil {
			return fmt.Errorf("failed to set config at %s: %w", path, err)
		}
	}
	return nil
}

// GetBatch gets multiple configs in a batch operation
func (c *configServiceImpl) GetBatch(ctx context.Context, paths []string) (map[string]ConfigValue, error) {
	result := make(map[string]ConfigValue)
	var firstErr error

	for _, path := range paths {
		value, err := c.Get(ctx, path)
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
	// Get current node value
	value, err := c.GetEffective(ctx, path, env, namespace)
	if err == nil {
		result[path] = value
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
func (c *configServiceImpl) Import(ctx context.Context, configs map[string]ConfigValue) error {
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

		// Set up watch
		c.watchZkNode(path)
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
	err := c.loadStructFromZk(ctx, env, namespace, binding)
	if err != nil {
		return nil, fmt.Errorf("failed to load initial configuration: %w", err)
	}

	// Subscribe to changes
	handler := func(event ConfigChangeEvent) {
		err := c.loadStructFromZk(ctx, env, namespace, binding)
		if err != nil {
			// Log error but don't fail the subscription
			fmt.Printf("Error updating struct: %v\n", err)
		} else if binding.UpdateCallback != nil {
			binding.UpdateCallback(binding.StructPtr)
		}
	}

	subscriptionID, err := c.Subscribe(path, handler)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to config changes: %w", err)
	}

	binding.subscriptionID = subscriptionID

	// Track the binding
	c.structBindingsMu.Lock()
	if c.structBindings == nil {
		c.structBindings = make(map[*ConfigStructBinding]bool)
	}
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

	err := c.loadStructFromZk(ctx, env, namespace, binding)
	if err != nil {
		return fmt.Errorf("failed to reload struct: %w", err)
	}

	if binding.UpdateCallback != nil {
		binding.UpdateCallback(binding.StructPtr)
	}

	return nil
}

// loadStructFromZk loads configuration data from ZooKeeper into a struct
func (c *configServiceImpl) loadStructFromZk(ctx context.Context, env string, namespace string, binding *ConfigStructBinding) error {
	binding.mu.Lock()
	defer binding.mu.Unlock()

	// Export all configs under the path
	configs, err := c.Export(ctx, binding.Path, env, namespace)
	if err != nil {
		return fmt.Errorf("failed to export configs: %w", err)
	}

	// Convert to a flat map that Viper can use
	flatConfig := make(map[string]any)

	// Convert the zookeeper path structure to a nested map structure
	for configPath, configValue := range configs {
		// Remove the binding path prefix to get the relative path
		relPath := strings.TrimPrefix(configPath, binding.Path)
		relPath = strings.TrimPrefix(relPath, "/")

		// Convert :: path notation to nested map keys
		keys := strings.Split(relPath, "::")

		// Handle empty path (root config)
		if relPath == "" {
			// If this is a direct value at the root path
			flatConfig["value"] = configValue.Value
			continue
		}

		// Create nested path using dots for Viper
		viperKey := strings.Join(keys, ".")
		flatConfig[viperKey] = configValue.Value
	}

	// Use Viper to load the config into the struct
	v := viper.New()
	v.SetConfigType("json") // Use JSON as an intermediate format

	// Convert the map to JSON and load it into Viper
	jsonData, err := json.Marshal(flatConfig)
	if err != nil {
		return fmt.Errorf("failed to convert config to JSON: %w", err)
	}

	err = v.ReadConfig(strings.NewReader(string(jsonData)))
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	// Unmarshal into the struct
	err = v.Unmarshal(binding.StructPtr)
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

// SetFromStruct implements setting configuration values from a struct using Viper
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

	// Initialize a new Viper instance
	v1 := viper.New()
	v1.SetConfigType("json")

	// Convert struct to JSON
	jsonData, err := json.Marshal(structPtr)
	if err != nil {
		return fmt.Errorf("failed to marshal struct to JSON: %w", err)
	}

	// Load JSON into Viper
	err = v1.ReadConfig(bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to read config into Viper: %w", err)
	}

	// Get all settings as a flattened map
	allSettings := v1.AllSettings()

	// Convert the flattened map to ZooKeeper paths
	zkConfigs := make(map[string]any)

	// Process the settings recursively
	err = processViperSettings(allSettings, "", path, env, namespace, zkConfigs)
	if err != nil {
		return fmt.Errorf("failed to process Viper settings: %w", err)
	}

	// Use SetBatch to set all configs
	return c.SetBatch(ctx, zkConfigs)
}

// processViperSettings recursively processes Viper settings and converts them to ZooKeeper paths
func processViperSettings(settings map[string]any, prefix string, basePath string, env string, namespace string, result map[string]any) error {
	for k, v := range settings {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}

		switch value := v.(type) {
		case map[string]any:
			// Recursively process nested maps
			err := processViperSettings(value, key, basePath, env, namespace, result)
			if err != nil {
				return err
			}
		default:
			// Convert dot notation to :: notation
			zkKey := strings.ReplaceAll(key, ".", "::")

			// Add the base path
			if basePath != "" && basePath != "/" {
				zkKey = basePath + "::" + zkKey
			}

			// Add to result map
			result[zkKey] = v
		}
	}

	return nil
}
