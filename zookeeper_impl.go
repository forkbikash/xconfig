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

// configServiceImpl implements the ConfigService interface
type configServiceImpl struct {
	zkConn           *zk.Conn
	baseZkPath       string
	delimiter        string
	viper            *viper.Viper
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
func NewConfigService(zkServers []string, opts ...Option) (ConfigService, error) {
	// Default options
	options := ServiceOptions{
		BasePath:  "/config",
		Delimiter: "::",
	}

	// Apply user-provided options
	for _, opt := range opts {
		opt(&options)
	}

	// Create and configure Viper instance
	v := viper.NewWithOptions(viper.KeyDelimiter(options.Delimiter))
	v.SetConfigType("json")

	conn, _, err := zk.Connect(zkServers, time.Second*60)
	if err != nil {
		return nil, FormatError(ErrCodeConnection, err, "failed to connect to ZooKeeper")
	}

	// Ensure base path exists
	exists, _, err := conn.Exists(options.BasePath)
	if err != nil {
		return nil, FormatError(ErrCodeInternal, err, "failed to check base path")
	}

	if !exists {
		_, err = conn.Create(options.BasePath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return nil, FormatError(ErrCodeInternal, err, "failed to create base path")
		}
	}

	service := &configServiceImpl{
		zkConn:           conn,
		baseZkPath:       options.BasePath,
		delimiter:        options.Delimiter,
		viper:            v,
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

// ServiceOptions contains configuration options for the config service
type ServiceOptions struct {
	BasePath  string // Base path in ZooKeeper
	Delimiter string // Delimiter used for hierarchical keys
}

// Option is a function type for applying options to ServiceOptions
type Option func(*ServiceOptions)

// WithBasePath sets the base path in ZooKeeper
func WithBasePath(basePath string) Option {
	return func(opts *ServiceOptions) {
		opts.BasePath = basePath
	}
}

// WithDelimiter sets the delimiter for hierarchical keys
func WithDelimiter(delimiter string) Option {
	return func(opts *ServiceOptions) {
		opts.Delimiter = delimiter
	}
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

// processZkEvent processes ZooKeeper events and sends appropriate notifications
func (c *configServiceImpl) processZkEvent(e zk.Event) {
	logicalPath := strings.TrimPrefix(e.Path, c.baseZkPath)

	switch e.Type {
	case zk.EventNodeCreated:
		log.Println("ZK node created", e.Path)
		c.handleNodeCreated(e.Path, logicalPath)

	case zk.EventNodeDataChanged:
		log.Println("ZK node data changed", e.Path)
		c.handleNodeDataChanged(e.Path, logicalPath)

	case zk.EventNodeDeleted:
		log.Println("ZK node deleted", e.Path)
		c.handleNodeDeleted(e.Path, logicalPath)
	}
}

// handleNodeCreated processes node creation events
func (c *configServiceImpl) handleNodeCreated(zkPath, logicalPath string) {
	data, _, err := c.zkConn.Get(zkPath)
	if err == nil {
		var value ConfigValue
		if err := json.Unmarshal(data, &value); err == nil {
			c.notificationChan <- ConfigChangeEvent{
				Path:       logicalPath,
				NewValue:   value,
				ChangeType: Created,
				Timestamp:  time.Now(),
			}
		}
	}
}

// handleNodeDataChanged processes node data change events
func (c *configServiceImpl) handleNodeDataChanged(zkPath, logicalPath string) {
	newData, _, err := c.zkConn.Get(zkPath)
	if err == nil {
		var newValue ConfigValue
		if err := json.Unmarshal(newData, &newValue); err == nil {
			// We could get the old value here if needed, but would require caching
			c.notificationChan <- ConfigChangeEvent{
				Path:       logicalPath,
				NewValue:   newValue,
				ChangeType: Updated,
				Timestamp:  time.Now(),
			}
		}
	}
}

// handleNodeDeleted processes node deletion events
func (c *configServiceImpl) handleNodeDeleted(_, logicalPath string) {
	// We can only know the node was deleted, not its previous value
	// unless we had cached it, which would be a feature enhancement
	c.notificationChan <- ConfigChangeEvent{
		Path:       logicalPath,
		ChangeType: Deleted,
		Timestamp:  time.Now(),
	}
}

// watchZkNodeRecurse sets up a watcher on a ZooKeeper node
func (c *configServiceImpl) watchZkNodeRecurse(ctx context.Context, path string) error {
	zkPath := c.getZkPath(path)
	log.Println("Watching ZK node recursively", zkPath)

	// Set up a watch on the node
	go func() {
		ch, err := c.zkConn.AddWatchCtx(ctx, zkPath, true)
		if err != nil {
			log.Printf("Error setting up recursive watch on %s: %v", zkPath, err)
			return
		}

		for {
			select {
			case e, ok := <-ch:
				if !ok {
					log.Printf("Channel closed for watch on %s", zkPath)
					return
				}
				c.processZkEvent(e)
			case <-ctx.Done():
				log.Printf("Context canceled for watch on %s", zkPath)
				return
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
			return ConfigValue{}, FormatError(ErrCodeNotFound, err, "config not found at path")
		}
		return ConfigValue{}, FormatError(ErrCodeInternal, err, "failed to get config")
	}

	var value ConfigValue
	if err := json.Unmarshal(data, &value); err != nil {
		return ConfigValue{}, FormatError(ErrCodeInternal, err, "failed to unmarshal config")
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
		return FormatError(ErrCodeInternal, err, "failed to marshal config")
	}

	exists, stat, err := c.zkConn.Exists(zkPath)
	if err != nil {
		return FormatError(ErrCodeInternal, err, "failed to check config existence")
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
		return FormatError(ErrCodeInternal, err, "failed to set config")
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
			return FormatError(ErrCodeInternal, err, "failed to check parent path")
		}

		if !exists {
			_, err = c.zkConn.Create(zkPath, nil, 0, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists {
				return FormatError(ErrCodeInternal, err, "failed to create parent node")
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
		return FormatError(ErrCodeInternal, err, "failed to check children")
	}

	if len(children) > 0 {
		return FormatError(ErrCodeInvalidInput, nil, "cannot delete node with children, delete children first")
	}

	exists, stat, err := c.zkConn.Exists(zkPath)
	if err != nil {
		return FormatError(ErrCodeInternal, err, "failed to check config existence")
	}

	if !exists {
		return nil // Already deleted
	}

	err = c.zkConn.Delete(zkPath, stat.Version)
	if err != nil {
		return FormatError(ErrCodeInternal, err, "failed to delete config")
	}

	return nil
}

// Exists checks if a configuration exists
func (c *configServiceImpl) Exists(ctx context.Context, path string) (bool, error) {
	zkPath := c.getZkPath(path)

	exists, _, err := c.zkConn.Exists(zkPath)
	if err != nil {
		return false, FormatError(ErrCodeInternal, err, "failed to check config existence")
	}

	return exists, nil
}

// List gets all child nodes under a path
func (c *configServiceImpl) List(ctx context.Context, path string) ([]string, error) {
	zkPath := c.getZkPath(path)

	children, _, err := c.zkConn.Children(zkPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, FormatError(ErrCodeNotFound, err, "path does not exist")
		}
		return nil, FormatError(ErrCodeInternal, err, "failed to list children")
	}

	return children, nil
}

// Subscribe adds a subscriber for config changes
func (c *configServiceImpl) Subscribe(ctx context.Context, path string, handler ConfigChangeHandler) (string, error) {
	if handler == nil {
		return "", FormatError(ErrCodeInvalidInput, errors.New("handler cannot be nil"), "")
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
		return "", FormatError(ErrCodeInternal, err, "failed to set up watch")
	}

	return subscriptionID, nil
}

// Unsubscribe removes a subscriber
func (c *configServiceImpl) Unsubscribe(subscriptionID string) error {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()

	if _, exists := c.subscriptions[subscriptionID]; !exists {
		return FormatError(ErrCodeNotFound, nil, "subscription ID not found")
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

	return ConfigValue{}, FormatError(ErrCodeNotFound, lastErr, "config not found in hierarchy")
}

// SetBatch sets multiple configs in a batch operation
func (c *configServiceImpl) SetBatch(ctx context.Context, configs map[string]any, watch ...bool) error {
	for path, value := range configs {
		err := c.Set(ctx, path, value, watch...)
		if err != nil {
			return FormatError(ErrCodeInternal, err, "failed to set config at "+path)
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
			return FormatError(ErrCodeInternal, err, "failed to marshal config")
		}

		exists, stat, err := c.zkConn.Exists(zkPath)
		if err != nil {
			return FormatError(ErrCodeInternal, err, "failed to check config existence")
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
			return FormatError(ErrCodeInternal, err, "failed to import config at "+path)
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
		return nil, FormatError(ErrCodeInvalidInput, errors.New("structPtr cannot be nil"), "")
	}

	v := reflect.ValueOf(structPtr)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return nil, FormatError(ErrCodeInvalidInput, errors.New("structPtr must be a pointer to a struct"), "")
	}

	binding := &ConfigStructBinding{
		Path:           path,
		StructPtr:      structPtr,
		UpdateCallback: callback,
	}

	// Do the initial loading
	err := c.loadStructFromZk(ctx, binding.Path, env, namespace, binding)
	if err != nil {
		return nil, FormatError(ErrCodeInternal, err, "failed to load initial configuration")
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
		return nil, FormatError(ErrCodeInternal, err, "failed to subscribe to config changes")
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
		return FormatError(ErrCodeInvalidInput, errors.New("binding cannot be nil"), "")
	}

	if binding.subscriptionID != "" {
		err := c.Unsubscribe(binding.subscriptionID)
		if err != nil {
			return FormatError(ErrCodeInternal, err, "failed to unsubscribe")
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
		return FormatError(ErrCodeInvalidInput, errors.New("binding cannot be nil"), "")
	}

	err := c.loadStructFromZk(ctx, binding.Path, env, namespace, binding)
	if err != nil {
		return FormatError(ErrCodeInternal, err, "failed to reload struct")
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
		return FormatError(ErrCodeInternal, err, "failed to export configs")
	}

	// Convert to a format that Viper can use
	configMap := make(map[string]any)
	for configPath, configValue := range configs {
		// Remove the binding path prefix to get the relative path
		relPath := strings.TrimPrefix(configPath, binding.Path)
		relPath = strings.TrimPrefix(relPath, "/")
		relPath = strings.TrimPrefix(relPath, namespace+c.delimiter)
		relPath = strings.TrimPrefix(relPath, env+c.delimiter)
		relPath = strings.TrimPrefix(relPath, "global"+c.delimiter)

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
		return FormatError(ErrCodeInternal, err, "failed to convert config to JSON")
	}

	// Use the instance Viper to load the config into the struct
	err = c.viper.ReadConfig(bytes.NewReader(jsonData))
	if err != nil {
		return FormatError(ErrCodeInternal, err, "failed to read config")
	}

	// Unmarshal into the struct
	err = c.viper.Unmarshal(binding.StructPtr, func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "json"
	})
	if err != nil {
		return FormatError(ErrCodeInternal, err, "failed to unmarshal config into struct")
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
		return FormatError(ErrCodeInvalidInput, errors.New("structPtr cannot be nil"), "")
	}

	v := reflect.ValueOf(structPtr)
	if v.Kind() != reflect.Ptr {
		return FormatError(ErrCodeInvalidInput, errors.New("structPtr must be a pointer"), "")
	}

	// Dereference pointer if it's a pointer to a pointer
	if v.Elem().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Elem().Kind() != reflect.Struct {
		return FormatError(ErrCodeInvalidInput, errors.New("structPtr must point to a struct"), "")
	}

	// Convert struct to JSON
	jsonData, err := json.Marshal(structPtr)
	if err != nil {
		return FormatError(ErrCodeInternal, err, "failed to marshal struct to JSON")
	}

	// Unmarshal JSON into a map
	var settings map[string]any
	if err := json.Unmarshal(jsonData, &settings); err != nil {
		return FormatError(ErrCodeInternal, err, "failed to unmarshal JSON into map")
	}

	// Flatten the settings map with custom delimiter "::" and base path.
	zkConfigs := make(map[string]any)
	c.flattenMap("", settings, zkConfigs, path, env, namespace)

	// Use SetBatch to set all configurations.
	return c.SetBatch(ctx, zkConfigs)
}

// flattenMap recursively flattens the nested map with proper path prefixing
func (c *configServiceImpl) flattenMap(prefix string, input map[string]any, output map[string]any, basePath string, env string, namespace string) {
	for k, v := range input {
		key := k
		if prefix != "" {
			key = prefix + c.delimiter + k
		}

		fullPath := key
		if basePath != "" && basePath != "/" {
			fullPath = basePath + c.delimiter + key
		}

		if subMap, isMap := v.(map[string]any); isMap {
			c.flattenMap(key, subMap, output, basePath, env, namespace)
		} else {
			output[namespace+c.delimiter+env+c.delimiter+fullPath] = v
		}
	}
}
