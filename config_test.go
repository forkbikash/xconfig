package config

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/Shopify/zk"
)

func TestSdk(t *testing.T) {
	testSdk()
}

func testSdk() {
	ctx := context.Background()

	client, _ := NewConfigService(
		[]string{"localhost:2181"},
		WithEnvironment("prod"),
		WithNamespace("ecommerce"),
	)
	defer client.Close()

	type AppConfig struct {
		Database struct {
			URL      string `json:"url,omitempty"`
			PoolSize int    `json:"pool_size,omitempty"`
			Username string `json:"username,omitempty"`
		} `json:"database,omitempty"`
	}

	appConfig := &AppConfig{}
	appConfig.Database.URL = "postgresql://localhost:5432/default"
	appConfig.Database.PoolSize = 5
	err := client.SetFromStruct(ctx, "", appConfig)
	if err != nil {
		log.Fatalf("Failed to save config: %v", err)
	}

	loadedConfig := &AppConfig{}
	loadedConfig.Database.Username = "bikash"
	_, err = client.BindStructWithCallback(ctx, "", loadedConfig, func(updated any) {
		cfg := updated.(*AppConfig)
		fmt.Println("Configuration updated!")
		fmt.Printf("  Database URL: %s\n", cfg.Database.URL)
		fmt.Printf("  Database Pool Size: %d\n", cfg.Database.PoolSize)
		fmt.Printf("  Database Username: %s\n", cfg.Database.Username)
	})
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Println("Initial configuration:")
	fmt.Printf("  Database URL: %s\n", loadedConfig.Database.URL)
	fmt.Printf("  Database Pool Size: %d\n", loadedConfig.Database.PoolSize)
	fmt.Printf("  Database Username: %s\n", loadedConfig.Database.Username)

	fmt.Println("\nUpdating configuration...")
	appConfig2 := &AppConfig{}
	appConfig2.Database.URL = "postgresql://prod-db:5432/orders"
	err = client.SetFromStruct(ctx, "", appConfig2)
	if err != nil {
		log.Fatalf("Failed to update config: %v", err)
	}

	time.Sleep(time.Second * 5)

	fmt.Println("Updated configuration:")
	fmt.Printf("  Database URL: %s\n", loadedConfig.Database.URL)
	fmt.Printf("  Database Pool Size: %d\n", loadedConfig.Database.PoolSize)
	fmt.Printf("  Database Pool Size: %s\n", loadedConfig.Database.Username)
}

func TestAddWatch(t *testing.T) {
	c, _, err := zk.Connect([]string{"127.0.0.1:2181"}, 60*time.Second)
	if err != nil {
		panic(err)
	}

	ch, err := c.AddWatchCtx(context.Background(), "/bikash", true)
	if err != nil {
		panic(err)
	}

	for e := range ch {
		// zk.Event {Type: EventNodeDataChanged (3), State: 3, Path: "/config/ecommerce::prod::database::pool_size", Err: error nil, Server: ""}
		// zk.Event {Type: EventNodeCreated (1), State: 3, Path: "/config/ecommerce::prod::database::pool_size", Err: error nil, Server: ""}
		// zk.Event {Type: EventNodeDeleted (2), State: 3, Path: "/config/ecommerce::prod::database::pool_size", Err: error nil, Server: ""}
		fmt.Printf("%+v\n", e)
	}
}

func TestGetW(t *testing.T) {
	c, _, err := zk.Connect([]string{"127.0.0.1:2181"}, 60*time.Second)
	if err != nil {
		panic(err)
	}

	data, _, ch, err := c.GetW("/bikash")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", string(data))

	for e := range ch {
		fmt.Printf("%+v\n", e)
	}
}
