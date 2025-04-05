package config

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	szk "github.com/Shopify/zk"
)

func TestConfig(t *testing.T) {
	configTest()
}

func configTest() {
	ctx := context.Background()

	client := NewZkClient(
		[]string{"localhost:2181"},
		WithEnvironment("prod"),
		WithNamespace("ecommerce"),
	)
	defer client.Close()

	type AppConfig struct {
		Database struct {
			URL      string `json:"url"`
			PoolSize int    `json:"pool_size"`
			Username string `json:"username"`
		} `json:"database"`
	}

	appConfig := &AppConfig{}
	appConfig.Database.URL = "postgresql://localhost:5432/default"
	appConfig.Database.PoolSize = 5
	appConfig.Database.Username = "bikash"
	err := client.SetConfig(ctx, appConfig)
	if err != nil {
		log.Fatalf("Failed to save config: %v", err)
	}

	loadedConfig := &AppConfig{}
	err = client.LoadConfigWithWatch(ctx, loadedConfig, func(updated any) {
		cfg := updated.(*AppConfig)
		fmt.Println("Configuration updated!")
		fmt.Printf("  Database URL: %s\n", cfg.Database.URL)
		fmt.Printf("  Database Pool Size: %d\n", cfg.Database.PoolSize)
	})
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Println("Initial configuration:")
	fmt.Printf("  Database URL: %s\n", loadedConfig.Database.URL)
	fmt.Printf("  Database Pool Size: %d\n", loadedConfig.Database.PoolSize)

	fmt.Println("\nUpdating configuration...")
	appConfig.Database.URL = "postgresql://prod-db:5432/orders"
	err = client.SetConfig(ctx, appConfig)
	if err != nil {
		log.Fatalf("Failed to update config: %v", err)
	}

	time.Sleep(time.Second * 5)

	fmt.Println("Updated configuration:")
	fmt.Printf("  Database URL: %s\n", loadedConfig.Database.URL)
	fmt.Printf("  Database Pool Size: %d\n", loadedConfig.Database.PoolSize)
}

func TestConfigShopifyZk(t *testing.T) {
	c, _, err := szk.Connect([]string{"127.0.0.1:2181"}, 20*time.Second)
	if err != nil {
		panic(err)
	}

	ch, err := c.AddWatch("/", true)
	if err != nil {
		panic(err)
	}
	for e := range ch {
		fmt.Printf("%+v\n", e)
	}
}
