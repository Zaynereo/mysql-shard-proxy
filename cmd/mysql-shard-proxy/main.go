// Command mysql-shard-proxy serves a MySQL protocol endpoint that routes
// queries across hash shards and splits reads onto replicas.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/Zaynereo/mysql-shard-proxy/internal/config"
	"github.com/Zaynereo/mysql-shard-proxy/internal/logging"
	"github.com/Zaynereo/mysql-shard-proxy/internal/proxy"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}
	log := logging.New(cfg.LogLevel)
	app, err := proxy.New(cfg, log)
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	if err := app.Run(context.Background()); err != nil {
		log.Error("proxy stopped", "err", err)
		os.Exit(1)
	}
}
