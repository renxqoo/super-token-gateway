package main

import (
	"context"
	"encoding/base64"
	"os"
	"os/signal"
	"syscall"

	native "gateway.local/plugin-host"
)

func main() { os.Exit(run()) }
func run() int {
	input, output, err := native.OpenStdio()
	if err != nil {
		return 2
	}
	defer input.Close()
	defer output.Close()
	config := native.DefaultConfig()
	if len(os.Args) == 3 {
		data, err := base64.StdEncoding.DecodeString(os.Args[2])
		if err != nil {
			return 2
		}
		config, err = native.ParseConfig(data)
		if err != nil {
			return 2
		}
		if config.Validate() != nil {
			return 2
		}
		if os.Args[1] == "--worker" {
			return native.RunWorker(config, input, output)
		}
		if os.Args[1] != "--config" {
			return 2
		}
	} else if len(os.Args) != 1 {
		return 2
	}
	executable, err := os.Executable()
	if err != nil {
		return 2
	}
	server, err := native.New(config, executable)
	if err != nil {
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if server.Serve(ctx, input, output) != nil {
		return 1
	}
	return 0
}
