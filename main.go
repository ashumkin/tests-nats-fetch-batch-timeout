package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("usage: go run main.go SERVER SUBJECT MAX_WAIT_TIME")
	}
	server := os.Args[1]
	subj := os.Args[2]
	conn, err := nats.Connect(server)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to connect to %s: %w", server, err))
	}
	stream, err := conn.JetStream()
	if err != nil {
		log.Fatal(fmt.Errorf("failed to get JS: %w", err))
	}
	durable := strings.ReplaceAll(strings.ReplaceAll(subj+"-durable", ".", "_"), ">", "_")
	sub, err := stream.PullSubscribe(subj, durable)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to subscribe to %s (%s): %w", subj, durable, err))
	}
	maxWaitS := os.Args[3]
	maxWait, err := time.ParseDuration(maxWaitS)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to parse time %s: %w", maxWaitS, err))
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), maxWait)
		batch, err := sub.FetchBatch(10, nats.Context(ctx))
		if err != nil {
			slog.Warn("failed to fetch batch", "err", err)
		}
		for m := range batch.Messages() {
			slog.Info("message received:", "msg", m)
		}
		if bErr := batch.Error(); bErr != nil {
			slog.Warn("failed to fetch batch", "err", bErr)
		}
		cancel()
	}
}
