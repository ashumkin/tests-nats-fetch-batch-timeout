package main

import (
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
		batch, err := sub.FetchBatch(10, nats.MaxWait(maxWait))
		if err != nil {
			slog.Error("failed to fetch batch", "err", err, "sub", sub.Subject)

			continue
		}
		c := 0
		for m := range batch.Messages() {
			slog.Info("message received:", "msg", m)
			m.Ack()
			c += 1
		}
		if bErr := batch.Error(); bErr != nil {
			slog.Warn("failed to fetch batch", "err", bErr, "count", c)

			continue
		}
		slog.Info("batch processed", "count", c)
	}
}
