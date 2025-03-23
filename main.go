package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/petuhovskiy/overload/ingest"
	"github.com/petuhovskiy/overload/internal/log"
	"go.uber.org/zap"
)

func runMany(ctx context.Context, n int, f func(ctx context.Context) error) {
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		i := i
		ctx := log.With(ctx, zap.Int("worker", i))

		go func() {
			defer wg.Done()
			err := f(ctx)
			if err != nil {
				log.Error(ctx, "worker failed", zap.Error(err))
			} else {
				log.Info(ctx, "worker finished")
			}
		}()
	}

	wg.Wait()
}

func main() {
	_ = log.DefaultGlobals()

	connstr := os.Getenv("CONNSTR")
	if connstr == "" {
		fmt.Println("Error: DB_CONN_STR environment variable not set")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the reporter in a separate goroutine
	go ingest.ReportUploadSpeed(ctx, connstr)

	// Configure and run the ingest operation
	conf := ingest.Config{
		TableName: "data42",
	}

	runMany(ctx, 10, func(ctx context.Context) error {
		return ingest.RunCopy(ctx, connstr, conf)
	})

	// Allow some time for the reporter to show the final results
	time.Sleep(5 * time.Second)
}
