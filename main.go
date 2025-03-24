package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/petuhovskiy/overload/autoai"
	"github.com/petuhovskiy/overload/internal/log"
	"github.com/sashabaranov/go-openai"
)

func main() {
	_ = log.DefaultGlobals()

	connstr := os.Getenv("CONNSTR")
	if connstr == "" {
		fmt.Println("Error: DB_CONN_STR environment variable not set")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logsConnstr := os.Getenv("LOGS_CONNSTR")
	pool, err := pgxpool.New(context.Background(), logsConnstr)
	if err != nil {
		fmt.Println("Error: failed to connect to database:", err)
		os.Exit(1)
	}
	defer pool.Close()
	dbHistory := autoai.NewDBHistory(pool)

	openaiToken := os.Getenv("OPENAI_TOKEN")
	openaiClient := openai.NewClient(openaiToken)

	gen := autoai.NewGenerator(openaiClient, dbHistory)

	for {
		gen.DoIteration(ctx, connstr)
	}

	// // Start the reporter in a separate goroutine
	// go ingest.ReportUploadSpeed(ctx, connstr)

	// // Configure and run the ingest operation
	// conf := ingest.Config{
	// 	TableName: "data42",
	// }

	// multi.RunMany(ctx, 10, func(ctx context.Context) error {
	// 	return ingest.RunCopy(ctx, connstr, conf)
	// })

	// // Allow some time for the reporter to show the final results
	// time.Sleep(5 * time.Second)
}
