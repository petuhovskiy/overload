package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/petuhovskiy/overload/internal/log"
	"go.uber.org/zap"
)

// RunCopy runs COPY query to ingest data as fast as possible.
// It generates random data and inserts it into the table.
func RunCopy(ctx context.Context, connstr string, conf Config) error {
	log.Info(ctx, "ingest started", zap.Any("conf", conf))
	defer log.Info(ctx, "ingest finished")

	conf.Normalize()

	conn, err := pgx.Connect(ctx, connstr)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if err := createTable(ctx, conn, conf.TableName); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Start tracking metrics
	startTime := time.Now()
	rowsInserted := int64(0)
	lastReportTime := startTime
	lastReportRows := int64(0)

	// Column names for the COPY operation
	columns := []string{"tid", "bid", "aid", "delta", "mtime", "filler"}

	// Process data in batches
copy:
	for {
		select {
		case <-ctx.Done():
			break copy
		default:
		}

		// Determine batch size for this iteration
		batchSize := conf.BatchSize

		// Generate and copy batch of rows
		rows := make([][]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			rows[i] = generateRandomRow()
		}

		// Use CopyFrom for efficient batch insertion
		n, err := conn.CopyFrom(
			ctx,
			pgx.Identifier{conf.TableName},
			columns,
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("failed to copy data: %w", err)
		}

		rowsInserted += n

		// Report progress periodically
		now := time.Now()
		if now.Sub(lastReportTime) > time.Second*2 {
			rowsSinceLastReport := rowsInserted - lastReportRows
			duration := now.Sub(lastReportTime).Seconds()
			rowsPerSecond := float64(rowsSinceLastReport) / duration

			log.Info(ctx, "ingest progress",
				zap.Int64("rows_inserted", rowsInserted),
				zap.Float64("rows_per_second", rowsPerSecond),
				zap.Duration("elapsed", now.Sub(startTime)),
			)

			lastReportTime = now
			lastReportRows = rowsInserted
		}
	}

	return nil
}
