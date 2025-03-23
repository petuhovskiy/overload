package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/petuhovskiy/overload/internal/log"
	"go.uber.org/zap"
)

// RunGenerate runs INSERT INTO ... query to ingest data as fast as possible.
// It generates random data on the server side and inserts it into the table.
// It should be faster than COPY because it doesn't need to transfer data over the network.
func RunGenerate(ctx context.Context, connstr string, conf Config) error {
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

	// Create a server-side data generation query
	// This uses PostgreSQL's random functions to generate data directly in the database
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (tid, bid, aid, delta, mtime, filler)
		SELECT
			(s %% 100000)::int, -- tid: use modulo of series value instead of random
			(s %% 10000)::int, -- bid: use modulo of series value
			(s %% 10000000)::int, -- aid: use modulo of series value
			(s %% 1000000 - 500000)::int, -- delta: simpler calculation
			now() - ((s %% 30) * interval '1 day'), -- simpler timestamp generation
			lpad(s::text, 22, '0') -- much faster than md5
		FROM (SELECT generate_series AS s FROM generate_series(1, $1)) subq
	`, conf.TableName)

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

		// Execute the insert query with server-side data generation
		tag, err := conn.Exec(ctx, insertQuery, batchSize)
		if err != nil {
			return fmt.Errorf("failed to insert data: %w", err)
		}

		rowsInserted += tag.RowsAffected()

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
