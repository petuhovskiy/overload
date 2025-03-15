package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/petuhovskiy/overload/internal/log"
	"go.uber.org/zap"
)

type statsSnapshot struct {
	// size in bytes
	DatabaseSize uint64
	// timestamp of the snapshot
	Timestamp time.Time
}

// ReportUploadSpeed will print database size growth every second.
func ReportUploadSpeed(ctx context.Context, connstr string) {
	ctx = log.With(ctx, zap.String("job", "stats"))

	log.Info(ctx, "started")

	var lastSnapshot *statsSnapshot
	var conn *pgx.Conn
	var err error

	close := func() {
		if conn != nil {
			err := conn.Close(ctx)
			if err != nil {
				log.Error(ctx, "failed to close connection", zap.Error(err))
			}
			conn = nil
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}

		if conn == nil {
			conn, err = pgx.Connect(ctx, connstr)
			if err != nil {
				log.Error(ctx, "failed to connect", zap.Error(err))
				close()
				continue
			}
		}

		snapshot, err := getStatsSnapshot(ctx, conn)
		if err != nil {
			log.Error(ctx, "failed to get stats snapshot", zap.Error(err))
			close()
			continue
		}

		if lastSnapshot != nil {
			sizeDiff := int64(snapshot.DatabaseSize) - int64(lastSnapshot.DatabaseSize)
			timeDiff := snapshot.Timestamp.Sub(lastSnapshot.Timestamp).Seconds()
			speed := float64(sizeDiff) / timeDiff
			speedHuman := humanizeBytes(int64(speed)) + "/s"
			sizeHuman := humanizeBytes(int64(snapshot.DatabaseSize))

			log.Info(ctx, "fetched", zap.Any("speed", speedHuman), zap.String("size", sizeHuman))
		}
		lastSnapshot = snapshot
	}
}

// humanizeBytes converts bytes to human readable format.
// For example, 1024 -> "1.0 KB", 12345 -> "12.3 KB".
func humanizeBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return "0 B"
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func getStatsSnapshot(ctx context.Context, conn *pgx.Conn) (*statsSnapshot, error) {
	var snapshot statsSnapshot

	row := conn.QueryRow(ctx, "SELECT pg_database_size(current_database()), now()")
	err := row.Scan(&snapshot.DatabaseSize, &snapshot.Timestamp)
	if err != nil {
		return nil, err
	}

	return &snapshot, nil
}
