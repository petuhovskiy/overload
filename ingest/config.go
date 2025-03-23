package ingest

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const (
	defaultTableName = "data42"
	defaultBatchSize = 1000000
)

type Config struct {
	TableName string
	BatchSize int
}

func (conf *Config) Normalize() {
	if conf.TableName == "" {
		conf.TableName = defaultTableName
	}

	if conf.BatchSize == 0 {
		conf.BatchSize = defaultBatchSize
	}
}

// createTable creates table if not exists.
// It uses default schema for pgbench_history.
//
// CREATE TABLE pgbench_history (
//
//	tid int,
//	bid int,
//	aid int,
//	delta int,
//	mtime timestamp,
//	filler char(22)
//
// );
func createTable(ctx context.Context, conn *pgx.Conn, tableName string) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			tid int,
			bid int,
			aid int,
			delta int,
			mtime timestamp,
			filler char(22)
		);
	`, tableName))
	return err
}
