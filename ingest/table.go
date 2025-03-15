package ingest

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

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
