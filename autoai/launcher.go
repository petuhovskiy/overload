package autoai

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/petuhovskiy/overload/internal/log"
	"github.com/petuhovskiy/overload/internal/multi"
	"go.uber.org/zap"
)

func StressQuery(ctx context.Context, connstr string, query Query) (*ExecStats, error) {
	ctx = log.With(ctx, zap.String("query", query.SQL))

	log.Info(ctx, "connecting to database")
	conn, err := pgx.Connect(ctx, connstr)
	if err != nil {
		log.Error(ctx, "failed to connect to database", zap.Error(err))
		return nil, err
	}
	defer conn.Close(ctx)

	const iterationDuration = time.Minute

	stats, err := executeAndMeasure(ctx, conn, query, iterationDuration)
	if err != nil {
		return nil, fmt.Errorf("failed initial run: %w", err)
	}

	log.Info(ctx, "query execution statistics", zap.Any("stats", stats))

	if stats.Count == 0 || stats.Avg == 0 {
		return &stats, nil
	}

	for iter := 0; iter < 4; iter++ {
		n := 50

		ch := make(chan ExecStats, n)
		multi.RunMany(ctx, n, func(ctx context.Context) error {
			res, err := executeAndMeasure(ctx, conn, query, iterationDuration)
			ch <- res
			return err
		})

		var sum time.Duration
		var count int
		for i := 0; i < n; i++ {
			st := <-ch
			sum += st.Avg * time.Duration(st.Count)
			count += st.Count
		}

		if count == 0 {
			break
		}

		stats.Avg = sum / time.Duration(count)
		stats.Count = count

		log.Info(ctx, "query execution statistics", zap.Any("stats", stats))
	}

	return &stats, nil
}

type ExecStats struct {
	Min, Avg, Max time.Duration
	Count         int
}

func executeAndMeasure(ctx context.Context, conn *pgx.Conn, query Query, duration time.Duration) (ExecStats, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	stats := ExecStats{
		Min:   time.Hour,
		Max:   0,
		Count: 0,
	}

	sum := time.Duration(0)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			start := time.Now()
			_, err := conn.Exec(ctx, query.SQL)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					log.Info(ctx, "query execution timed out or canceled")
					break loop
				}
				return ExecStats{}, err
			}
			elapsed := time.Since(start)

			stats.Count++

			if stats.Count == 1 {
				log.Info(ctx, "first query executed", zap.Duration("elapsed", elapsed))
			}

			stats.Min = min(stats.Min, elapsed)
			stats.Max = max(stats.Max, elapsed)
			sum += elapsed
		}
	}

	if stats.Count > 0 {
		stats.Avg = sum / time.Duration(stats.Count)
	}
	return stats, nil
}
