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

type Launcher struct {
	db *DBHistory
}

func (l *Launcher) Run(ctx context.Context, connstr string, query Query) ExecStats {
	ctx = log.With(ctx, zap.String("query", query.SQL))

	log.Info(ctx, "connecting to database")

	const iterationDuration = time.Minute

	stats := executeAndMeasure(ctx, connstr, query, iterationDuration)
	einfo := stats.ToExecInfo(query.SQL, 1)
	go l.db.SaveQueryExecInfo(einfo)

	log.Info(ctx, "query execution statistics", zap.Any("stats", stats))
	if stats.ToExecInfo("", 1).IsFailed {
		return stats
	}

	n := 25

	for iter := 0; iter < 4; iter++ {
		n *= 2

		ch := make(chan ExecStats, n)
		multi.RunMany(ctx, n, func(ctx context.Context) error {
			res := executeAndMeasure(ctx, connstr, query, iterationDuration)
			ch <- res
			return res.Error
		})

		var errors []error
		var sts []ExecStats

		var sum time.Duration
		var count int
		for i := 0; i < n; i++ {
			st := <-ch
			sts = append(sts, st)

			if st.Count > 0 {
				sum += st.Avg
				count++
			}

			if st.Error != nil {
				errors = append(errors, st.Error)
			}
		}

		if count > 0 {
			sum /= time.Duration(count)
			sum /= time.Duration(count)
		}

		// join all errors in a single error
		var err error
		if len(errors) > 0 {
			err = errors[0]
			for _, e := range errors[1:] {
				err = fmt.Errorf("%w; %v", err, e)
			}
		}

		stats = ExecStats{
			Count: count,
			Avg:   sum,
			Error: err,
		}
		go l.db.SaveQueryExecInfo(stats.ToExecInfo(query.SQL, n))

		log.Info(ctx, "query execution statistics", zap.Any("stats", stats))
	}

	return stats
}

type ExecStats struct {
	Min, Avg, Max time.Duration
	Count         int
	Error         error
}

func (s *ExecStats) ToExecInfo(query string, conns int) *QueryExecInfo {
	failed := s.Error != nil || s.Count == 0 || s.Avg == 0
	qps := 0.0
	if s.Avg > 0 {
		qps = 1 / s.Avg.Seconds()
	}

	comment := ""
	if s.Error != nil {
		comment = fmt.Sprintf("error: %s", s.Error)
	} else if s.Count == 0 || s.Avg == 0 {
		comment = "timeout"
	} else {
		comment = "ok"
	}

	return &QueryExecInfo{
		Query:    query,
		IsFailed: failed,
		QPS:      float32(qps),
		Conns:    conns,
		Comment:  comment,
		Info:     s,
	}
}

func executeAndMeasure(ctx context.Context, connstr string, query Query, duration time.Duration) ExecStats {
	conn, err := pgx.Connect(ctx, connstr)
	if err != nil {
		log.Error(ctx, "failed to connect to database", zap.Error(err))
		return ExecStats{
			Error: err,
		}
	}
	defer conn.Close(ctx)

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
				stats.Error = err
				return stats
			}
			elapsed := time.Since(start)

			stats.Count++

			stats.Min = min(stats.Min, elapsed)
			stats.Max = max(stats.Max, elapsed)
			sum += elapsed
		}
	}

	if stats.Count > 0 {
		stats.Avg = sum / time.Duration(stats.Count)
	}
	return stats
}
