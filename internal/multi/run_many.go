package multi

import (
	"context"
	"sync"

	"github.com/petuhovskiy/overload/internal/log"
	"go.uber.org/zap"
)

func RunMany(ctx context.Context, n int, f func(ctx context.Context) error) {
	wg := sync.WaitGroup{}
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
