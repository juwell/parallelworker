package parallelworker

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

type ParallelWorker struct {
	workerCount int32
	ctx          context.Context
	finish       chan struct{}
	isWait       bool
}

func NewParallelWorker(ctx context.Context) ParallelWorker {
	return ParallelWorker{
		workerCount: 0,
		ctx:          ctx,
		finish:       make(chan struct{}),
	}
}

type WorkerFunc func(ctx context.Context, params... interface{})

// add a worker function
// this function will start a new goroutine to do your function immediately
// it is thread safe
// params: those are using to save the params when you want to use when the fun be called
func (p *ParallelWorker) AddWorker(fun WorkerFunc, params... interface{}) error {
	if p.isWait {
		return errors.New(`waiting for all done, can't add new work`)
	}

	// add worker count
	atomic.AddInt32(&p.workerCount, 1)
	go func() {
		defer func() {
			newVal := atomic.AddInt32(&p.workerCount, -1)
			// if not at wait mode, can't close p.finish, because may be new work
			// will be added, so when it is wait mode, and count is 0, we should close
			// the p.finish
			if p.isWait && newVal <= 0 {
				defer func() {
					// close an channel may be panic, it is ok, recover and ignore it
					recover()
				}()
				close(p.finish)
			}
		}()

		// real do work
		fun(p.ctx, params...)
	}()

	return nil
}

// waiting for all the work to be done
// this function is using the default timeout value that is 1 minute
// you can't cancel this just only wait for all work to be done, or timeout
// if you want to set your own timeout value, please use WaitContext instead
//
// Advise: always use WaitContext, because you can cancel working at any time
// and any where, that may be can save the cpu & memory resources
func (p *ParallelWorker) Wait() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	p.WaitContext(ctx)
}

// waiting for all the work to be done with Context
//
// this function mast be called after all AddWorking
func (p *ParallelWorker) WaitContext(ctx context.Context) {
	p.isWait = true

	tempCount := atomic.LoadInt32(&p.workerCount)
	if tempCount <= 0 {
		defer func() {
			// close an channel may be panic, it is ok, recover and ignore it
			recover()
		}()
		close(p.finish)
		return
	}

	select {
	case <-ctx.Done():
		// noting need to do here
	case <-p.finish:
		// noting need to do here
	}
}
