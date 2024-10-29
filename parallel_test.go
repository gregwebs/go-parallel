package parallel_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/gregwebs/go-parallel"
	"github.com/gregwebs/go-recovery"
	"github.com/shoenig/test/must"
)

func TestQueueWorkers(t *testing.T) {
	workNone := func(_ int) error { return nil }
	{
		queue := make(chan int)
		close(queue)
		err := parallel.CollectErrors(parallel.QueueWorkers(0, queue, workNone))
		must.Nil(t, err)
	}

	{
		queue := make(chan int)
		go recovery.Go(func() error {
			queue <- 1
			queue <- 1
			close(queue)
			return nil
		})
		err := parallel.CollectErrors(parallel.QueueWorkers(2, queue, workNone))
		must.Nil(t, err)
	}

	{
		tracked := make([]bool, 10)
		workTracked := func(i int) error { tracked[i] = true; return nil }
		queue := make(chan int)
		go recovery.Go(func() error {
			queue <- 0
			queue <- 1
			close(queue)
			return nil
		})
		err := parallel.CollectErrors(parallel.QueueWorkers(2, queue, workTracked))
		must.Nil(t, err)
		must.False(t, tracked[2])
		must.True(t, tracked[1])
		must.True(t, tracked[0])
	}
}

func arrayWorkers1[T any](nParallel int, objects []T, worker func(int, T) error) []error {
	cancel := make(chan struct{})
	errors := parallel.ArrayWorkers1(nParallel, objects, cancel, worker)
	return parallel.CancelAfterFirstError(cancel, errors)
}

func TestCancelAfterFirstError(t *testing.T) {
	cancel := make(chan struct{}, 10)
	{
		errChan := make(chan error, 10)
		close(errChan)
		errs := parallel.CancelAfterFirstError(cancel, errChan)
		must.Nil(t, errs)
	}

	{
		errChan := make(chan error, 10)
		errChan <- errors.New("first error")
		errChan <- errors.New("second error")
		close(errChan)
		errs := parallel.CancelAfterFirstError(cancel, errChan)
		must.Len(t, 2, errs)
	}
}

func TestArrayWorkers1(t *testing.T) {
	workNone := func(_ int, _ bool) error { return nil }
	{
		tracked := make([]bool, 10)
		err := arrayWorkers1(0, tracked, workNone)
		must.Nil(t, err)
	}

	{
		tracked := make([]bool, 1)
		err := arrayWorkers1(10, tracked, workNone)
		must.Nil(t, err)
	}

	{
		tracked := make([]bool, 10)
		workTracked := func(i int, _ bool) error { tracked[i] = true; return nil }
		err := arrayWorkers1(1, tracked, workTracked)
		must.Nil(t, err)
		must.SliceNotContains(t, tracked, false)
	}

	{
		tracked := make([]bool, 10)
		workTracked := func(i int, _ bool) error { tracked[i] = true; return nil }
		err := arrayWorkers1(2, tracked, workTracked)
		must.Nil(t, err)
		must.SliceNotContains(t, tracked, false)
	}
}

type SyncNumber struct {
	Number int
	sync.Mutex
}

func (sn *SyncNumber) Add(x int) {
	sn.Lock()
	defer sn.Unlock()
	sn.Number = sn.Number + x
}

func TestBatchWorkers(t *testing.T) {
	workNone := func(_ []bool) error { return nil }
	{
		tracked := make([]bool, 10)
		bw := parallel.BatchWork{Size: 2, Parallelism: 0}
		err := parallel.BatchWorkers(bw, tracked, workNone)
		must.Nil(t, err)
	}

	{
		tracked := make([]bool, 10)
		bw := parallel.BatchWork{Size: 2, Parallelism: 2}
		err := parallel.BatchWorkers(bw, tracked, workNone)
		must.Nil(t, err)
	}

	output := SyncNumber{Number: 0}
	add := func(batch []int) error {
		for _, x := range batch {
			output.Add(x)
		}
		return nil
	}
	work := make([]int, 10)
	for i := range work {
		work[i] = i + 1
	}

	{
		output = SyncNumber{Number: 0}
		bw := parallel.BatchWork{Size: 1, Parallelism: 1}
		err := parallel.BatchWorkers(bw, work, add)
		must.Nil(t, err)
		must.Eq(t, output.Number, 55)
	}

	{
		output = SyncNumber{Number: 0}
		bw := parallel.BatchWork{Size: 2, Parallelism: 2}
		err := parallel.BatchWorkers(bw, work, add)
		must.Nil(t, err)
		must.Eq(t, output.Number, 55)
	}

	{
		output = SyncNumber{Number: 0}
		bw := parallel.BatchWork{Size: 3, Parallelism: 3}
		err := parallel.BatchWorkers(bw, work, add)
		must.Nil(t, err)
		must.Eq(t, output.Number, 55)
	}
}

func TestBatchWorkersSmallBatchSize(t *testing.T) {
	output := SyncNumber{Number: 0}
	add := func(batch []int) error {
		for _, x := range batch {
			output.Add(x)
		}
		return nil
	}
	work := make([]int, 3)
	for i := range work {
		work[i] = i + 1
	}
	bw := parallel.BatchWork{Size: 1, Parallelism: 2}
	err := parallel.BatchWorkers(bw, work, add)
	must.Nil(t, err)
	must.Eq(t, 6, output.Number)
}
