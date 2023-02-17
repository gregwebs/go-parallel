package parallel_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/gregwebs/go-recovery"
	"github.com/stretchr/testify/assert"
	"github.com/gregwebs/go-parallel"
)

func TestRecoveredCall(t *testing.T) {
	err := recovery.Call(func() error {
		return nil
	})
	assert.Nil(t, err)
	err = recovery.Call(func() error {
		return fmt.Errorf("return error")
	})
	assert.NotNil(t, err)
	err = recovery.Call(func() error {
		panic("panic")
	})
	assert.NotNil(t, err)
	assert.Equal(t, "panic", err.Error())
}

func TestGoRecovered(t *testing.T) {
	noError := func(err error) {
		assert.Nil(t, err)
	}
	errHappened := func(err error) {
		assert.NotNil(t, err)
	}
	recovery.GoHandler(noError, func() error {
		return nil
	})
	recovery.GoHandler(errHappened, func() error {
		panic("panic")
	})

	wait := make(chan struct{})
	go recovery.GoHandler(noError, func() error {
		wait <- struct{}{}
		return nil
	})
	go recovery.GoHandler(errHappened, func() error {
		defer func() { wait <- struct{}{} }()
		panic("panic")
	})
	<-wait
	<-wait
}

func TestConcurrent(t *testing.T) {
	var err error
	workNone := func(_ int) error { return nil }
	err = parallel.Concurrent(0, workNone)
	assert.Nil(t, err)
	err = parallel.Concurrent(2, workNone)
	assert.Nil(t, err)

	tracked := make([]bool, 10)
	workTracked := func(i int) error { tracked[i] = true; return nil }
	err = parallel.Concurrent(0, workTracked)
	assert.Nil(t, err)
	assert.False(t, tracked[0])

	tracked = make([]bool, 10)
	err = parallel.Concurrent(2, workTracked)
	assert.Nil(t, err)
	assert.False(t, tracked[2])
	assert.True(t, tracked[1])
	assert.True(t, tracked[0])
}

func TestQueueWorkers(t *testing.T) {
	var err error
	var queue chan int
	workNone := func(_ int) error { return nil }
	queue = make(chan int)
	err = parallel.CollectErrors(parallel.QueueWorkers(0, queue, workNone))
	assert.Nil(t, err)
	close(queue)

	queue = make(chan int)
	go recovery.Go(func() error {
		queue <- 1
		queue <- 1
		close(queue)
		return nil
	})
	err = parallel.CollectErrors(parallel.QueueWorkers(2, queue, workNone))
	assert.Nil(t, err)

	tracked := make([]bool, 10)
	workTracked := func(i int) error { tracked[i] = true; return nil }
	queue = make(chan int)
	go recovery.Go(func() error {
		queue <- 0
		queue <- 1
		close(queue)
		return nil
	})
	err = parallel.CollectErrors(parallel.QueueWorkers(2, queue, workTracked))
	assert.Nil(t, err)
	assert.False(t, tracked[2])
	assert.True(t, tracked[1])
	assert.True(t, tracked[0])
}

func arrayWorkers1[T any](nParallel int, objects []T, worker func(int, T) error) error {
	cancel := make(chan struct{})
	errors := parallel.ArrayWorkers1(nParallel, objects, cancel, worker)
	return parallel.CancelAfterFirstError(cancel, errors)
}

func TestArrayWorkers1(t *testing.T) {
	var err error
	workNone := func(_ int, _ bool) error { return nil }
	tracked := make([]bool, 10)
	err = arrayWorkers1(0, tracked, workNone)
	assert.Nil(t, err)

	tracked = make([]bool, 10)
	err = arrayWorkers1(10, tracked, workNone)
	assert.Nil(t, err)

	tracked = make([]bool, 10)
	workTracked := func(i int, _ bool) error { tracked[i] = true; return nil }
	err = arrayWorkers1(0, tracked, workTracked)
	assert.Nil(t, err)
	assert.False(t, tracked[0])

	tracked = make([]bool, 10)
	workTracked = func(i int, _ bool) error { tracked[i] = true; return nil }
	err = arrayWorkers1(2, tracked, workTracked)
	assert.Nil(t, err)
	assert.True(t, tracked[0])
	assert.True(t, tracked[1])
	assert.True(t, tracked[2])
	assert.True(t, tracked[9])
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

func batchWorkers[T any](nParallel int, batchSize int, objects []T, worker func([]T) error) error {
	bw := parallel.BatchWork{
		Size:        batchSize,
		Parallelism: nParallel,
		Cancel:      make(chan struct{}),
	}
	queue := parallel.BatchedChannel(bw, objects)
	errors := parallel.QueueWorkers(bw.Parallelism, queue, worker)
	return parallel.CancelAfterFirstError(bw.Cancel, errors)
}

func TestBatchWorkers(t *testing.T) {
	var err error
	workNone := func(_ []bool) error { return nil }
	tracked := make([]bool, 10)
	err = batchWorkers(0, 2, tracked, workNone)
	assert.Nil(t, err)

	tracked = make([]bool, 10)
	err = batchWorkers(2, 2, tracked, workNone)
	assert.Nil(t, err)

	work := make([]int, 10)
	for i, _ := range work {
		work[i] = i + 1
	}
	output := SyncNumber{Number: 0}
	add := func(batch []int) error {
		for _, x := range batch {
			output.Add(x)
		}
		return nil
	}
	err = batchWorkers(1, 1, work, add)
	assert.Nil(t, err)
	assert.Equal(t, output.Number, 55)

	output = SyncNumber{Number: 0}
	err = batchWorkers(2, 2, work, add)
	assert.Nil(t, err)
	assert.Equal(t, output.Number, 55)

	output = SyncNumber{Number: 0}
	err = batchWorkers(3, 3, work, add)
	assert.Nil(t, err)
	assert.Equal(t, output.Number, 55)
}
