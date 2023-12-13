package parallel

import (
	"reflect"
	"sync"

	"github.com/gregwebs/go-recovery"
	"go.uber.org/multierr"
)

// n is the number of go routines to spawn
// errors are returned as a combined multierr
// panics in the given function are recovered and converted to an error
func Concurrent(n int, fn func(int) error) error {
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go recovery.GoHandler(func(err error) { errs[i] = err }, func() error {
			defer wg.Done()
			errs[i] = fn(i)
			return nil
		})
	}
	wg.Wait()
	return multierr.Combine(errs...)
}

// try to send to a channel, return true if sent, false if not
func TrySend[T any](c chan<- T, obj T) bool {
	select {
	case c <- obj:
		return true
	default:
		return false
	}
}

// try to send to a channel, return true if sent, false if not
func TryRecv[T any](c <-chan T) (receivedObject T, received bool) {
	select {
	case receivedObject = <-c:
		received = true
	default:
		received = false
	}
	return
}

// Combine all errors from a channel of errors into a single multierr error.
func CollectErrors(errors <-chan error) error {
	var errResult error
	for e := range errors {
		errResult = multierr.Combine(errResult, e)
	}
	return errResult
}

// spawn N parallel workers that apply fn to T.
// Any panics in the given fn are recovered and returned as errors.
// Work is taken out of the given queue and given to the first available worker.
// Once all workers are full, reflect.Select is used to select the next worker.
// Close the given queue to shutdown the workers.
//
// Workers will continue to work after encountering an error.
// Errors are sent to the returned error channel.
// When the given queue is closed and the work is processed, the returned error channel will be closed.
func QueueWorkers[T any](n int, queue <-chan T, fn func(T) error) <-chan error {
	errors := make(chan error)
	chans := make([]chan T, n)
	for i := range chans {
		chans[i] = make(chan T)
	}
	closeChans := func() {
		for i := range chans {
			close(chans[i])
		}
	}

	var chanValues []reflect.Value
	distributeWork := func(work T) {
		for _, c := range chans {
			if worked := TrySend(c, work); worked {
				return
			}
		}

		// No worker channels were available
		// Use a select to let the runtime choose the next available channel
		// Our channels are dynamic, so to use reflect.Select
		// Reflection has some additional overhead
		// However, for most use cases of this function it shouldn't be noticeable.
		if chanValues == nil {
			chanValues = make([]reflect.Value, len(chans))
			for i := range chans {
				chanValues[i] = reflect.ValueOf(chans[i])
			}
		}
		workReflected := reflect.ValueOf(work)
		cases := make([]reflect.SelectCase, len(chans))
		for i := range chans {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectSend, Chan: chanValues[i], Send: workReflected}
		}
		_, _, _ = reflect.Select(cases)
	}

	// distribute the work over the worker channels
	go recovery.Go(func() error {
		defer closeChans()
		for work := range queue {
			distributeWork(work)
		}
		return nil
	})

	// run a concurrent worker for each channel
	// send errors to the error channel
	go func() {
		defer close(errors)

		// work the channels in parallel
		extraErrors := Concurrent(n, func(i int) error {
			// Attempt to send an error or recovered panic to the error channel.
			// But if the error channel is blocked, don't wait:
			// go ahead and process the work
			// try sending the error again when done processing that work
			var errs error
			for work := range chans[i] {
				err := recovery.Call(func() error { return fn(work) })
				if err != nil {
					errs = multierr.Combine(errs, err)
				}
				if errs != nil {
					if sent := TrySend(errors, err); sent {
						errs = nil
					}
				}
			}
			return errs
		})

		// after all work is processed, ensure all errors are sent before the channel is closed
		if extraErrors != nil {
			errors <- extraErrors
		}
	}()

	return errors
}

// For functions that take a cancel channel and return an error channel.
// This helper will cancel on the first error.
// Waits for all processing to complete.
// Returns a multierror of any resulting errors.
//
// This does not immediately stop existing processing (which requires panicing).
// This cancels future work distribution.
func CancelAfterFirstError(cancel chan struct{}, errors <-chan error) error {
	if errResult := <-errors; errResult != nil {
		cancel <- struct{}{}
		return multierr.Combine(errResult, CollectErrors(errors))
	}
	return nil
}

type withIndex[T any] struct {
	Index int
	val   T
}

// Just operate on one object at a time
// Processing continues until completion or a value is read from the cancel channel
// Returns a channel of errors
// Uses QueueWorkers under the hood
func ArrayWorkers1[T any](nParallel int, objects []T, cancel <-chan struct{}, fn func(int, T) error) <-chan error {
	if len(objects) < nParallel {
		nParallel = len(objects)
	}

	if len(objects) == 1 {
		errors := make(chan error)
		go recovery.Go(func() error {
			errors <- recovery.Call(func() error {
				return fn(0, objects[0])
			})
			return nil
		})
		return errors
	}

	queue := make(chan withIndex[T])
	// put the objects into the queue
	go recovery.Go(func() error {
		defer close(queue)

		for i, object := range objects {
			if _, canceled := TryRecv(cancel); canceled {
				break
			}
			queue <- withIndex[T]{Index: i, val: object}
		}
		return nil
	})

	withIndexFn := func(wi withIndex[T]) error {
		return fn(wi.Index, wi.val)
	}

	return QueueWorkers(nParallel, queue, withIndexFn)
}

// If the length is too small, decrease the batch size
func adjustBatchingForLength(nParallel, batchSize, total int) int {
	if batchSize*nParallel > total {
		if nParallel > total {
			return 1
		} else {
			return total / nParallel
		}
	}
	return batchSize
}

type BatchWork struct {
	Size        int
	Parallelism int
	Cancel      chan struct{}
}

// BatchWorkers combines BatchedChannel, QueueWorkers, and CancelAfterFirstError
// The given objects are batched up and worked in parallel
func BatchWorkers[T any](bw BatchWork, objects []T, worker func([]T) error) error {
	if bw.Cancel == nil {
		bw.Cancel = make(chan struct{})
	}
	queue := BatchedChannel(bw, objects)
	errors := QueueWorkers(bw.Parallelism, queue, worker)
	return CancelAfterFirstError(bw.Cancel, errors)
}

// If the length is too small, decrease the batch size
func (bw *BatchWork) AdjustForSmallLength(total int) int {
	bw.Size = adjustBatchingForLength(bw.Parallelism, bw.Size, total)
	return bw.Size
}

// BatchedChannel sends slices of Batchwork.Size objects to the resulting channel
func BatchedChannel[T any](bw BatchWork, objects []T) <-chan []T {
	sender := make(chan []T)
	total := len(objects)
	if total == 0 {
		return sender
	}
	batchSize := bw.AdjustForSmallLength(total)

	go recovery.Go(func() error {
		defer close(sender)

		tryCancel := func() bool { return false }
		if bw.Cancel != nil {
			tryCancel = func() bool {
				_, canceled := TryRecv(bw.Cancel)
				return canceled
			}
		}

		for i := 0; ((i + 1) * batchSize) <= total; i += 1 {
			lower := i * batchSize
			if canceled := tryCancel(); canceled {
				return nil
			}
			sender <- objects[lower : lower+batchSize]
		}
		rem := total % batchSize
		if rem > 0 {
			if canceled := tryCancel(); canceled {
				return nil
			}
			sender <- objects[total-rem : total]
		}
		return nil
	})

	return sender
}

func MapBatches[T any, U any](nParallel int, objects []T, fn func(T) (U, error)) ([]U, error) {
	total := len(objects)
	out := make([]U, total)
	batchSize := total / nParallel
	err := Concurrent(nParallel, func(n int) (err error) {
		max := (n + 1) * batchSize
		// Because we do integer division there is an extra remainder
		// Add it onto the end
		if n == nParallel-1 {
			max = total
		}
		lower := n * batchSize
		// log.Printf("[index] parallel batch %d from %d:%d", n+1, lower, max)
		for i, object := range objects[lower:max] {
			var transformed U
			err := recovery.Call(func() error {
				var e error
				transformed, e = fn(object)
				return e
			})
			if err != nil {
				return err
			}
			out[lower+i] = transformed
		}
		// log.Printf("[index] parallel batch complete %d:%d", lower, max)
		return nil
	})
	return out, err
}
