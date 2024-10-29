package parallel

import (
	"reflect"
	"sync"

	"github.com/gregwebs/go-recovery"
)

// Concurrent spawns n go routines each of which runs the given function.
// a panic in the given function is recovered and converted to an error
// errors are returned as []error, a slice of errors
// If there are no errors, the slice will be nil
// To combine the errors as a single error, use errors.Join
func Concurrent(n int, fn func(int) error) []error {
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
	return removeNilErrors(errs...)
}

func sendErrorRecover(c chan<- error, err error) error {
	if err == nil {
		return nil
	}
	return sendWithRecover(c, err)
}

// return true if successful, false if recovered
func sendWithRecover[T any](c chan<- T, obj T) error {
	return recovery.Call(func() error {
		c <- obj
		return nil
	})
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

// try to receive from a channel, return false if nothing received
func TryRecv[T any](c <-chan T) (receivedObject T, received bool) {
	select {
	case receivedObject = <-c:
		received = true
	default:
		received = false
	}
	return
}

// Wait for all errors from a channel of errors.
// errors are returned as []error, a slice of errors
// If there are no errors, the slice will be nil
// To combine the errors as a single error, use errors.Join
func CollectErrors(errChannel <-chan error) []error {
	var errResult []error
	for err := range errChannel {
		errResult = append(errResult, err)
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
	if n == 0 {
		n = 1
	}
	errChannel := make(chan error)
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
	var cases []reflect.SelectCase

	distributeWork := func(work T) {
		for _, c := range chans {
			if worked := TrySend(c, work); worked {
				return
			}
		}

		workReflected := reflect.ValueOf(work)
		// No worker channels were available
		// Use a select to let the runtime choose the next available channel
		if chanValues == nil {
			chanValues = make([]reflect.Value, len(chans))
			for i := range chans {
				chanValues[i] = reflect.ValueOf(chans[i])
			}
		}
		if cases == nil {
			cases = make([]reflect.SelectCase, len(chans))
			for i := range chans {
				cases[i] = reflect.SelectCase{Dir: reflect.SelectSend, Chan: chanValues[i], Send: workReflected}
			}
		} else {
			for i := range chans {
				cases[i].Send = workReflected
			}
		}
		_, _, _ = reflect.Select(cases)
	}

	errorChannelOrPanic := func(err error) {
		if errRecover := sendErrorRecover(errChannel, err); errRecover != nil {
			panic(err)
		}
	}

	// distribute the work over the worker channels
	go recovery.GoHandler(errorChannelOrPanic, func() error {
		defer closeChans()
		for work := range queue {
			distributeWork(work)
		}
		return nil
	})

	// send errors to the error channel
	// An unexpected panic while writing to the error channel will panic
	go recovery.GoHandler(func(err error) { panic(err) }, func() error {
		defer close(errChannel)
		return sendErrorRecover(errChannel, recovery.Call(func() error {
			unsentErrorsAll := make([][]error, n)
			// run a concurrent worker for each channel
			recoveredErrors := Concurrent(n, func(i int) error {
				// Attempt to send an error or recovered panic to the error channel.
				// But if the error channel is blocked, don't wait:
				// go ahead and process the work
				// try sending the error again when done processing that work
				var unsentErrors []error
				for work := range chans[i] {
					if err := recovery.Call(func() error { return fn(work) }); err != nil {
						if sent := TrySend(errChannel, err); !sent {
							unsentErrors = append(unsentErrors, err)
						}
					}
				}
				// Try sending unsent errors again
				// Don't block though so the Go routine can be shutdown
				for unsentI, err := range unsentErrors {
					if sent := TrySend(errChannel, err); sent {
						unsentErrors[unsentI] = nil
					}
				}
				unsentErrorsAll[i] = unsentErrors
				return nil
			})

			// Now block while sending any remaining errors
			for _, errs := range unsentErrorsAll {
				for _, e := range errs {
					if e != nil {
						errChannel <- e
					}
				}
			}

			for _, err := range recoveredErrors {
				errChannel <- err
			}

			return nil
		}))
	})

	return errChannel
}

// For functions that take a cancel channel and return an error channel.
// Attempt to cancel all processing but wait for it to finish.
//
// This helper will trigger the cancel channel after the first error.
// It then waits for the error channel to be closed.
//
// errors are returned as []error, a slice of errors
// If there are no errors, the slice will be nil
// To combine the errors as a single error, use errors.Join
func CancelAfterFirstError(cancel chan struct{}, errChannel <-chan error) []error {
	for {
		errResult, ok := <-errChannel
		if !ok {
			return nil
		}
		if errResult == nil {
			continue
		}
		cancel <- struct{}{}
		return append([]error{errResult}, CollectErrors(errChannel)...)
	}
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

	errChannel := make(chan error, 1)
	if len(objects) == 1 {
		go recovery.GoHandler(func(err error) { panic(err) }, func() error {
			defer close(errChannel)
			err := recovery.Call(func() error {
				return fn(0, objects[0])
			})
			if err != nil {
				errChannel <- err
			}
			return nil
		})
		return errChannel
	}

	queue := make(chan withIndex[T])
	// put the objects into the queue
	go recovery.GoHandler(func(err error) { panic(err) }, func() error {
		defer close(errChannel)
		return sendErrorRecover(errChannel, recovery.Call(func() error {
			defer close(queue)

			for i, object := range objects {
				if _, canceled := TryRecv(cancel); canceled {
					break
				}
				queue <- withIndex[T]{Index: i, val: object}
			}
			return nil
		}))
	})

	withIndexFn := func(wi withIndex[T]) error {
		return fn(wi.Index, wi.val)
	}

	// queue and errChannel are not closed even though errQueueWorkers is?
	errQueueWorkers := QueueWorkers(nParallel, queue, withIndexFn)
	return ChannelMerge(errQueueWorkers, errChannel)
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
func BatchWorkers[T any](bw BatchWork, objects []T, worker func([]T) error) []error {
	if bw.Cancel == nil {
		bw.Cancel = make(chan struct{})
	}
	queue, errBatched := BatchedChannel(bw, objects)
	errors := ChannelMerge(QueueWorkers(bw.Parallelism, queue, worker), errBatched)
	return CancelAfterFirstError(bw.Cancel, errors)
}

// If the length is too small, decrease the batch size
func (bw *BatchWork) AdjustForSmallLength(total int) int {
	bw.Size = adjustBatchingForLength(bw.Parallelism, bw.Size, total)
	return bw.Size
}

// BatchedChannel sends slices of Batchwork.Size objects to the resulting channel.
// The error channel should not have an error but is there in case there is a panic in the batching go routine.
func BatchedChannel[T any](bw BatchWork, objects []T) (<-chan []T, <-chan error) {
	sender := make(chan []T)
	total := len(objects)
	if total == 0 {
		return sender, nil
	}
	batchSize := bw.AdjustForSmallLength(total)

	errChannel := make(chan error, 1)
	go recovery.GoHandler(func(err error) { panic(err) }, func() error {
		defer close(errChannel)
		return sendErrorRecover(errChannel, recovery.Call(func() error {
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
		}))
	})

	return sender, errChannel
}

func MapBatches[T any, U any](nParallel int, objects []T, fn func(T) (U, error)) ([]U, []error) {
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

// This is the same as errors.Join but does not wrap the array
func removeNilErrors(errs ...error) []error {
	n := 0
	for _, err := range errs {
		if err != nil {
			n++
		}
	}
	if n == 0 {
		return nil
	}
	newErrs := make([]error, 0, n)
	for _, err := range errs {
		if err != nil {
			newErrs = append(newErrs, err)
		}
	}
	return newErrs
}

// From this article: https://go.dev/blog/pipelines
func ChannelMerge[T any](cs ...<-chan T) <-chan T {
	var wg sync.WaitGroup
	out := make(chan T)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan T) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
