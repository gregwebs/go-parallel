# go-parallel

parallelism in Go using generics

## Parallel functions

Higher level

* MapBatches - a batched parallel map
* BatchWorkers - spawn parallel workers to work batches
* ArrayWorkers1 - unbatched parallel map

Lower Level

* QueueWorkers - spawn parallel workers
* BatchedChannel - for batching data into a channel

## Error handling

This library relies on go-recovery to trap panics that occur in user supplied work functions.
This library does have unhandled panics, but only in places where panics should never occur.
Errors and panics are written to an error channel for maximum flexibility.
There are helpers for common patterns for dealing with errors:

* CollectErrors (wait and convert to a slice)
* CancelAfterFirstError (cancel and wait and convert to a slice)

## Concurrency helpers

See [go-concurrent](https://github.com/gregwebs/go-concurrent), which this library uses.
