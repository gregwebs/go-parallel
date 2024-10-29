# go-parallel

parallelism in Go using generics

## Parallel functions

* MapBatches
* QueueWorkers
* ArrayWorkers1
* BatchedChannel

## Concurrency helpers

* Concurrent: run n functions concurrently
* TrySend
* TryRecv

## Error handling

This library relies on go-recovery to trap panics that occur in user supplied work functions.
This library does have unhandled panics, but only in places where panics should never occur.
Errors and panics are written to an error channel for maximum flexibility.
There are helpers for common patterns for dealing with errors:

* CollectErrors (wait and convert to a slice)
* CancelAfterFirstError (cancel and wait and convert to a slice)
