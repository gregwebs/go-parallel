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

This library relies on go-recovery to trap panics that occur in go routines.
go-recovery by default will log panics but can be configured to send them to an error monitoring service.

For maximum flexibility with error handling, many of the parallel functions return an error channel.
Any errors that occur in work functions will be put into the error channel.
There are helpers for common patterns for dealing with the errors:

* CollectErrors
* CancelAfterFirstError
