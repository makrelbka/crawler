package crawler

import (
	"context"
	"crawler/internal/fs"
	"crawler/internal/workerpool"
	"encoding/json"
	"sync"
)

// Configuration holds the configuration for the crawler, specifying the number of workers for
// file searching, processing, and accumulating tasks. The values for SearchWorkers, FileWorkers,
// and AccumulatorWorkers are critical to efficient performance and must be defined in
// every configuration.
type Configuration struct {
	SearchWorkers      int // Number of workers responsible for searching files.
	FileWorkers        int // Number of workers for processing individual files.
	AccumulatorWorkers int // Number of workers for accumulating results.
}

// Combiner is a function type that defines how to combine two values of type R into a single
// result. Combiner is not required to be thread-safe
//
// Combiner can either:
//   - Modify one of its input arguments to include the result of the other and return it,
//     or
//   - Create a new combined result based on the inputs and return it.
//
// It is assumed that type R has a neutral element (forming a monoid)
type Combiner[R any] func(current R, accum R) R

// Crawler represents a concurrent crawler implementing a map-reduce model with multiple workers
// to manage file processing, transformation, and accumulation tasks. The crawler is designed to
// handle large sets of files efficiently, assuming that all files can fit into memory
// simultaneously.
type Crawler[T, R any] interface {
	// Collect performs the full crawling operation, coordinating with the file system
	// and worker pool to process files and accumulate results. The result type R is assumed
	// to be a monoid, meaning there exists a neutral element for combination, and that
	// R supports an associative combiner operation.
	// The result of this collection process, after all reductions, is returned as type R.
	//
	// Important requirements:
	// 1. Number of workers in the Configuration is mandatory for managing workload efficiently.
	// 2. FileSystem and Accumulator must be thread-safe.
	// 3. Combiner does not need to be thread-safe.
	// 4. If an accumulator or combiner function modifies one of its arguments,
	//    it should return that modified value rather than creating a new one,
	//    or alternatively, it can create and return a new combined result.
	// 5. Context cancellation is respected across workers.
	// 6. Type T is derived by json-deserializing the file contents, and any issues in deserialization
	//    must be handled within the worker.
	// 7. The combiner function will wait for all workers to complete, ensuring no goroutine leaks
	//    occur during the process.
	Collect(
		ctx context.Context,
		fileSystem fs.FileSystem,
		root string,
		conf Configuration,
		accumulator workerpool.Accumulator[T, R],
		combiner Combiner[R],
	) (R, error)
}

type crawlerImpl[T, R any] struct{}

func New[T, R any]() *crawlerImpl[T, R] {
	return &crawlerImpl[T, R]{}
}

func (c *crawlerImpl[T, R]) Collect(
	ctx context.Context,
	fileSystem fs.FileSystem,
	root string,
	conf Configuration,
	accumulator workerpool.Accumulator[T, R],
	combiner Combiner[R],
) (R, error) {
	fileChan := make(chan string) // Channel to store file paths
	var errOutput error           // Variable to store errors
	var result R

	// Wait group to manage combiner goroutine
	wg := new(sync.WaitGroup)

	// Create worker pools for processing
	pool := workerpool.New[T, R]()
	stringPool := workerpool.New[string, T]()

	// Transform file paths into parsed data
	transformedChan := stringPool.Transform(ctx, conf.FileWorkers, fileChan, func(file string) T {
		return c.transformer(fileSystem, file, &errOutput)
	})

	// Accumulate parsed data into results
	accumulateResult := pool.Accumulate(ctx, conf.AccumulatorWorkers, transformedChan, accumulator)

	// Move combiner logic into its own function
	wg.Add(1)
	go c.combineResults(ctx, accumulateResult, combiner, &result, wg)

	// List all files using the searcher
	stringPool.List(ctx, conf.SearchWorkers, root, func(parentPath string) []string {
		return c.searcher(ctx, fileSystem, parentPath, fileChan, &errOutput)
	})

	// Close file channel when done
	close(fileChan)
	// Waiting for combiner to finish
	wg.Wait()

	if errOutput != nil {
		// Return errors encountered during execution
		return result, errOutput
	}

	// Return the result or context error if canceled
	return result, ctx.Err()
}

func (c *crawlerImpl[T, R]) searcher(
	ctx context.Context,
	fileSystem fs.FileSystem,
	parentPath string,
	fileChan chan<- string,
	errOutput *error,
) []string {
	defer func() {
		// Catch panics and store the error
		if recoverErr := recover(); recoverErr != nil {
			if errRead, ok := recoverErr.(error); ok {
				*errOutput = errRead
			}
		}
	}()

	// Read directory contents
	entries, errRead := fileSystem.ReadDir(parentPath)
	if errRead != nil {
		// Store the read error
		*errOutput = errRead
	}

	var result []string
	// Generate paths for each child
	for _, entry := range entries {
		path := fileSystem.Join(parentPath, entry.Name())
		if entry.IsDir() {
			result = append(result, path)
		} else {
			select {
			// Exit if context is canceled
			case <-ctx.Done():
				return nil
			// Send file path to channel
			case fileChan <- path:
			}
		}
	}
	// Return subdirectories
	return result
}

func (c *crawlerImpl[T, R]) transformer(
	fileSystem fs.FileSystem,
	file string,
	errOutput *error,
) T {
	defer func() {
		// Catch panics and store the error
		if recoverErr := recover(); recoverErr != nil {
			if errRead, ok := recoverErr.(error); ok {
				*errOutput = errRead
			}
		}
	}()

	// Default empty var
	var empty T

	// Try to open the file
	data, errOpen := fileSystem.Open(file)
	if errOpen != nil {
		*errOutput = errOpen
		return empty
	}
	defer data.Close() // Ensure the file is closed

	// Decode file content
	decoder := json.NewDecoder(data)
	var parsed T
	if errDecode := decoder.Decode(&parsed); errDecode != nil {
		*errOutput = errDecode
		return empty
	}

	// Return decoded content
	return parsed
}

func (c *crawlerImpl[T, R]) combineResults(
	ctx context.Context,
	accumulateResult <-chan R,
	combiner Combiner[R],
	result *R,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for {
		select {
		// Exit if context is canceled
		case <-ctx.Done():
			return
		case el, ok := <-accumulateResult:
			// Exit when channel is closed
			if !ok {
				return
			}
			// Combining results
			*result = combiner(*result, el)
		}
	}
}

