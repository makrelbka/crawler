package workerpool

import (
	"context"
	"sync"
)

// Accumulator is a function type used to aggregate values of type T into a result of type R.
// It must be thread-safe, as multiple goroutines will access the accumulator function concurrently.
// Each worker will produce intermediate results, which are combined with an initial or
// accumulated value.
type Accumulator[T, R any] func(current T, accum R) R

// Transformer is a function type used to transform an element of type T to another type R.
// The function is invoked concurrently by multiple workers, and therefore must be thread-safe
// to ensure data integrity when accessed across multiple goroutines.
// Each worker independently applies the transformer to its own subset of data, and although
// no shared state is expected, the transformer must handle any internal state in a thread-safe
// manner if present.
type Transformer[T, R any] func(current T) R

// Searcher is a function type for exploring data in a hierarchical manner.
// Each call to Searcher takes a parent element of type T and returns a slice of T representing
// its child elements. Since multiple goroutines may call Searcher concurrently, it must be
// thread-safe to ensure consistent results during recursive  exploration.
//
// Important considerations:
//  1. Searcher should be designed to avoid race conditions, particularly if it captures external
//     variables in closures.
//  2. The calling function must handle any state or values in closures, ensuring that
//     captured variables remain consistent throughout recursive or hierarchical search paths.
type Searcher[T any] func(parent T) []T

// Pool is the primary interface for managing worker pools, with support for three main
// operations: Transform, Accumulate, and List. Each operation takes an input channel, applies
// a transformation, accumulation, or list expansion, and returns the respective output.
type Pool[T, R any] interface {
	// Transform applies a transformer function to each item received from the input channel,
	// with results sent to the output channel. Transform operates concurrently, utilizing the
	// specified number of workers. The number of workers must be explicitly defined in the
	// configuration for this function to handle expected workloads effectively.
	// Since multiple workers may call the transformer function concurrently, it must be
	// thread-safe to prevent race conditions or unexpected results when handling shared or
	// internal state. Each worker independently applies the transformer function to its own
	// data subset.
	Transform(ctx context.Context, workers int, input <-chan T, transformer Transformer[T, R]) <-chan R

	// Accumulate applies an accumulator function to the items received from the input channel,
	// with results accumulated and sent to the output channel. The accumulator function must
	// be thread-safe, as multiple workers concurrently update the accumulated result.
	// The output channel will contain intermediate accumulated results as R
	Accumulate(ctx context.Context, workers int, input <-chan T, accumulator Accumulator[T, R]) <-chan R

	// List expands elements based on a searcher function, starting
	// from the given element. The searcher function finds child elements for each parent,
	// allowing exploration in a tree-like structure.
	// The number of workers should be configured based on the workload, ensuring each worker
	// independently processes assigned elements.
	List(ctx context.Context, workers int, start T, searcher Searcher[T])
}

type poolImpl[T, R any] struct{}

func New[T, R any]() *poolImpl[T, R] {
	return &poolImpl[T, R]{}
}

func (p *poolImpl[T, R]) Accumulate(
	ctx context.Context,
	workers int,
	input <-chan T,
	accumulator Accumulator[T, R],
) <-chan R {
	// Create a channel for the results
	result := make(chan R)
	// Wait group to synchronize goroutines
	wg := new(sync.WaitGroup)

	for i := 0; i < workers; i++ {
		// Increment wait group counter
		wg.Add(1)
		go func() {
			var acc R // Accumulator variable
			defer func() {
				// Decrement wait group counter
				defer wg.Done()
				select {
				// Exit if context is canceled
				case <-ctx.Done():
					return
				// Send accumulated result
				case result <- acc:
				}
			}()
			for {
				select {
				// Exit if context is canceled
				case <-ctx.Done():
					return
				// Receive input from channel
				case v, ok := <-input:
					// Exit if channel is closed
					if !ok {
						return
					}
					select {
					// Exit if context is canceled
					case <-ctx.Done():
						return
					default:
						// Update accumulator
						acc = accumulator(v, acc)
					}
				}
			}
		}()
	}

	go func() {
		// Close result channel when done
		defer close(result)
		// Wait for all workers to finish
		wg.Wait()
	}()

	// Return the result channel
	return result
}

func (p *poolImpl[T, R]) List(ctx context.Context, workers int, start T, searcher Searcher[T]) {
	// Initialize the current and next levels
	var currentLevel []T
	var nextLevel []T
	var mu sync.Mutex // Mutex to protect shared resources
	var wg sync.WaitGroup // Wait group for worker synchronization

	currentLevel = append(currentLevel, start) // Start with the initial item

	for len(currentLevel) > 0 {
		nextLevel = nil
		// Add workers to the wait group
		wg.Add(workers)

		for i := 0; i < workers; i++ {
			go func() {
				// Decrement wait group counter
				defer wg.Done()
				for {
					select {
					// Exit if context is canceled
					case <-ctx.Done():
						return
					default:
						var item T
						var ok bool

						// Lock to safely access the current level
						mu.Lock()
						if len(currentLevel) > 0 {
							item, currentLevel = currentLevel[0], currentLevel[1:]
							ok = true
						}
						// Unlock after accessing current level
						mu.Unlock()
						// Exit if no items are left
						if !ok {
							return
						}

						// Search for children
						children := searcher(item)
						// Lock to append to the next level
						mu.Lock()
						nextLevel = append(nextLevel, children...)
						// Unlock after appending
						mu.Unlock()
					}
				}
			}()
		}
		// Wait for all workers to complete
		wg.Wait()
		// Move to the next level
		currentLevel = nextLevel
	}
}

func (p *poolImpl[T, R]) Transform(
	ctx context.Context,
	workers int,
	input <-chan T,
	transformer Transformer[T, R],
) <-chan R {
	// Create a channel for the results
	result := make(chan R)

	// Wait group for worker synchronization
	wg := new(sync.WaitGroup)
	for i := 0; i < workers; i++ {
		// Increment wait group counter
		wg.Add(1)
		go func() {
			// Decrement wait group counter
			defer wg.Done()

			for {
				select {
				// Exit if context is canceled
				case <-ctx.Done():
					return
				case v, ok := <-input:
					// Exit if channel is closed
					if !ok {
						return
					}
					select {
					// Exit if context is canceled
					case <-ctx.Done():
						return
					// Transform and send the result
					case result <- transformer(v):
					}
				}
			}
		}()
	}

	go func() {
		defer close(result) // Close result channel when done
		wg.Wait() // Wait for all workers to finish
	}()

	return result // Return the result channel
}
