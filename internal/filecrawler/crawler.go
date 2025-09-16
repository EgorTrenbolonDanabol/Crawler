package crawler

import (
	"context"
	"crawler/internal/fs"
	"crawler/internal/workerpool"
	"encoding/json"
	"fmt"
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

func (c *crawlerImpl[T, R]) Search(
	ctx context.Context,
	fileSystem fs.FileSystem,
	root string,
	searchWorkers int,
	errorChan chan error,
) <-chan string {
	pool := workerpool.New[string, R]()
	files := make(chan string)
	go func() {
		defer close(files)
		pool.List(ctx, searchWorkers, root, func(parent string) []string {
			defer func() {
				if r := recover(); r != nil {
					// превратили панику в error и отправили в канал ошибок
					switch v := r.(type) {
					case error:
						errorChan <- v
					default:
						errorChan <- fmt.Errorf("%v", v)
					}
				}
			}()
			slice, err := fileSystem.ReadDir(parent)

			if err != nil {
				errorChan <- err
				return nil
			}

			ans := make([]string, 0)
			for _, entry := range slice {
				path := fileSystem.Join(parent, entry.Name())
				if entry.IsDir() {
					ans = append(ans, path)
				} else {
					select {
					case <-ctx.Done():
					case files <- path:

					}
				}

			}

			return ans
		})
	}()

	return files
}

func (c *crawlerImpl[T, R]) ProcessFile(
	ctx context.Context,
	fileSystem fs.FileSystem,
	inp <-chan string,
	workers int,
	errorChan chan error,
) <-chan T {
	poolTransform := workerpool.New[string, T]()
	jsons := poolTransform.Transform(ctx, workers, inp, func(filePath string) T {
		var t T

		defer func() {
			if r := recover(); r != nil {
				switch v := r.(type) {
				case error:
					errorChan <- v
				default:
					errorChan <- fmt.Errorf("%v", v)
				}
			}
		}()
		file, err := fileSystem.Open(filePath)

		if err != nil {
			errorChan <- err
			return t
		}
		defer file.Close()

		if err := json.NewDecoder(file).Decode(&t); err != nil {
			errorChan <- err
			return t
		}
		return t
	})
	return jsons

}

func (c *crawlerImpl[T, R]) Combiner(
	ctx context.Context,
	combiner Combiner[R],
	inp <-chan T,
	workers int,
	accumulator workerpool.Accumulator[T, R],
) <-chan R {
	poolAccumulate := workerpool.New[T, R]()

	accumulatedChannel := poolAccumulate.Accumulate(ctx, workers, inp, accumulator)
	var accum R
	res := make(chan R)
	go func() {
		defer close(res)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-accumulatedChannel:
				if !ok {
					select {
					case <-ctx.Done():
						return
					case res <- accum:
					}
					return
				}
				accum = combiner(v, accum)
			}
		}

	}()
	return res

}

func (c *crawlerImpl[T, R]) Collect(
	ctx context.Context,
	fileSystem fs.FileSystem,
	root string,
	conf Configuration,
	accumulator workerpool.Accumulator[T, R],
	combiner Combiner[R],
) (R, error) {
	errorChan := make(chan error)
	ctxErr, cancel := context.WithCancelCause(ctx)
	ctxForPipeline := context.WithoutCancel(ctxErr)

	filesChan := c.Search(ctxErr, fileSystem, root, conf.SearchWorkers, errorChan)                      // gain all files
	processedFiles := c.ProcessFile(ctxForPipeline, fileSystem, filesChan, conf.FileWorkers, errorChan) // transform elements to T type
	combine := c.Combiner(ctxForPipeline, combiner, processedFiles, conf.AccumulatorWorkers, accumulator)
	for {
		select {
		case e := <-errorChan: // if there was an error in something worker
			cancel(e) // calls cancel function with this error
		case val := <-combine: // if result value is calculated
			return val, context.Cause(ctxErr) // returns this value and (perhaps) happened error
		}
	}

}
