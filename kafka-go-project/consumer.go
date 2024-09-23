package main

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	numWorkers   = 5   // Number of workers in the pool
	taskQueueCap = 100 // Capacity of the task queue
)

type Task struct {
	Record *kgo.Record
	Client *kgo.Client
}

func main() {
	// Create a new Kafka client
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("my_consumer_group"),
		kgo.ConsumeTopics("my_topic"),
		kgo.DisableAutoCommit(), // We'll commit offsets manually
	)
	if err != nil {
		panic("Unable to create Kafka client: " + err.Error())
	}
	defer client.Close()

	// Create a context to manage cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Handle graceful shutdown on interrupt signal
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		fmt.Println("\nReceived interrupt signal. Shutting down...")
		cancel()
	}()

	// Create a task queue (channel) for tasks
	taskQueue := make(chan Task, taskQueueCap)

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, &wg, taskQueue)
	}

	// Start the main loop to fetch messages
	for {
		select {
		case <-ctx.Done():
			// Context canceled, stop fetching messages
			close(taskQueue) // Signal workers to stop after processing current tasks
			wg.Wait()        // Wait for all workers to finish
			fmt.Println("All workers have completed. Exiting.")
			return
		default:
			// Poll for messages
			fetches := client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				break
			}

			// Handle errors
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					fmt.Printf("Error: %v\n", err)
				}
				continue
			}

			// Enqueue messages as tasks
			fetches.EachRecord(func(record *kgo.Record) {
				task := Task{
					Record: record,
					Client: client,
				}
				taskQueue <- task
			})
		}
	}
}

// Worker function processes tasks from the taskQueue
func worker(ctx context.Context, wg *sync.WaitGroup, taskQueue <-chan Task) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			// Context canceled, stop the worker
			return
		case task, ok := <-taskQueue:
			if !ok {
				// taskQueue closed, no more tasks
				return
			}
			// Process the message
			err := processMessage(task.Record)
			if err != nil {
				fmt.Printf("Failed to process message at offset %d: %v\n", task.Record.Offset, err)
				// Handle the error (e.g., log, retry, send to dead-letter queue)
			} else {
				// Manually commit the offset after successful processing
				commitOffsets(task.Client, task.Record)
			}
		}
	}
}

// processMessage handles the business logic for a message
func processMessage(record *kgo.Record) error {
	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("Worker processed message at offset %d: %s\n", record.Offset, string(record.Value))
	// Add your actual processing logic here

	return nil // Return an error if processing fails
}

// commitOffsets commits the offset after processing
func commitOffsets(client *kgo.Client, record *kgo.Record) {
	offsets := map[string]map[int32]kgo.EpochOffset{
		record.Topic: {
			record.Partition: kgo.EpochOffset{
				Offset: record.Offset + 1,
			},
		},
	}

	client.CommitOffsets(context.Background(), offsets, nil)
}
