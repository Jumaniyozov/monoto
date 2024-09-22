package main

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Create a new Kafka client as a consumer
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("my_consumer_group"),
		kgo.ConsumeTopics("my_topic"),
	)
	if err != nil {
		panic("Unable to create Kafka client: " + err.Error())
	}
	defer client.Close()

	// Set up a context to handle cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown on interrupt signal
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		cancel()
	}()

kafka_loop:
	for {
		fetches := client.PollFetches(ctx)

		select {
		case <-ctx.Done():
			break kafka_loop
		default:
			if fetches.IsClientClosed() {
				break kafka_loop
			}
			// Handle any errors encountered
			fetches.EachError(func(topic string, partition int32, err error) {
				log.Printf("Error consuming from topic %s partition %d: %v", topic, partition, err)
			})

			// Process the messages
			fetches.EachRecord(func(record *kgo.Record) {
				fmt.Printf("Received message: key=%s value=%s offset=%d\n", string(record.Key), string(record.Value), record.Offset)
			})
		}
	}

	fmt.Println("Consumer shutting down.")
}
