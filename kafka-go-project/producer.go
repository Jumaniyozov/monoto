package main

import (
	"context"
	"fmt"
	user "github.com/jumaniyozov/monoto/protos/gen/protos"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"time"
)

func main() {
	// Create a new Kafka client
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.TransactionalID("my-transactional-id"),
		kgo.ProducerBatchMaxBytes(2*1024*1024),
		kgo.ProducerLinger(50*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.RequestRetries(5),
		kgo.RetryBackoffFn(func(retries int) time.Duration {
			return 100 * time.Millisecond
		}),
		kgo.RequiredAcks(kgo.AllISRAcks()), // Wait for all in-sync replicas
	)
	if err != nil {
		panic("Unable to create Kafka client: " + err.Error())
	}
	defer client.Close()

	usr := &user.User{
		Id:   1,
		Name: "Alice",
	}

	value, err := proto.Marshal(usr)
	if err != nil {
		panic("Failed to serialize user: " + err.Error())
	}

	// Prepare the record to send
	record := &kgo.Record{
		Topic: "my_topic",
		Value: value,
	}

	err = client.BeginTransaction()
	if err != nil {
		panic("Failed to begin transaction: " + err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce messages within the transaction
	client.Produce(ctx, record, nil)
	client.Produce(ctx, record, nil)

	// Commit the transaction
	err = client.EndTransaction(ctx, kgo.TryCommit)
	if err != nil {
		panic("Failed to commit transaction: " + err.Error())
	}

	//// Produce the message

	//client.Produce(ctx, record, func(_ *kgo.Record, err error) {
	//	if err != nil {
	//		fmt.Printf("Failed to deliver message: %v\n", err)
	//	} else {
	//		fmt.Println("Message delivered successfully")
	//	}
	//})
	//
	// Wait for the message to be delivered
	err = client.Flush(context.Background())
	if err != nil {
		return
	}
	fmt.Println("Messages sent in transaction successfully.")
}
