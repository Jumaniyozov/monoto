package main

import (
	"context"
	"fmt"
	user "github.com/jumaniyozov/monoto/protos/gen/protos"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

func main() {
	metrics := kprom.NewMetrics("namespace")
	// Create a new Kafka client
	client, err := kgo.NewClient(
		kgo.WithHooks(metrics),
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
		//kgo.DialTLSConfig(&tls.Config{
		//	InsecureSkipVerify: true,
		//}),
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
		Key:   []byte("my_key"),
		Value: value,
	}

	// Ensure transactions are managed sequentially
	err = client.BeginTransaction()
	if err != nil {
		log.Printf("Failed to begin transaction: %v\n", err)
	} else {
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

		// Wait for the message to be delivered
		err = client.Flush(context.Background())
		if err != nil {
			return
		}
		fmt.Println("Messages sent in transaction successfully.")
	}
}
