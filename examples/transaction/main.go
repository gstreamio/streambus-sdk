// Command transaction demonstrates StreamBus transactional production.
//
// A transactional producer writes to several partitions atomically: readers
// configured for read_committed see either every message in the transaction
// or none of them. The producer registers a transactional ID with the
// coordinator, which fences any earlier producer using the same ID.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gstreamio/streambus-sdk/client"
	"github.com/gstreamio/streambus-sdk/protocol"
)

func main() {
	fmt.Println("StreamBus SDK - Transactional Producer Example")
	fmt.Println("==============================================")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	config := client.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}

	c, err := client.New(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	topic := "sdk-transactions"
	if err := c.CreateTopic(ctx, topic, 3, 1); err != nil {
		log.Printf("Topic creation: %v (may already exist)", err)
	}

	txnConfig := client.DefaultTransactionalProducerConfig()
	txnConfig.TransactionID = "sdk-example-txn"

	tp, err := client.NewTransactionalProducer(c, txnConfig)
	if err != nil {
		log.Fatalf("Failed to create transactional producer: %v", err)
	}
	defer tp.Close()

	// A committed transaction: all three messages become visible together.
	if err := commitBatch(ctx, tp, topic); err != nil {
		log.Fatalf("Committed batch failed: %v", err)
	}

	// An aborted transaction: nothing written here is ever visible to a
	// read_committed consumer.
	if err := abortBatch(ctx, tp, topic); err != nil {
		log.Fatalf("Aborted batch failed: %v", err)
	}

	stats := tp.Stats()
	fmt.Printf("\nStats: committed=%d aborted=%d\n",
		stats.TransactionsCommitted, stats.TransactionsAborted)

	fmt.Println("\n✓ Example completed successfully!")
}

func commitBatch(ctx context.Context, tp *client.TransactionalProducer, topic string) error {
	fmt.Println("Beginning transaction to commit...")
	if err := tp.BeginTransaction(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	for i := 0; i < 3; i++ {
		msg := protocol.Message{
			Key:       []byte(fmt.Sprintf("committed:%d", i)),
			Value:     []byte(fmt.Sprintf("message %d from a committed transaction", i)),
			Timestamp: time.Now().UnixNano(),
		}
		if err := tp.Send(ctx, topic, int32(i), msg); err != nil {
			// Abandon the whole transaction: a partial write must not commit.
			if abortErr := tp.AbortTransaction(ctx); abortErr != nil {
				log.Printf("Abort after send failure also failed: %v", abortErr)
			}
			return fmt.Errorf("send: %w", err)
		}
		fmt.Printf("  staged %s -> partition %d\n", msg.Key, i)
	}

	if err := tp.CommitTransaction(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	fmt.Println("  committed - all three messages are now visible together")
	return nil
}

func abortBatch(ctx context.Context, tp *client.TransactionalProducer, topic string) error {
	fmt.Println("\nBeginning transaction to abort...")
	if err := tp.BeginTransaction(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	msg := protocol.Message{
		Key:       []byte("aborted:0"),
		Value:     []byte("this message is never visible to read_committed"),
		Timestamp: time.Now().UnixNano(),
	}
	if err := tp.Send(ctx, topic, 0, msg); err != nil {
		if abortErr := tp.AbortTransaction(ctx); abortErr != nil {
			log.Printf("Abort after send failure also failed: %v", abortErr)
		}
		return fmt.Errorf("send: %w", err)
	}

	if err := tp.AbortTransaction(ctx); err != nil {
		return fmt.Errorf("abort: %w", err)
	}
	fmt.Println("  aborted - the staged message stays invisible")
	return nil
}
