package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gstreamio/streambus-sdk/client"
)

func main() {
	fmt.Println("StreamBus SDK - Basic Producer/Consumer Example")
	fmt.Println("================================================")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create client configuration
	config := client.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}

	// Create client
	c, err := client.New(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	fmt.Println("Connected to StreamBus broker")

	// Create topic
	topicName := "sdk-example"
	fmt.Printf("Creating topic '%s'...\n", topicName)
	if err := c.CreateTopic(ctx, topicName, 3, 1); err != nil {
		log.Printf("Topic creation: %v (may already exist)", err)
	}

	// Produce messages
	fmt.Println("\nProducing messages...")
	produceMessages(ctx, c, topicName)

	// Wait a bit for messages to be written
	time.Sleep(1 * time.Second)

	// Consume messages
	fmt.Println("\nConsuming messages...")
	consumeMessages(ctx, c, topicName)

	fmt.Println("\n✓ Example completed successfully!")
}

func produceMessages(ctx context.Context, c *client.Client, topic string) {
	producer := client.NewProducer(c)
	defer producer.Close()

	messages := []struct {
		key   string
		value string
	}{
		{"user:1", "User 1 logged in"},
		{"user:2", "User 2 created account"},
		{"user:3", "User 3 made purchase"},
		{"order:100", "Order 100 placed"},
		{"order:101", "Order 101 shipped"},
	}

	for i, msg := range messages {
		err := producer.Send(ctx, topic, []byte(msg.key), []byte(msg.value))
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}
		fmt.Printf("  [%d] Sent: key=%s, value=%s\n", i+1, msg.key, msg.value)
	}

	// Send batches the producer is still holding; without this the messages
	// may not have reached the broker before the consumer below reads.
	if err := producer.Flush(ctx, topic); err != nil {
		log.Printf("Failed to flush: %v", err)
	}

	fmt.Printf("\nProduced %d messages\n", len(messages))
}

func consumeMessages(ctx context.Context, c *client.Client, topic string) {
	// Create consumer for partition 0
	consumer := client.NewConsumer(c, topic, 0)
	defer consumer.Close()

	// Seek to beginning
	if err := consumer.Seek(0); err != nil {
		log.Fatalf("Failed to seek: %v", err)
	}

	// Fetch messages
	messageCount := 0
	for i := 0; i < 10; i++ {
		record, err := consumer.FetchOne(ctx)
		if err != nil {
			// No more messages available
			break
		}

		messageCount++
		fmt.Printf("  [%d] Received: key=%s, value=%s, offset=%d\n",
			messageCount, string(record.Key), string(record.Value), record.Offset)
	}

	fmt.Printf("\nConsumed %d messages\n", messageCount)
}
