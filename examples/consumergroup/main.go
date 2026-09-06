// Command consumergroup demonstrates coordinated consumption with a
// StreamBus consumer group.
//
// The group consumer joins a group through the broker's coordinator, receives
// a partition assignment, heartbeats to hold its membership, and commits
// offsets back to the coordinator so a restart resumes where it left off.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gstreamio/streambus-sdk/client"
)

func main() {
	fmt.Println("StreamBus SDK - Consumer Group Example")
	fmt.Println("======================================")
	fmt.Println()

	// Ctrl-C leaves the group cleanly rather than waiting for the session
	// timeout to expire, so a rebalance starts immediately.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config := client.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}

	c, err := client.New(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	topic := "sdk-example"
	if err := c.CreateTopic(ctx, topic, 3, 1); err != nil {
		log.Printf("Topic creation: %v (may already exist)", err)
	}

	groupConfig := client.DefaultGroupConsumerConfig()
	groupConfig.GroupID = "sdk-example-group"
	groupConfig.Topics = []string{topic}
	groupConfig.ClientID = fmt.Sprintf("example-%d", os.Getpid())

	gc, err := client.NewGroupConsumer(c, groupConfig)
	if err != nil {
		log.Fatalf("Failed to create group consumer: %v", err)
	}
	defer gc.Close()

	// Subscribe joins the group and blocks until an assignment arrives.
	if err := gc.Subscribe(ctx); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	fmt.Printf("Joined group %q with assignment: %v\n\n",
		groupConfig.GroupID, gc.Assignment())

	fmt.Println("Polling for messages (Ctrl-C to leave the group)...")
	for ctx.Err() == nil {
		batches, err := gc.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Poll failed: %v", err)
			time.Sleep(time.Second)
			continue
		}

		offsets := make(map[string]map[int32]int64)
		for topic, partitions := range batches {
			for partition, messages := range partitions {
				for _, msg := range messages {
					fmt.Printf("  %s[%d]@%d key=%s value=%s\n",
						topic, partition, msg.Offset,
						string(msg.Key), string(msg.Value))
				}

				if len(messages) == 0 {
					continue
				}
				// Commit the offset to resume from: one past the last
				// message actually processed.
				last := messages[len(messages)-1].Offset
				if offsets[topic] == nil {
					offsets[topic] = make(map[int32]int64)
				}
				offsets[topic][partition] = last + 1
			}
		}

		if len(offsets) > 0 {
			if err := gc.CommitSync(ctx, offsets); err != nil {
				log.Printf("Commit failed: %v", err)
			}
		}
	}

	fmt.Println("\nLeaving group...")
}
