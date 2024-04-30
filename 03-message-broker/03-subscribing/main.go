package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})
	subcriber, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}
	msgCh, err := subcriber.Subscribe(context.Background(), "progress")
	if err != nil {
		panic(err)
	}

	for msg := range msgCh {
		fmt.Printf("Message ID: %s - %s%%\n", msg.UUID, string(msg.Payload))
		msg.Ack()
	}
}
