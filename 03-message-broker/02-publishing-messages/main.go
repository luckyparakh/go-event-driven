package main

import (
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}
	publisher.Publish("progress", &message.Message{
		UUID:    watermill.NewShortUUID(),
		Payload: []byte("50"),
	})
	publisher.Publish("progress", &message.Message{
		UUID:    watermill.NewShortUUID(),
		Payload: []byte("100"),
	})
}
