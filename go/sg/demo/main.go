package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"time"

	"github.com/sandglass/sandglass-client/go/sg"
	"github.com/sandglass/sandglass-grpc/go/sgproto"
)

func main() {
	addr := flag.String("addrs", "", "GRPC addresses of sandglass cluster")

	flag.Parse()

	c, err := sg.NewClient(sg.WithAddresses(*addr))
	if err != nil {
		panic(err)
	}

	defer c.Close()

	topic := "payments"

	partitions, err := c.ListPartitions(context.Background(), topic)
	if err != nil {
		panic(err)
	}

	partition := partitions[0]

	producer := sg.NewAsyncProducer(c, topic, partition)
	msgs := make([]*sg.ProducerMessage, 0, 1e3)
	for i := 0; i < 1e3; i++ {
		msg := &sg.ProducerMessage{
			Value: randomBytes(1e4),
		}
		msgs = append(msgs, msg)
	}
	start := time.Now()
	producer.ProduceMessages(context.TODO(), msgs)

	fmt.Println("took", time.Since(start))
	start = time.Now()

	fmt.Println("consuming...")
	ctx := context.Background()
	consumer := c.NewConsumer(topic, partition, "group", "consumer1")

	consumeCh, err := consumer.Consume(ctx)
	if err != nil {
		panic(err)
	}

	n := 0
	var msg *sgproto.Message
	msgsToAck := []*sgproto.Message{}
	for msg = range consumeCh {
		msgsToAck = append(msgsToAck, msg)
		n++
		if len(msgsToAck) == 1000 {
			err = consumer.Acknowledge(context.Background(), msgsToAck...)
			if err != nil {
				panic(err)
			}

			msgsToAck = msgsToAck[:0]
		}
	}

	err = consumer.Acknowledge(context.Background(), msg)
	if err != nil {
		panic(err)
	}

	fmt.Println("took", time.Since(start), "for", n)
	if n > 0 {
		fmt.Println("each took", time.Duration(time.Since(start).Nanoseconds()/int64(n)))
	}
}

func randomBytes(n int) []byte {
	data := make([]byte, n)
	rand.Read(data)
	return data
}
