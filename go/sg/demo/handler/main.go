package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/sandglass/sandglass-client/go/sg"
	"github.com/sandglass/sandglass-grpc/go/sgproto"
)

func main() {
	addr := flag.String("addrs", ":7170", "GRPC addresses of sandglass cluster")
	sleepDur := flag.String("sleep", "50ms", "sleep time")

	flag.Parse()

	dur, err := time.ParseDuration(*sleepDur)
	if err != nil {
		panic(err)
	}

	c, err := sg.NewClient(sg.WithAddresses(*addr))
	if err != nil {
		panic(err)
	}

	defer c.Close()

	mux := sg.NewMux()
	mux.SubscribeFunc("emails", func(msg *sgproto.Message) error {
		fmt.Println(string(msg.Value))
		return nil
	})

	m := &sg.MuxManager{
		Client:               c,
		Mux:                  mux,
		ReFetchSleepDuration: dur,
	}

	err = m.Start()
	if err != nil {
		log.Fatal(err)
	}
}
