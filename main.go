package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"crawler/internal/config"
	"crawler/internal/pulsar"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("설정 파일 로드 실패: %v\n", err)
	}

	pClient, err := pulsar.NewClient(cfg)
	if err != nil {
		log.Fatalf("Pulsar 클라이언트 생성 실패: %v\n", err)
	}
	defer pClient.Close()

	consumer, err := pClient.CreateConsumer("user", cfg.CrawlerName+"_LinkFinder")
	if err != nil {
		log.Fatalf("Consumer 구독 실패: %v\n", err)
	}
	defer consumer.Close()

	ctx := context.Background()

	for {
		msg, err := consumer.Receive(ctx)

		time.Sleep(1000 * time.Millisecond)

		if err != nil {
			fmt.Printf("%s\n", err.Error())
		} else {
			fmt.Printf("%s\n", string(msg.Payload()))
			if err := consumer.AckID(msg.ID()); err != nil {
				log.Printf("Ack 실패: %v (msgID: %v)\n", err, msg.ID())
			}
		}
	}
}
