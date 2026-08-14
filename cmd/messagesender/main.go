package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"crawler/internal/config"
	"crawler/internal/crawler"
	"crawler/internal/pulsar"

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("설정 파일 로드 실패: %v\n", err)
	}

	cClient := crawler.NewClient(cfg)

	pClient, err := pulsar.NewClient(cfg)
	if err != nil {
		log.Fatalf("Pulsar 클라이언트 생성 실패: %v\n", err)
	}
	defer pClient.Close()

	userProd, _ := pClient.CreateProducer("user")
	profileProd, _ := pClient.CreateProducer("profile")
	contentProd, _ := pClient.CreateProducer("content")
	imageProd, _ := pClient.CreateProducer("image")

	defer userProd.Close()
	defer profileProd.Close()

	defer contentProd.Close()
	defer imageProd.Close()

	producers := map[string]pulsarClient.Producer{
		"user":    userProd,
		"profile": profileProd,
		"content": contentProd,
		"image":   imageProd,
	}

	fmt.Println("Input Topic and Messages to Publish (-1 to exit)")
	fmt.Println("Topics: user, profile, content, image")
	fmt.Println("Format : [topic] [N or T][Profile Name(/Number)] ...")
	fmt.Println("ex) user Nhello")
	fmt.Println("ex) profile TWorld/1233\n")

	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()

	for {
		fmt.Print("Input > ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if parts[0] == "-1" {
			break
		}

		topic := parts[0]
		prod, ok := producers[topic]
		if !ok {
			fmt.Println("Topic must be one of user, profile, content, image.")
			continue
		}

		messages := parts[1:]
		if len(messages) == 0 {
			fmt.Println("No Messages")
			continue
		}

		if topic == "image" {
			for _, m := range messages {
				prod.SendAsync(ctx, &pulsarClient.ProducerMessage{Payload: []byte(m)}, func(id pulsarClient.MessageID, msg *pulsarClient.ProducerMessage, err error) {})
			}
			fmt.Println("Sent image messages.")
		} else {
			linkType := "posts"
			if topic == "user" {
				linkType = "users"
			}

			for _, m := range messages {
				if !strings.HasPrefix(m, "N") && !strings.HasPrefix(m, "T") {
					fmt.Printf("%s: First Character must be N or T\n", m)
					continue
				}

				if cClient.CheckLinkNotVisited(m, linkType) {
					if topic == "user" {
						cClient.RegisterLink(m, "users")
					}
					prod.SendAsync(ctx, &pulsarClient.ProducerMessage{Payload: []byte(m)}, func(id pulsarClient.MessageID, msg *pulsarClient.ProducerMessage, err error) {})
					fmt.Printf("Sent: %s\n", m)
				} else {
					fmt.Printf("%s: Already visited or Connect Failed\n", m)
				}
			}
		}
	}
}
