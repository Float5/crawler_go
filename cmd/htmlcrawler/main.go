package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/crawler"
	"crawler/internal/pulsar"

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"
)

type ProcessResult struct {
	msg       pulsarClient.Message
	link      string
	htmlBody  string
	blogType  string
	isSuccess bool
}

type job struct {
	msg  pulsarClient.Message
	link string
}

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config file: %v\n", err)
	}

	cClient := crawler.NewClient(cfg)

	pClient, err := pulsar.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Pulsar client: %v\n", err)
	}
	defer pClient.Close()

	consumer, err := pClient.CreateConsumer("content", cfg.CrawlerName+"_HTMLCrawler")
	if err != nil {
		log.Fatalf("Failed to subscribe Consumer: %v\n", err)
	}
	defer consumer.Close()

	fmt.Println("HTMLCrawler Started. Waiting for messages...")

	ctx := context.Background()

	naverWorkers := cfg.CrawlPerSecondMap["HTMLCrawler_N"]
	tistoryWorkers := cfg.CrawlPerSecondMap["HTMLCrawler_T"]

	naverQueue := make(chan job, naverWorkers*2)
	tistoryQueue := make(chan job, tistoryWorkers*2)

	resultsChan := make(chan ProcessResult, (naverWorkers+tistoryWorkers)*2)

	processLink := func(msg pulsarClient.Message, targetLink string) {
		slashIdx := strings.Index(targetLink, "/")
		if slashIdx == -1 {
			consumer.Ack(msg)
			return
		}

		profileName := targetLink[1:slashIdx]
		writingNumber := targetLink[slashIdx+1:]

		var reqURL string
		var blogType string
		switch targetLink[0] {
		case 'N':
			reqURL = fmt.Sprintf("https://blog.naver.com/PostView.nhn?blogId=%s&logNo=%s", profileName, writingNumber)
			blogType = "naver"
		case 'T':
			reqURL = fmt.Sprintf("https://%s.tistory.com/m/%s", profileName, writingNumber)
			blogType = "tistory"
		default:
			consumer.Ack(msg)
			return
		}

		if !cClient.IsAllowedByRobots(reqURL) {
			consumer.Ack(msg)
			return
		}

		bodyBytes, statusCode, err := cClient.DoRequest("GET", reqURL, nil, nil)
		if err != nil || statusCode != 200 {
			fmt.Printf("error: %v, code: %d, link: %s\n", err, statusCode, targetLink)
			resultsChan <- ProcessResult{msg: msg, link: targetLink, isSuccess: false}
			return
		}

		resultsChan <- ProcessResult{
			msg:       msg,
			link:      targetLink,
			htmlBody:  string(bodyBytes),
			blogType:  blogType,
			isSuccess: true,
		}
	}

	startWorkers := func(queue chan job, count int) {
		for i := 0; i < count; i++ {
			go func() {
				for j := range queue {
					processLink(j.msg, j.link)
					time.Sleep(1 * time.Second)
				}
			}()
		}
	}
	startWorkers(naverQueue, naverWorkers)
	startWorkers(tistoryQueue, tistoryWorkers)

	go func() {
		for {
			cm, err := consumer.Receive(ctx)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			link := string(cm.Payload())
			if link == "" {
				consumer.Ack(cm)
				continue
			}

			dbCheckLink := strings.Replace(link, "/", "%20", 1)
			if !cClient.CheckLinkNotVisited(dbCheckLink, "posts") {
				consumer.Ack(cm)
				continue
			}

			if strings.HasPrefix(link, "N") {
				naverQueue <- job{msg: cm, link: link}
			} else if strings.HasPrefix(link, "T") {
				tistoryQueue <- job{msg: cm, link: link}
			} else {
				consumer.Ack(cm)
			}
		}
	}()

	bodies := make(map[string]string)
	var processedMsgs []pulsarClient.Message

	for res := range resultsChan {
		if !res.isSuccess {
			consumer.Nack(res.msg)
			continue
		}

		dbCheckLink := strings.Replace(res.link, "/", "%20", 1)
		htmlStorageLink := strings.Replace(res.link, "/", " ", 1)

		if cClient.CheckLinkNotVisited(dbCheckLink, "posts") {
			fmt.Printf("%s Crawled\n", res.link)

			bodyMap := map[string]interface{}{
				"body":      res.htmlBody,
				"blog":      res.blogType,
				"timestamp": time.Now().Unix(),
			}
			bodyJSON, _ := json.Marshal(bodyMap)

			bodies[htmlStorageLink[1:]] = string(bodyJSON)
			processedMsgs = append(processedMsgs, res.msg)
		} else {
			consumer.Ack(res.msg)
		}

		if len(bodies) >= cfg.BodiesThreshold {
			if err := cClient.PostHTMLContent(bodies); err == nil {
				for _, msg := range processedMsgs {
					consumer.Ack(msg)
				}
				if cfg.Verbose {
					fmt.Printf("%d HTML 저장 완료\n", len(bodies))
				}
			} else {
				for _, msg := range processedMsgs {
					consumer.Nack(msg)
				}
			}
			bodies = make(map[string]string)
			processedMsgs = nil
		}
	}
}
