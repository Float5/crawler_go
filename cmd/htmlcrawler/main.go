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
	msg       pulsarClient.Message // ConsumerMessage -> Message로 수정
	link      string
	htmlBody  string
	blogType  string
	isSuccess bool
}

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

	consumer, err := pClient.CreateConsumer("content", cfg.CrawlerName+"_HTMLCrawler")
	if err != nil {
		log.Fatalf("Consumer 구독 실패: %v\n", err)
	}
	defer consumer.Close()

	fmt.Println("🚀 HTMLCrawler 시작됨. 메시지 대기 중...")

	ctx := context.Background()
	delayN := time.Duration(1000/cfg.CrawlPerSecondMap["HTMLCrawler_N"]) * time.Millisecond
	delayT := time.Duration(1000/cfg.CrawlPerSecondMap["HTMLCrawler_T"]) * time.Millisecond

	sem := make(chan struct{}, cfg.MaxConcurrentRequests)
	resultsChan := make(chan ProcessResult, cfg.MaxConcurrentRequests*2)

	bodies := make(map[string]string)
	var processedMsgs []pulsarClient.Message // ConsumerMessage -> Message로 수정

	go func() {
		for {
			start := time.Now()

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

			sem <- struct{}{}

			if strings.HasPrefix(link, "N") {
				time.Sleep(delayN)
			} else {
				time.Sleep(delayT)
			}

			// 매개변수 타입을 pulsarClient.Message로 수정
			go func(msg pulsarClient.Message, targetLink string) {
				defer func() { <-sem }()

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
					fmt.Printf("error: %s, code: %d\n", err.Error(), statusCode)
					fmt.Printf("delay 30s\n")
					resultsChan <- ProcessResult{msg: msg, link: targetLink, isSuccess: false}
					time.Sleep(120000 * time.Millisecond)
					return
				}

				resultsChan <- ProcessResult{
					msg:       msg,
					link:      targetLink,
					htmlBody:  string(bodyBytes),
					blogType:  blogType,
					isSuccess: true,
				}
			}(cm, link)

			elapsed := time.Since(start)
			fmt.Printf("실행 시간: %v\n", elapsed)
		}
	}()

	for res := range resultsChan {
		if !res.isSuccess {
			consumer.Nack(res.msg)
			time.Sleep(time.Duration(cfg.NaverTimeoutWaitingTime) * time.Millisecond)
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
					fmt.Printf("✅ %d 개 HTML 저장 완료\n", len(bodies))
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
