package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

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

	profileProducer, _ := pClient.CreateProducer("profile")
	defer profileProducer.Close()

	contentProducer, _ := pClient.CreateProducer("content")
	defer contentProducer.Close()

	consumer, err := pClient.CreateConsumer("user", cfg.CrawlerName+"_LinkFinder")
	if err != nil {
		log.Fatalf("Consumer 구독 실패: %v\n", err)
	}
	defer consumer.Close()

	fmt.Println("🚀 LinkFinder 시작됨. 메시지 대기 중...")

	ctx := context.Background()

	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		link := string(msg.Payload())
		if link == "" {
			consumer.Ack(msg)
			continue
		}

		var ack bool
		var validPages []string

		if strings.HasPrefix(link, "N") {
			validPages, ack = processNaverBlog(cfg, cClient, link[1:])
		} else if strings.HasPrefix(link, "T") {
			validPages, ack = processTistoryBlog(cfg, cClient, link[1:])
		} else {
			ack = true
		}

		if ack {
			sendBatchMessages(ctx, profileProducer, validPages)
			sendBatchMessages(ctx, contentProducer, validPages)
			consumer.Ack(msg)
		} else {
			consumer.Nack(msg)
		}
	}
}

func processNaverBlog(cfg *config.Config, client *crawler.Client, blogName string) ([]string, bool) {
	var validPages []string
	foundPostIds := make(map[string]bool)
	currentPage := 1
	delay := time.Duration(1000/cfg.CrawlPerSecondMap["LinkFinder_N"]) * time.Millisecond

	for {
		targetURL := fmt.Sprintf("https://blog.naver.com/PostTitleListAsync.naver?blogId=%s&currentPage=%d&countPerPage=30", blogName, currentPage)
		headers := map[string]string{"Referer": "https://blog.naver.com/" + blogName}

		if !client.IsAllowedByRobots(targetURL) {
			return validPages, true
		}

		body, statusCode, err := client.DoRequest("GET", targetURL, headers, nil)
		if err != nil || statusCode >= 400 {
			return validPages, false
		}

		htmlStr := string(body)
		if strings.Contains(htmlStr, `"resultCode":"E"`) {
			return validPages, true
		}

		re := regexp.MustCompile(`logNo=([0-9]+)`)
		matches := re.FindAllStringSubmatch(htmlStr, -1)

		if len(matches) == 0 {
			break
		}

		duplicateFound := false
		pagesInCall := 0

		for _, match := range matches {
			postID := match[1]
			if foundPostIds[postID] {
				duplicateFound = true
				break
			}
			foundPostIds[postID] = true
			validPages = append(validPages, fmt.Sprintf("N%s/%s", blogName, postID))
			pagesInCall++
		}

		if duplicateFound || pagesInCall == 0 {
			break
		}

		currentPage++
		time.Sleep(delay)
	}

	return validPages, true
}

func processTistoryBlog(cfg *config.Config, client *crawler.Client, blogName string) ([]string, bool) {
	rssURL := fmt.Sprintf("https://%s.tistory.com/rss", blogName)
	if !client.IsAllowedByRobots(rssURL) {
		return nil, true
	}

	body, statusCode, err := client.DoRequest("GET", rssURL, nil, nil)
	if err != nil || statusCode >= 400 {
		return nil, false
	}

	re := regexp.MustCompile(`<link>https://[^/]+/([0-9]+)</link>`)
	match := re.FindStringSubmatch(string(body))
	if len(match) < 2 {
		return nil, true
	}

	maxIndex, _ := strconv.Atoi(match[1])
	if maxIndex == 0 {
		return nil, true
	}

	var validPages []string
	var mu sync.Mutex
	emptyPageCnt := 0
	delay := time.Duration(1000/cfg.CrawlPerSecondMap["LinkFinder_T"]) * time.Millisecond

	sem := make(chan struct{}, cfg.MaxConcurrentRequests)

	for currentIndex := maxIndex; currentIndex > 0; currentIndex-- {
		mu.Lock()
		if emptyPageCnt >= 20 {
			mu.Unlock()
			break
		}
		mu.Unlock()

		sem <- struct{}{}
		time.Sleep(delay)

		go func(idx int) {
			defer func() { <-sem }()

			postURL := fmt.Sprintf("https://%s.tistory.com/%d", blogName, idx)
			if !client.IsAllowedByRobots(postURL) {
				return
			}

			headers := map[string]string{"Range": "bytes=0-256"}
			respBytes, code, err := client.DoRequest("GET", postURL, headers, nil)
			if err != nil || code >= 400 {
				return
			}

			respStr := string(respBytes)
			titleTagOpen := strings.Index(respStr, "<title>")
			titleTagClose := strings.Index(respStr, "</title>")

			var htmlTitle string
			if titleTagOpen != -1 && titleTagClose != -1 && titleTagClose > titleTagOpen {
				htmlTitle = respStr[titleTagOpen+7 : titleTagClose]
			}

			mu.Lock()
			defer mu.Unlock()

			if htmlTitle != "TISTORY" {
				emptyPageCnt = 0
				validPages = append(validPages, fmt.Sprintf("T%s/%d", blogName, idx))
			} else {
				emptyPageCnt++
			}
		}(currentIndex)
	}

	return validPages, true
}

func sendBatchMessages(ctx context.Context, producer pulsarClient.Producer, messages []string) {
	for _, msgStr := range messages {
		producer.SendAsync(ctx, &pulsarClient.ProducerMessage{
			Payload: []byte(msgStr),
		}, func(id pulsarClient.MessageID, msg *pulsarClient.ProducerMessage, err error) {})
	}
}
