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
		log.Fatalf("Failed to load config file: %v\n", err)
	}

	cClient := crawler.NewClient(cfg)

	pClient, err := pulsar.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Pulsar client: %v\n", err)
	}
	defer pClient.Close()

	profileProducerNaver, _ := pClient.CreateProducer("profile-naver")
	profileProducerTistory, _ := pClient.CreateProducer("profile-tistory")
	contentProducerNaver, _ := pClient.CreateProducer("content-naver")
	contentProducerTistory, _ := pClient.CreateProducer("content-tistory")

	defer profileProducerNaver.Close()
	defer profileProducerTistory.Close()
	defer contentProducerNaver.Close()
	defer contentProducerTistory.Close()

	consumerNaver, err := pClient.CreateConsumer("user-naver", cfg.CrawlerName+"_LinkFinder_N")
	if err != nil {
		log.Fatalf("Failed to subscribe Naver Consumer: %v\n", err)
	}
	defer consumerNaver.Close()

	consumerTistory, err := pClient.CreateConsumer("user-tistory", cfg.CrawlerName+"_LinkFinder_T")
	if err != nil {
		log.Fatalf("Failed to subscribe Tistory Consumer: %v\n", err)
	}
	defer consumerTistory.Close()

	fmt.Println("LinkFinder Started. Waiting for messages...")

	ctx := context.Background()
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		runNaverLoop(ctx, cfg, cClient, consumerNaver, profileProducerNaver, contentProducerNaver)
	}()
	go func() {
		defer wg.Done()
		runTistoryLoop(ctx, cfg, cClient, consumerTistory, profileProducerTistory, contentProducerTistory)
	}()

	wg.Wait()
}

func runNaverLoop(ctx context.Context, cfg *config.Config, cClient *crawler.Client, consumer pulsarClient.Consumer, profileProducer, contentProducer pulsarClient.Producer) {
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		link := string(msg.Payload())
		if link == "" || !strings.HasPrefix(link, "N") {
			consumer.Ack(msg)
			continue
		}

		validPages, ack := processNaverBlog(cfg, cClient, link[1:])

		if ack {
			fmt.Printf("[Naver] %d pages found in %s\n", len(validPages), link)
			sendMessages(ctx, profileProducer, validPages)
			sendMessages(ctx, contentProducer, validPages)
			consumer.Ack(msg)
		} else {
			consumer.Nack(msg)
		}
	}
}

func runTistoryLoop(ctx context.Context, cfg *config.Config, cClient *crawler.Client, consumer pulsarClient.Consumer, profileProducer, contentProducer pulsarClient.Producer) {
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		link := string(msg.Payload())
		if link == "" || !strings.HasPrefix(link, "T") {
			consumer.Ack(msg)
			continue
		}

		validPages, ack := processTistoryBlog(cfg, cClient, link[1:])

		if ack {
			fmt.Printf("[Tistory] %d pages found in %s\n", len(validPages), link)
			sendMessages(ctx, profileProducer, validPages)
			sendMessages(ctx, contentProducer, validPages)
			consumer.Ack(msg)
		} else {
			consumer.Nack(msg)
		}
	}
}

func processNaverBlog(cfg *config.Config, client *crawler.Client, blogName string) ([]string, bool) {
	fmt.Printf("[Naver] Start to process \"N%s\"\n", blogName)

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

			if len(validPages)%300 == 0 {
				fmt.Printf("[Naver] %s: %d pages found so far\n", blogName, len(validPages))
			}
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
	fmt.Printf("[Tistory] Start to process \"T%s\"\n", blogName)

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
			_, code, err := client.DoRequest("GET", postURL, headers, nil)
			if err != nil || code >= 400 {
				emptyPageCnt++
				return
			}

			mu.Lock()
			defer mu.Unlock()

			emptyPageCnt = 0
			validPages = append(validPages, fmt.Sprintf("T%s/%d", blogName, idx))
			if len(validPages)%100 == 0 {
				fmt.Printf("[Tistory] %s: %d pages found so far\n", blogName, len(validPages))
			}
		}(currentIndex)
	}

	return validPages, true
}

func sendMessages(ctx context.Context, producer pulsarClient.Producer, messages []string) {
	for _, msgStr := range messages {
		producer.SendAsync(ctx, &pulsarClient.ProducerMessage{
			Payload: []byte(msgStr),
		}, func(id pulsarClient.MessageID, msg *pulsarClient.ProducerMessage, err error) {})
	}
}
