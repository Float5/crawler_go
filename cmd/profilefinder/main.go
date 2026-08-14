package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
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

	userProducer, err := pClient.CreateProducer("user")
	if err != nil {
		log.Fatalf("Failed to create User Producer: %v\n", err)
	}
	defer userProducer.Close()

	consumer, err := pClient.CreateConsumer("profile", cfg.CrawlerName+"_ProfileFinder")
	if err != nil {
		log.Fatalf("Failed to subscribe Consumer: %v\n", err)
	}
	defer consumer.Close()

	fmt.Println("ProfileFinder Started. Waiting for messages...")

	ctx := context.Background()

	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			fmt.Printf("%s\n", err.Error())
			time.Sleep(100 * time.Millisecond)
			continue
		}

		payload := string(msg.Payload())
		if len(payload) < 2 {
			consumer.Ack(msg)
			continue
		}

		slashIdx := strings.Index(payload, "/")
		if slashIdx == -1 {
			consumer.Ack(msg)
			continue
		}

		profileName := payload[1:slashIdx]
		writingNumber := payload[slashIdx+1:]

		var userIDs []string
		var processErr error

		if strings.HasPrefix(payload, "N") {
			userIDs, processErr = handleNaverSympathy(cfg, cClient, profileName, writingNumber)
		} else if strings.HasPrefix(payload, "T") {
			userIDs, processErr = handleTistoryComment(cfg, cClient, profileName, writingNumber)
		} else {
			consumer.Ack(msg)
			continue
		}

		if processErr != nil {
			consumer.Nack(msg)
			continue
		}

		fmt.Printf("%d Profiles found in %s\n\n", len(userIDs), payload)
		for _, userID := range userIDs {
			userProducer.SendAsync(ctx, &pulsarClient.ProducerMessage{
				Payload: []byte(userID),
			}, func(id pulsarClient.MessageID, msg *pulsarClient.ProducerMessage, err error) {})
		}

		consumer.Ack(msg)
	}
}

func handleNaverSympathy(cfg *config.Config, client *crawler.Client, profileName, writingNumber string) ([]string, error) {
	fmt.Printf("Start to process \"N%s/%s\"\n", profileName, writingNumber)

	targetURL := fmt.Sprintf("https://blog.naver.com/api/blogs/%s/posts/%s/sympathy-users?itemCount=100&timeStamp=9999999999999", profileName, writingNumber)
	referer := fmt.Sprintf("https://blog.naver.com/SympathyHistoryList.naver?blogId=%s&logNo=%s", profileName, writingNumber)

	if !client.IsAllowedByRobots(targetURL) {
		return nil, nil
	}

	client.Delay(1000/cfg.CrawlPerSecondMap["ProfileFinder_N"], "ProfileFinder_N")

	headers := map[string]string{"Referer": referer}
	body, statusCode, err := client.DoRequest("GET", targetURL, headers, nil)
	if err != nil || statusCode >= 400 {
		return nil, fmt.Errorf("Request Failed (Status Code: %d): %v", statusCode, err)
	}

	re := regexp.MustCompile(`"domainIdOrBlogId":"([^"]+)"`)
	matches := re.FindAllStringSubmatch(string(body), -1)

	userIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		userID := "N" + match[1]
		if client.CheckLinkNotVisited(userID, "users") {
			if client.RegisterLink(userID, "users") {
				userIDs = append(userIDs, userID)
			}
		}
	}

	return userIDs, nil
}

func handleTistoryComment(cfg *config.Config, client *crawler.Client, profileName, writingNumber string) ([]string, error) {
	fmt.Printf("Start to process \"T%s/%s\"\n", profileName, writingNumber)

	targetURL := fmt.Sprintf("https://%s.tistory.com/m/api/%s/comment", profileName, writingNumber)

	if !client.IsAllowedByRobots(targetURL) {
		return nil, nil
	}

	client.Delay(1000/cfg.CrawlPerSecondMap["ProfileFinder_T"], "ProfileFinder_T")

	body, statusCode, err := client.DoRequest("GET", targetURL, nil, nil)
	if err != nil || statusCode >= 400 {
		return nil, fmt.Errorf("Request Failed (Status Code: %d): %v", statusCode, err)
	}

	re := regexp.MustCompile(`"homepage"\s*:\s*"https://([^"/]+)`)
	matches := re.FindAllStringSubmatch(string(body), -1)

	userIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		fullHost := match[1]
		if !strings.Contains(fullHost, ".tistory.com") {
			continue
		}

		subDomain := strings.Split(fullHost, ".")[0]
		userID := "T" + subDomain

		if client.CheckLinkNotVisited(userID, "users") {
			if client.RegisterLink(userID, "users") {
				userIDs = append(userIDs, userID)
			}
		}
	}

	return userIDs, nil
}
