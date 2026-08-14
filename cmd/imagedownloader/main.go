package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/pulsar"
)

func getExtension(contentType string) string {
	mimeMap := map[string]string{
		"image/jpeg":   ".jpg",
		"image/png":    ".png",
		"image/gif":    ".gif",
		"image/webp":   ".webp",
		"image/x-icon": ".ico",
	}

	for mime, ext := range mimeMap {
		if strings.Contains(contentType, mime) {
			return ext
		}
	}
	return ".bin"
}

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

	consumer, err := pClient.CreateConsumer("image", cfg.CrawlerName+"_ImageDownloader")
	if err != nil {
		log.Fatalf("Consumer 구독 실패: %v\n", err)
	}
	defer consumer.Close()

	folderName := "DownloadedImages"
	if err := os.MkdirAll(folderName, os.ModePerm); err != nil {
		log.Fatalf("폴더 생성 실패: %v\n", err)
	}

	fmt.Println("🚀 ImageDownloader 시작됨. 메시지 대기 중...")

	ctx := context.Background()
	counter := 0

	httpClient := &http.Client{Timeout: 15 * time.Second}

	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		imgURL := string(msg.Payload())
		if imgURL == "" {
			consumer.Ack(msg)
			continue
		}

		tempFileName := filepath.Join(folderName, "tmp")
		out, err := os.Create(tempFileName)
		if err != nil {
			consumer.Nack(msg)
			continue
		}

		resp, err := httpClient.Get(imgURL)
		if err != nil || resp.StatusCode >= 400 {
			out.Close()
			os.Remove(tempFileName)
			consumer.Nack(msg)
			continue
		}

		_, err = io.Copy(out, resp.Body)
		resp.Body.Close()
		out.Close()

		if err != nil {
			os.Remove(tempFileName)
			consumer.Nack(msg)
			continue
		}

		ct := resp.Header.Get("Content-Type")
		ext := getExtension(ct)
		if ext == ".bin" {
			os.Remove(tempFileName)
			consumer.Ack(msg)
			continue
		}

		counter++
		finalFileName := filepath.Join(folderName, fmt.Sprintf("image%d%s", counter, ext))

		if err := os.Rename(tempFileName, finalFileName); err != nil {
			os.Remove(tempFileName)
			consumer.Nack(msg)
			continue
		}

		if cfg.Verbose {
			fmt.Printf("✅ 다운로드 성공: %s (%s)\n", finalFileName, ct)
		}

		consumer.Ack(msg)
	}
}
