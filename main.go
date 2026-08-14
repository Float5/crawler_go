package main

import (
	"fmt"
	"log"

	"crawler/internal/config"
	"crawler/internal/crawler"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("설정 파일 로드 실패: %v\n", err)
	}

	cClient := crawler.NewClient(cfg)

	if cClient.CheckLinkNotVisited("Nhaesung_88", "posts") {
		fmt.Printf("true")
	} else {
		fmt.Printf("false")
	}
}
