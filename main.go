package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

import _ "net/http/pprof"

const BASE_URL = "https://commoncrawl.s3.amazonaws.com/"

func main() {

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	inputFile := os.Args[1]
	workersCount, _ := strconv.ParseInt(os.Args[2], 10, 32)

	lines, err := readLines(inputFile)
	if err != nil {
		log.Fatalf("readLines: %s", err)
	}

	start := time.Now()

	var workersWaitGroup sync.WaitGroup

	pathsChannel := make(chan SourceDestination, 150)

	logger := NewLogger()
	go logger.run()

	for w := 1; w <= int(workersCount); w++ {
		workersWaitGroup.Add(1)
		go LinkExtractionWorker(pathsChannel, &workersWaitGroup, logger)
	}

	for _, line := range lines {
		sourceWarc := BASE_URL + line
		file := filepath.Base(sourceWarc)
		pathsChannel <- SourceDestination{SourceFile: sourceWarc, DestinationFile: "links/" + file + ".parquet"}
	}

	close(pathsChannel)

	workersWaitGroup.Wait()
	logger.quit()

	fmt.Println("Job completed in:", time.Now().Sub(start))

}
