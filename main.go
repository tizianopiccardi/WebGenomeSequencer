package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

//import _ "net/http/pprof"

//const BASE_URL = "https://commoncrawl.s3.amazonaws.com/"

func main() {

	//go func() {
	//	log.Println(http.ListenAndServe(":6060", nil))
	//}()

	inputFile := os.Args[1]
	outputPath := os.Args[2]
	workersCount, _ := strconv.ParseInt(os.Args[3], 10, 32)

	lines, err := readLines(inputFile)
	if err != nil {
		log.Fatalf("readLines: %s", err)
	}
	err = os.MkdirAll(outputPath, os.ModePerm)
	if err != nil {
		log.Fatalf("Unable to create the output directory: %s", err)
	}

	start := time.Now()

	var workersWaitGroup sync.WaitGroup

	pathsChannel := make(chan SourceDestination, 150)

	logger, err := NewLogger("errors.log", "completed.txt")
	if err != nil {
		fmt.Println("Error in creating the log file...")
		return
	}
	go logger.run()

	for w := 1; w <= int(workersCount); w++ {
		workersWaitGroup.Add(1)
		go LinkExtractionWorker(pathsChannel, &workersWaitGroup, logger)
	}

	for _, line := range lines {
		sourceWarc := line
		//fmt.Println(sourceWarc)
		file := filepath.Base(sourceWarc)
		pathsChannel <- SourceDestination{SourceFile: sourceWarc, DestinationFile: outputPath + "/" + file + ".parquet"}
	}

	close(pathsChannel)


	workersWaitGroup.Wait()
	logger.quit()

	fmt.Println("Job completed in:", time.Now().Sub(start))

}
