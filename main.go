package main

import (
	"flag"
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



	urlPrefix := flag.String("urlPrefix", "", "Prefix for WARC URLs")
	flag.Parse()

	if len(flag.Args()) < 4 {
		fmt.Println("Missing parameters...", flag.Args())
		fmt.Println("Format: ./Sequencer [-urlPrefix prefix] <input_file> <output_path> <workers_count> <data_origin_name>")
		os.Exit(-1)
	}

	fmt.Println("urlPrefix =", *urlPrefix)

	inputFile := flag.Args()[0]
	fmt.Println("inputFile =", inputFile)

	outputPath := flag.Args()[1]
	fmt.Println("outputPath =", outputPath)

	workersCount, _ := strconv.ParseInt(flag.Args()[2], 10, 32)
	fmt.Println("workersCount =", workersCount)

	dataOrigin := flag.Args()[3]
	fmt.Println("dataOrigin =", dataOrigin)

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
		fmt.Println(dataOrigin, pathsChannel, &workersWaitGroup, logger)
		workersWaitGroup.Add(1)
		go LinkExtractionWorker(dataOrigin, pathsChannel, &workersWaitGroup, logger)
	}

	for _, line := range lines {
		sourceWarc := *urlPrefix + line
		//fmt.Println(sourceWarc)
		file := filepath.Base(sourceWarc)
		pathsChannel <- SourceDestination{SourceFile: sourceWarc, DestinationFile: outputPath + "/" + file + ".parquet"}
	}

	close(pathsChannel)


	workersWaitGroup.Wait()
	logger.quit()

	fmt.Println("Job completed in:", time.Now().Sub(start))

}
