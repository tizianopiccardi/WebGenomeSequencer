package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"
	"time"
)

import _ "net/http/pprof"


func enableDebugTools() {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	f, err := os.Create("trace.out")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	err = trace.Start(f)
	if err != nil {
		panic(err)
	}
	defer trace.Stop()
}


func main() {



	urlPrefix := flag.String("urlPrefix", "", "Prefix for WARC URLs")
	workersCount := flag.Int("workersCount", 1, "Number of workers")
	tempDirectory := flag.String("tempDirectory", "/tmp", "Cache to download the warc files")

	enableDebug := flag.Bool("debug", false, "Enable HTTP profile (port 6060) and trace")

	flag.Parse()

	if len(flag.Args()) < 3 {
		fmt.Println("Missing parameters...", flag.Args())
		fmt.Println("Format: ./Sequencer [-urlPrefix prefix] [-workersCount 1] [-tempDirectory /tmp] <input_file> <output_path> <data_origin_name>")
		os.Exit(-1)
	}

	inputFile := flag.Args()[0]
	outputPath := flag.Args()[1]
	dataOrigin := flag.Args()[2]

	fmt.Println("inputFile =", inputFile)
	fmt.Println("tempDirectory =", tempDirectory)
	fmt.Println("urlPrefix =", *urlPrefix)
	fmt.Println("outputPath =", outputPath)
	fmt.Println("workersCount =", *workersCount)
	fmt.Println("dataOrigin =", dataOrigin)

	fmt.Println("enableDebug =", *enableDebug)

	if *enableDebug {
		enableDebugTools()
		fmt.Println("Debug tools started")
	}

	lines, err := readLines(inputFile)
	if err != nil {
		log.Fatalf("readLines: %s", err)
	}

	// Create output path
	err = os.MkdirAll(outputPath, os.ModePerm)
	if err != nil {
		log.Fatalf("Unable to create the output directory: %s", err)
	}

	// Create download cache
	err = os.MkdirAll(*tempDirectory, os.ModePerm)
	if err != nil {
		log.Fatalf("Unable to create the cache directory: %s", err)
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

	for w := 1; w <= *workersCount; w++ {
		//fmt.Println(dataOrigin, pathsChannel, &workersWaitGroup, logger)
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
