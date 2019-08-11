package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type Link struct {
	Date     int64  `parquet:"name=date, type=INT64"`
	Source   string `parquet:"name=source, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Link     string `parquet:"name=link, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Fragment string `parquet:"name=fragment, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Tag      string `parquet:"name=tag, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Extras   string `parquet:"name=extras, type=UTF8, encoding=PLAIN_DICTIONARY"`
}


func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	inputFile := os.Args[1]
	readersCount,_ := strconv.ParseInt(os.Args[2], 10, 32)
	writersCount,_ := strconv.ParseInt(os.Args[3], 10, 32)
	maxLinkPerParquet,_ := strconv.ParseInt(os.Args[4], 10, 32)



	lines, err := readLines(inputFile)
	if err != nil {
		log.Fatalf("readLines: %s", err)
	}

	start := time.Now()

	var readersWaitGroup sync.WaitGroup
	var writersWaitGroup sync.WaitGroup

	warcPathsChannel := make(chan string, 150)
	writersChannel := make(chan *[]Link, 150)

	for w := 1; w <= int(readersCount); w++ {
		readersWaitGroup.Add(1)
		go readerWorker(warcPathsChannel, writersChannel, &readersWaitGroup )
	}

	for w := 1; w <= int(writersCount); w++ {
		writersWaitGroup.Add(1)
		go writerWorker(writersChannel, &writersWaitGroup, maxLinkPerParquet)
	}


	for _, line := range lines {
		//fmt.Println(i)
		warcPathsChannel <- line
	}

	close(warcPathsChannel)

	readersWaitGroup.Wait()

	close(writersChannel)
	writersWaitGroup.Wait()

	fmt.Println("Job completed in:", time.Now().Sub(start))


}