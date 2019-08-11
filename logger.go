package main

import (
	"fmt"
	"time"
)

type WriteRequest struct {
	writerID   uint64
	linksCount int64
}

type Logger struct {
	CounterChannel   chan WriteRequest
	FileStartChannel chan string
	FileEndChannel   chan string
	Errors           chan string
}

func NewLogger() Logger {
	logger := Logger{}
	logger.CounterChannel = make(chan WriteRequest, 100)
	logger.FileStartChannel = make(chan string, 100)
	logger.FileEndChannel = make(chan string, 100)
	logger.Errors = make(chan string, 100) //NOT USED

	return logger
}

func (logger Logger) quit() {
	close(logger.CounterChannel)
}

func (logger Logger) run() {
	var total int64
	start := time.Now()
	for {
		//fmt.Println("RUNNING")
		select {
		case wr := <-logger.CounterChannel:
			total += wr.linksCount
			fmt.Println("New write request for writer #", wr.writerID, ":", total, "links in", time.Now().Sub(start))
		case fileStarted := <-logger.FileStartChannel:
			fmt.Println("New file started:", fileStarted)
		case fileEnd := <-logger.FileEndChannel:
			fmt.Println("File completed:", fileEnd)
		}
	}
}
