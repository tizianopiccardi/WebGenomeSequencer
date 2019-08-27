package main

import (
	"encoding/json"
	"fmt"
)

type Exception struct {
	Source          string
	ErrorType       string
	Message         string
	OriginalMessage string
}

type Logger struct {
	FileStartChannel chan SourceDestination
	FileEndChannel   chan SourceDestination
	Exceptions       chan Exception
}

func NewLogger() Logger {
	logger := Logger{}
	logger.FileStartChannel = make(chan SourceDestination, 100)
	logger.FileEndChannel = make(chan SourceDestination, 100)
	logger.Exceptions = make(chan Exception, 100) //NOT USED

	return logger
}

func (logger Logger) quit() {
	//close(logger.CounterChannel)
}

func (logger Logger) run() {

	for {
		//fmt.Println("RUNNING")
		select {
		case e := <-logger.Exceptions:
			jsonError, _ := json.Marshal(e)
			fmt.Println(string(jsonError))
		case fileStarted := <-logger.FileStartChannel:
			fmt.Println("New file started:", fileStarted.SourceFile)
		case fileEnd := <-logger.FileEndChannel:
			fmt.Println("File completed:", fileEnd.SourceFile)
		}
	}
}
