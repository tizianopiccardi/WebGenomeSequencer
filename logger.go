package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Exception struct {
	File            string
	Source          string
	ErrorType       string
	Message         string
	OriginalMessage string
}

type Logger struct {
	LogsFileName   string
	LogsFileWriter *os.File

	CompletedFiles       string
	CompletedFilesWriter *os.File

	FileStartChannel chan SourceDestination
	FileEndChannel   chan SourceDestination
	Exceptions       chan Exception
}

func NewLogger(fileName, completedFiles string) (Logger, error) {
	logger := Logger{LogsFileName: fileName, CompletedFiles: completedFiles}

	logFile, err1 := os.Create(fileName)
	cFile, err2 := os.Create(completedFiles)

	if err1 != nil {
		return logger, err1
	}
	if err2 != nil {
		return logger, err2
	}

	logger.LogsFileWriter = logFile
	logger.CompletedFilesWriter = cFile

	logger.FileStartChannel = make(chan SourceDestination, 100)
	logger.FileEndChannel = make(chan SourceDestination, 100)
	logger.Exceptions = make(chan Exception, 100)

	return logger, nil
}

func (logger Logger) quit() {
	//
	//close(logger.Exceptions)
	//close(logger.FileEndChannel)
	//close(logger.FileStartChannel)

	//logger.LogsFileWriter.Flush()
	logger.LogsFileWriter.Close()
	logger.CompletedFilesWriter.Close()
}

func (logger Logger) run() {

	for {
		//fmt.Println("RUNNING")
		select {
		case e := <-logger.Exceptions:
			jsonError, _ := json.Marshal(e)
			//fmt.Println(string(jsonError))
			logger.LogsFileWriter.Write(jsonError)
			logger.LogsFileWriter.Write([]byte("\n"))
		case fileStarted := <-logger.FileStartChannel:
			fmt.Println("New file started:", fileStarted.SourceFile)
		case fileEnd := <-logger.FileEndChannel:
			fmt.Println("File completed:", fileEnd.SourceFile)
			logger.CompletedFilesWriter.Write([]byte(fileEnd.SourceFile))
			logger.CompletedFilesWriter.Write([]byte("\n"))
		}
	}
}
