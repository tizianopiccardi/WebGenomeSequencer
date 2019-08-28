package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"strings"
)

func readLines(path string) ([]string, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := io.Reader(file)
	if strings.HasSuffix(path, "gz") {
		gz, err := gzip.NewReader(file)
		if err == nil {
			reader = gz
		}
	}

	var lines []string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}
