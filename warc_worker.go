package main

import (
	"bufio"
	"fmt"
	"github.com/PuerkitoBio/purell"
	"github.com/slyrz/warc"
	"github.com/tevino/abool"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/net/html"
	"golang.org/x/net/html/charset"
	"io"
	"net/url"
	"os"
	"strings"
	"time"
)
const CHUNK_SIZE = 500000

const PURELL_FLAGS = purell.FlagsUsuallySafeGreedy |
	purell.FlagForceHTTP |
	purell.FlagRemoveFragment |
	purell.FlagSortQuery

func getAbsoluteNormalized(pageUrl *url.URL, href string) (string, string) {
	hrefUrl, err := url.Parse(href)
	var fragment string
	if err == nil {
		fragment = hrefUrl.Fragment
		hrefUrl = pageUrl.ResolveReference(hrefUrl)
		return purell.NormalizeURL(hrefUrl, PURELL_FLAGS), fragment
	}
	return "", fragment
}



func getCharsetReader(reader *bufio.Reader, contentType string) io.Reader {
	bodySample, _ := reader.Peek(1024)
	encoding, _, _ := charset.DetermineEncoding(bodySample, contentType)
	return encoding.NewDecoder().Reader(reader)
}

func LinkExtractionWorker(inputWarcFile, outputParquet, dataOrigin string, logger Logger) {

	fileReader, err := os.Open(inputWarcFile)
	if err != nil {
		logger.Exceptions <- Exception{
			ErrorType:       "File not found",
			Message:         inputWarcFile,
			OriginalMessage: err.Error(),
		}
		panic(err)

	} else {

		recordsReader, err := warc.NewReader(fileReader)
		if err != nil {
			logger.Exceptions <- Exception{
				//File:            sourceFile,
				//Source:          exceptionsSource,
				ErrorType:       "WARC Reader failed",
				Message:         inputWarcFile,
				OriginalMessage: err.Error(),
			}
			panic(err)

		} else {




			// Channel to share the chucks to write
			writerChannel := make(chan *MarkersList, 150)

			// Synchronized boolean var to inform the reader if the writer failed
			failedWriterFlag := abool.New()

			// Get a message when the writer completed the job
			writerDone := make(chan bool)

			// - The writer runs waiting from links chunks from the channel
			// - If it fails, it sets the failedWriterFlag to TRUE and log the error
			// - The reader checks regularly the flag, if it's TRUE: break
			go WriteParquet(outputParquet, writerChannel, failedWriterFlag, writerDone, logger)

			ReadWarc(dataOrigin, recordsReader, writerChannel, failedWriterFlag, logger)

			// The reader ended, the file if completely processed and we can
			// inform the writer by closing the channel
			close(writerChannel)

			// Wait for the writer to complete
			<-writerDone

		}

		recordsReader.Close()
		fileReader.Close()

	}

}




func ReadWarc(dataOrigin string, recordsReader *warc.Reader, writersChannel chan *MarkersList,
	failedWriterFlag *abool.AtomicBool, logger Logger) {
	markersBuffer := MarkersList{}

	for {
		if markersBuffer.length >= CHUNK_SIZE {

			// If the writer is dead, stop the reader
			if failedWriterFlag.IsSet() {
				//LOG FAILED
				logger.Exceptions <- Exception{
					//File:      path,
					//Source:    exceptionsSource,
					ErrorType: "Reader controlled failure",
					Message:   "The writer failed and the reader is interrupting the job",
				}
				break
			}

			// Send the chunk and allocate a new list
			copied := markersBuffer.copy()
			writersChannel <- &copied
			markersBuffer = MarkersList{}
		}

		record, err := recordsReader.ReadRecord()
		if err != nil {
			if err != io.EOF {
				logger.Exceptions <- Exception{
					//File:            path,
					//Source:          exceptionsSource,
					ErrorType:       "Record malformed",
					Message:         "The reader failed to process the record",
					OriginalMessage: err.Error(),
				}
				panic(err)
			} else {
				break
			}
		} else {

			warcContentType := record.Header.Get("content-type")
			recordType := record.Header.Get("warc-type")

			if recordType == "response" && strings.HasPrefix(warcContentType, "application/http") {
				recordDate, err := time.Parse(time.RFC3339, record.Header.Get("warc-date"))
				if err != nil {
					logger.Exceptions <- Exception{
						ErrorType:       "Date parsing error",
						Message:         record.Header.Get("warc-date"),
						OriginalMessage: err.Error(),
					}
				} else {
					originalUrl := record.Header.Get("WARC-Target-URI")
					originalUrl = strings.Replace(strings.TrimSpace(originalUrl), "\n", "", -1)
					pageUrl, err := url.Parse(originalUrl)

					if err != nil {
						logger.Exceptions <- Exception{
							ErrorType:       "Not an URL",
							Message:         originalUrl,
							OriginalMessage: err.Error(),
						}
					} else {

						isSecure := false
						if strings.HasPrefix(originalUrl, "https") {
							isSecure = true
						}

						pageHostParts := strings.Split(pageUrl.Host, ".")

						for i := 0; i < len(pageHostParts)/2; i++ {
							j := len(pageHostParts) - i - 1
							pageHostParts[i], pageHostParts[j] = pageHostParts[j], pageHostParts[i]
						}

						invertedPageHost := strings.Join(pageHostParts, ".")

						normalizedPageUrl := purell.NormalizeURL(pageUrl, PURELL_FLAGS)

						reader := bufio.NewReader(record.Content)
						var httpStatusCode string
						var redirectLocation string
						var contentType string

						for {
							lineBytes, _, err := reader.ReadLine()

							if err == io.EOF {
								break
							}
							if len(lineBytes) < 1 {
								break
							}

							line := string(lineBytes)

							if strings.HasPrefix(line, "HTTP/") {
								if len(line) >= 12 {
									httpStatusCode = line[9:12]
								}
							}

							if strings.HasPrefix(line, "Location:") {
								if len(line) >= 10 {
									redirectLocation = line[10:]
								}
							}

							if strings.HasPrefix(line, "Content-Type:") {
								if len(line) >= 14 {
									contentType = line[14:]
								}
							}

						}

						extras := ""
						if httpStatusCode == "200" {

							if strings.HasPrefix(contentType, "text/html") {
								customReader := getCharsetReader(reader, contentType)
								pageLinks := getLinks(dataOrigin, recordDate.Unix(), pageUrl, &normalizedPageUrl, customReader, logger, isSecure, invertedPageHost)
								markersBuffer.appendList(pageLinks)
							}

						} else {

							if len(redirectLocation) > 0 {
								redirectLocation = strings.TrimSpace(redirectLocation)
								extras, _ = getAbsoluteNormalized(pageUrl, redirectLocation)
							}

						}

						// Add the marker to know that the crawler visited the page
						link := NewWebpageMarker(recordDate.Unix(), invertedPageHost, isSecure,
							normalizedPageUrl, httpStatusCode, extras, dataOrigin)
						markersBuffer.append(&link)

					}

				}
			}

		}
	}
	writersChannel <- &markersBuffer

}


func getLinks(dataOrigin string, crawlingTime int64, pageUrl *url.URL,
	normalizedPageUrl *string, body io.Reader, logger Logger,
	mainPageSecure bool, invertedPageHost string) *MarkersList {

	//Links in the current page
	pageLinks := MarkersList{}

	//Initialise tokenizer
	tokenizer := html.NewTokenizer(body)

	for {
		//get the next token type
		tokenType := tokenizer.Next()

		if tokenType == html.StartTagToken || tokenType == html.SelfClosingTagToken {
			//Get token info
			token := tokenizer.Token()
			// Tag a
			if "a" == token.Data {
				var hrefValue string
				for _, attr := range token.Attr {
					if attr.Key == "href" {
						hrefValue = strings.Replace(strings.TrimSpace(attr.Val), "\n", "", -1)
						hrefValue = strings.Replace(hrefValue, "\t", "", -1)
						hrefValue = strings.Replace(hrefValue, "\r", "", -1)
						hrefValue = strings.Replace(hrefValue, "\u0008", "", -1)
						break
					}
				}
				//fmt.Println(hrefValue)
				if !strings.HasPrefix(hrefValue, "javascript:") &&
					!strings.HasPrefix(hrefValue, "#") {

					isSecure := mainPageSecure
					if strings.HasPrefix(hrefValue, "https:") {
						isSecure = true
					}
					normalizedHrefValue, fragment := getAbsoluteNormalized(pageUrl, hrefValue)
					//fmt.Println(normalizedHrefValue)
					if len(normalizedHrefValue) > 0 {

						var extras strings.Builder
						for {
							//get the next token type
							tokenType = tokenizer.Next()
							token = tokenizer.Token()
							if tokenType == html.TextToken {
								extras.WriteString(strings.TrimSpace(html.UnescapeString(string(token.Data))))
							} else if tokenType == html.EndTagToken && token.Data == "a" {
								break
							} else if tokenType == html.ErrorToken {
								return &pageLinks

							}
						}

						extrasString := extras.String()
						if len(extrasString) > 256 {
							extrasString = extrasString[:256]
						}

						link := NewMarker(
							crawlingTime,
							invertedPageHost,
							isSecure,
							*normalizedPageUrl,
							normalizedHrefValue,
							fragment,
							token.Data,
							extrasString,
							dataOrigin)

						pageLinks.append(&link)
					} else {
						//LINK NORMALIZATION FAILED
						logger.Exceptions <- Exception{
							SourcePage: *normalizedPageUrl,
							ErrorType: "Link normalization failed",
							Message:   hrefValue,
						}
					}

				}

			} else if "link" == token.Data ||
				"area" == token.Data ||
				"form" == token.Data ||
				"script" == token.Data {

				var hrefValue string
				var extrasValue string

				var hrefKey string
				var extrasKey string
				linkAttributeFound := false

				switch token.Data {
				case "link":
					hrefKey = "href"
					extrasKey = "rel"
				case "script":
					hrefKey = "src"
					extrasKey = "type"
				case "form":
					hrefKey = "action"
					extrasKey = "method"
				case "area":
					hrefKey = "href"
					extrasKey = "alt"
				}

				for _, attr := range token.Attr {
					if attr.Key == hrefKey {
						hrefValue = strings.TrimSpace(attr.Val)
						linkAttributeFound = true
					} else if attr.Key == extrasKey {
						extrasValue = attr.Val
					}
				}

				if linkAttributeFound && len(hrefValue) > 0 {

					isSecure := mainPageSecure
					if strings.HasPrefix(hrefValue, "https:") {
						isSecure = true
					}

					normalizedHrefValue, fragment := getAbsoluteNormalized(pageUrl, hrefValue)

					if len(normalizedHrefValue) > 0 {
						link := NewMarker(
							crawlingTime,
							invertedPageHost,
							isSecure,
							*normalizedPageUrl,
							normalizedHrefValue,
							fragment,
							token.Data,
							extrasValue,
							dataOrigin)

						pageLinks.append(&link)
					}

				}

			}

		} else if tokenType == html.ErrorToken {
			err := tokenizer.Err()
			if err == io.EOF {
				//end of the file, break out of the loop
				break
			}
		}

	}

	return &pageLinks
}


func WriteParquet(destination string, writersChannel chan *MarkersList, failed *abool.AtomicBool, done chan bool, logger Logger) {

	//fmt.Println("Write in", destination)
	fw, err := local.NewLocalFileWriter(destination)
	if err != nil {
		failed.Set()
		logger.Exceptions <- Exception{
			//Source:          exceptionsSource,
			ErrorType:       "Write failed",
			Message:         "Impossible to create the file",
			OriginalMessage: err.Error(),
		}
		panic(err)
		// LOG IMPOSSIBLE TO CREATE THE FILE
	} else {
		//write
		pw, err := writer.NewParquetWriter(fw, new(Marker), 1)
		if err != nil {
			failed.Set()
			// LOG IMPOSSIBLE TO CREATE THE FILE
			panic(err)
		} else {

			pw.RowGroupSize = 128 * 1024 * 1024 //128M
			pw.CompressionType = parquet.CompressionCodec_GZIP
			pw.PageSize = 16 * 1024 * 1024

			// Iterate until it is open
			for linksChunk := range writersChannel {
				fmt.Println("New write request:", linksChunk.length, "links")
				for node := linksChunk.head; node != nil; node = node.next {
					if err := pw.Write(node.Marker); err != nil {
						failed.Set()
						logger.Exceptions <- Exception{
							//Source:          exceptionsSource,
							ErrorType:       "Write failed",
							Message:         "Impossible to write the record",
							OriginalMessage: err.Error(),
						}
						//LOG ERROR IN WRITING
						panic(err)

					}
				}
			}

			if err := pw.WriteStop(); err != nil {
				failed.Set()
				logger.Exceptions <- Exception{
					//Source:          exceptionsSource,
					ErrorType:       "Write failed",
					Message:         "Impossible to finalize the file",
					OriginalMessage: err.Error(),
				}
				// LOG IMPOSSIBLE TO FINALISE THE FILE
				panic(err)
			}
			fw.Close()
		}

	}

	done <- true
}
