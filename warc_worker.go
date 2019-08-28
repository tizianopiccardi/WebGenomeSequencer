package main

import (
	"bufio"
	"errors"
	"github.com/PuerkitoBio/purell"
	"github.com/slyrz/warc"
	"github.com/tevino/abool"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/net/html"
	"golang.org/x/net/html/charset"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const CHUNK_SIZE = 500000

//var locationRegex *regexp.Regexp = regexp.MustCompile(`\nLocation: ([^\n]*)\n`)

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

type SourceDestination struct {
	SourceFile      string
	DestinationFile string
}

func isValidUrl(toTest string) bool {
	u, _ := url.ParseRequestURI(toTest)
	if u.Host == "" {
		return false
	} else {
		return true
	}
}

func getCharsetReader(reader *bufio.Reader, contentType string) io.Reader {
	bodySample, _ := reader.Peek(1024)
	encoding, _, _ := charset.DetermineEncoding(bodySample, contentType)
	return encoding.NewDecoder().Reader(reader)
}

func getReader(path string) (io.ReadCloser, error) {
	if isValidUrl(path) {

		resp, err := http.Get(path)
		if err != nil {
			return nil, err
		}
		return resp.Body, nil
	} else {
		//fmt.Print("HERE")
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return file, nil
	}

	return nil, errors.New("URL/path not valid: " + path)
}

func LinkExtractionWorker(paths chan SourceDestination, workersWaitGroup *sync.WaitGroup, logger Logger) {

	exceptionsSource := ""

	for path := range paths {

		fileReader, err := getReader(path.SourceFile)
		if err != nil {
			logger.Exceptions <- Exception{
				File:            path.SourceFile,
				Source:          exceptionsSource,
				ErrorType:       "File not found",
				Message:         path.SourceFile,
				OriginalMessage: err.Error(),
			}

		} else {

			logger.FileStartChannel <- path

			recordsReader, err := warc.NewReader(fileReader)
			if err != nil {
				logger.Exceptions <- Exception{
					File:            path.SourceFile,
					Source:          exceptionsSource,
					ErrorType:       "WARC Reader failed",
					Message:         path.SourceFile,
					OriginalMessage: err.Error(),
				}

			} else {

				// Channel to share the chucks to write
				writerChannel := make(chan *LinksBuffer, 150)

				// Synchronized boolean var to inform the reader if the writer filed
				failedWriterFlag := abool.New()

				// Get a message when the writer completed the job
				writerDone := make(chan bool)

				// - The writer runs waiting from links chunks from the channel
				// - If it fails, it sets the failedWriterFlag to TRUE and log the error
				// - The reader checks regularly the flag, if it's TRUE: break
				go WriteParquet(path.DestinationFile, writerChannel, failedWriterFlag, writerDone, logger)

				ReadWarc(recordsReader, writerChannel, failedWriterFlag, logger, path.SourceFile)

				// The reader ended, the file if completely processed and we can
				// inform the writer by closing the channel
				close(writerChannel)

				// Wait for the writer to complete
				<-writerDone

			}

			recordsReader.Close()
			fileReader.Close()

			logger.FileEndChannel <- path
		}

	}

	workersWaitGroup.Done()
}

func ReadWarc(recordsReader *warc.Reader, writersChannel chan *LinksBuffer, failedWriterFlag *abool.AtomicBool, logger Logger, path string) {
	linksBuffer := LinksBuffer{}
	//var i int64
	//var total int64

	exceptionsSource := "Reader " + path

	for {
		if linksBuffer.length >= CHUNK_SIZE {

			// If the writer is dead, stop the reader
			if failedWriterFlag.IsSet() {
				//LOG FAILED
				//fmt.Println("WRITER FAILED")
				logger.Exceptions <- Exception{
					File:      path,
					Source:    exceptionsSource,
					ErrorType: "Reader controlled failure",
					Message:   "The writer failed and the reader is interrupting the job",
				}
				break
			}

			// Send the chunk and allocate a new list
			copied := linksBuffer.copy()
			writersChannel <- &copied
			linksBuffer = LinksBuffer{}
		}

		record, err := recordsReader.ReadRecord()
		if err != nil {
			if err != io.EOF {
				logger.Exceptions <- Exception{
					File:            path,
					Source:          exceptionsSource,
					ErrorType:       "Record malformed",
					Message:         "The reader failed to process the record",
					OriginalMessage: err.Error(),
				}
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
						File:            path,
						Source:          exceptionsSource,
						ErrorType:       "Date parsing error",
						Message:         record.Header.Get("warc-date"),
						OriginalMessage: err.Error(),
					}
				} else {
					originalUrl := record.Header.Get("WARC-Target-URI")
					pageUrl, err := url.Parse(originalUrl)

					if err != nil {
						logger.Exceptions <- Exception{
							File:            path,
							Source:          exceptionsSource,
							ErrorType:       "Not an URL",
							Message:         originalUrl,
							OriginalMessage: err.Error(),
						}
					} else {

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
								httpStatusCode = line[9:12]
							}

							if strings.HasPrefix(line, "Location:") {
								redirectLocation = line[10:]
							}

							if strings.HasPrefix(line, "Content-Type:") {
								contentType = line[14:]
							}

						}

						extras := ""
						if httpStatusCode == "200" {

							if strings.HasPrefix(contentType, "text/html") {
								customReader := getCharsetReader(reader, contentType)
								pageLinks := getLinks(recordDate.Unix(), pageUrl, &normalizedPageUrl, customReader, logger, path)
								linksBuffer.appendBuffer(pageLinks)
							}

						} else {

							if len(redirectLocation) > 0 {
								redirectLocation = strings.TrimSpace(redirectLocation)
								extras, _ = getAbsoluteNormalized(pageUrl, redirectLocation)
							}

						}

						// Add the marker to know that the crawler visited the page
						link := NewExistsMarker(recordDate.Unix(), normalizedPageUrl, httpStatusCode, extras)
						linksBuffer.append(&link)

					}

				}
			}

		}
	}
	writersChannel <- &linksBuffer

}

func getLinks(crawlingTime int64, pageUrl *url.URL, normalizedPageUrl *string, body io.Reader, logger Logger, path string) *LinksBuffer {

	exceptionsSource := "GetLinks in " + pageUrl.String()

	//Links in the current page
	pageLinks := LinksBuffer{}

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
						break
					}
				}
				//fmt.Println(hrefValue)
				if !strings.HasPrefix(hrefValue, "javascript:") &&
					!strings.HasPrefix(hrefValue, "#") {

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

						link := NewLink(crawlingTime,
							*normalizedPageUrl,
							normalizedHrefValue,
							fragment,
							token.Data,
							extrasString)

						pageLinks.append(&link)
					} else {
						//LINK NORMALIZATION FAILED
						logger.Exceptions <- Exception{
							File:      path,
							Source:    exceptionsSource,
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
					normalizedHrefValue, fragment := getAbsoluteNormalized(pageUrl, hrefValue)

					if len(normalizedHrefValue) > 0 {
						link := NewLink(crawlingTime,
							*normalizedPageUrl,
							normalizedHrefValue,
							fragment,
							token.Data,
							extrasValue)

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

func WriteParquet(destination string, writersChannel chan *LinksBuffer, failed *abool.AtomicBool, done chan bool, logger Logger) {

	exceptionsSource := "Writer " + destination

	//fmt.Println("Write in", destination)
	fw, err := local.NewLocalFileWriter(destination)
	if err != nil {
		failed.Set()
		logger.Exceptions <- Exception{
			Source:          exceptionsSource,
			ErrorType:       "Write failed",
			Message:         "Impossible to create the file",
			OriginalMessage: err.Error(),
		}
		// LOG IMPOSSIBLE TO CREATE THE FILE
	} else {
		//write
		pw, err := writer.NewParquetWriter(fw, new(Link), 1)
		if err != nil {
			failed.Set()
			// LOG IMPOSSIBLE TO CREATE THE FILE
		} else {

			pw.RowGroupSize = 128 * 1024 * 1024 //128M
			pw.CompressionType = parquet.CompressionCodec_GZIP
			pw.PageSize = 16 * 1024 * 1024

			// Iterate until it is open
			for linksChunk := range writersChannel {
				//fmt.Println("New write request:", linksChunk.length, "links")
				for node := linksChunk.head; node != nil; node = node.next {

					if err := pw.Write(node.Link); err != nil {
						failed.Set()
						logger.Exceptions <- Exception{
							Source:          exceptionsSource,
							ErrorType:       "Write failed",
							Message:         "Impossible to write the record",
							OriginalMessage: err.Error(),
						}
						//LOG ERROR IN WRITING
						break

					}
				}
			}

			if err := pw.WriteStop(); err != nil {
				failed.Set()
				logger.Exceptions <- Exception{
					Source:          exceptionsSource,
					ErrorType:       "Write failed",
					Message:         "Impossible to finalize the file",
					OriginalMessage: err.Error(),
				}
				// LOG IMPOSSIBLE TO FINALISE THE FILE
			}
			fw.Close()
		}

	}

	done <- true
}

//
//func WriteJson(destination string, writersChannel chan *LinksBuffer, failed *abool.AtomicBool, done chan bool) {
//
//	fmt.Println("Write in", destination)
//	f, err := os.Create(destination + ".gzip")
//
//	if err != nil {
//		failed.Set()
//	} else {
//		// Create gzip writer.
//		w := gzip.NewWriter(f)
//
//		for linksChunk := range writersChannel {
//			fmt.Println("New write request:", linksChunk.length, "links")
//			for node := linksChunk.head; node != nil; node = node.next {
//
//				linkJson, err := json.Marshal(node.Link)
//				if err != nil {
//					failed.Set()
//					break
//				}
//				w.Write(linkJson)
//				w.Write([]byte("\n"))
//			}
//		}
//
//		w.Close()
//	}
//
//	done <- true
//}
