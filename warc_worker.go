package main

import (
	"errors"
	"fmt"
	"github.com/PuerkitoBio/purell"
	"github.com/slyrz/warc"
	"github.com/tevino/abool"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/net/html"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const CHUNK_SIZE = 500000

var locationRegex *regexp.Regexp = regexp.MustCompile(`\nLocation: ([^\n]*)\n`)

const PURELL_FLAGS = purell.FlagsUsuallySafeGreedy |
	purell.FlagForceHTTP |
	purell.FlagRemoveFragment |
	purell.FlagSortQuery

func getAbsoluteNormalized(pageUrl *url.URL, href string) (string, *url.URL) {
	hrefUrl, err := url.Parse(href)
	if err == nil {
		if hrefUrl.Scheme == "" {
			hrefUrl = pageUrl.ResolveReference(hrefUrl)
		}
		return purell.NormalizeURL(hrefUrl, PURELL_FLAGS), hrefUrl
	}
	return "", hrefUrl
}

type SourceDestination struct {
	SourceFile      string
	DestinationFile string
}

func isValidUrl(toTest string) bool {
	_, err := url.ParseRequestURI(toTest)
	if err != nil {
		return false
	} else {
		return true
	}
}

func getReader(path string) (io.ReadCloser, error) {
	if isValidUrl(path) {
		resp, err := http.Get(path)
		if err != nil {
			return nil, err
		}
		return resp.Body, nil
	} else {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return file, nil
	}

	return nil, errors.New("URL/path not valid: " + path)
}

func LinkExtractionWorker(paths chan SourceDestination, workersWaitGroup *sync.WaitGroup, logger Logger) {

	for path := range paths {

		fileReader, err := getReader(path.SourceFile)
		if err != nil {
			//logger.Error
			//FILE NOT FOUND
		} else {

			logger.FileStartChannel <- path

			recordsReader, err := warc.NewReader(fileReader)
			if err != nil {
				//logger.Errors
				//FILE IN WRONG FORMAT
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
				go WriteParquet(path.DestinationFile, writerChannel, failedWriterFlag, writerDone)

				ReadWarc(recordsReader, writerChannel, failedWriterFlag, logger)

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

func ReadWarc(recordsReader *warc.Reader, writersChannel chan *LinksBuffer, failedWriterFlag *abool.AtomicBool, logger Logger) {
	linksBuffer := LinksBuffer{}

	for {
		if linksBuffer.length >= CHUNK_SIZE {

			// If the writer is dead, stop the reader
			if failedWriterFlag.IsSet() {
				//LOG FAILED
				fmt.Println("WRITER FAILED")
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
				//LOG: FILE FAILED DON'T ADD LINKS
			}
			break
		} else {

			contentType := record.Header.Get("content-type")
			recordType := record.Header.Get("warc-type")

			if recordType == "response" && strings.HasPrefix(contentType, "application/http") {
				recordDate, err := time.Parse(time.RFC3339, record.Header.Get("warc-date"))
				if err == nil {
					originalUrl := record.Header.Get("WARC-Target-URI")
					pageUrl, err := url.Parse(originalUrl)

					if err == nil {

						normalizedPageUrl := purell.NormalizeURL(pageUrl, PURELL_FLAGS)

						buf := make([]byte, 12)
						n, err := record.Content.Read(buf)

						if err == nil && n == 12 {
							httpStatusCode := string(buf[9:12])

							// OK response
							if httpStatusCode == "200" {
								contentBytes, err := ioutil.ReadAll(record.Content)
								if err == nil {
									content := string(contentBytes)
									pageLinks := getLinks(recordDate.Unix(), pageUrl, &normalizedPageUrl, &content)
									linksBuffer.appendBuffer(pageLinks)
								}
							} else {

								var absoluteUrl string
								if strings.HasPrefix(httpStatusCode, "3") {
									//Redirect 3xx will add in the Link filed the destination
									contentBytes, err := ioutil.ReadAll(record.Content)
									if err == nil {
										responseBody := string(contentBytes)
										locationString := locationRegex.FindStringSubmatch(responseBody)
										if len(locationString) > 1 {
											redirectionUrl := strings.TrimSpace(locationString[1])
											absoluteUrl, _ = getAbsoluteNormalized(pageUrl, redirectionUrl)
										}
									}
								}

								link := Link{
									Date:   recordDate.Unix(),
									Source: normalizedPageUrl,
									Link:   absoluteUrl,
									Tag:    httpStatusCode}
								linksBuffer.append(&link)

							}
						}

					}

				}
			}

		}
	}
	writersChannel <- &linksBuffer
}

func getLinks(crawlingTime int64, pageUrl *url.URL, normalizedPageUrl *string, body *string) *LinksBuffer {
	//Links in the current page
	pageLinks := LinksBuffer{}

	//Initialise tokenizer
	tokenizer := html.NewTokenizer(strings.NewReader(*body))

	pageExistsMarker := Link{Source: *normalizedPageUrl,
		Date: crawlingTime,
		Tag:  "exists",
	}

	pageLinks.append(&pageExistsMarker)

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
						hrefValue = strings.TrimSpace(attr.Val)
						break
					}
				}
				//fmt.Println(hrefValue)
				if !strings.HasPrefix(hrefValue, "javascript:") &&
					!strings.HasPrefix(hrefValue, "#") {
					normalizedHrefValue, hrefObject := getAbsoluteNormalized(pageUrl, hrefValue)
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

						//if !utf8.ValidString(normalizedHrefValue) &&
						//	!strings.HasPrefix(normalizedHrefValue,"http://aouclass.net")&&
						//	!strings.HasPrefix(*normalizedPageUrl,"http://awabi")&&
						//	!strings.HasPrefix(*normalizedPageUrl,"http://comic2"){
						//	fmt.Println(*body)
						//	fmt.Println(normalizedHrefValue)
						//}

						if !utf8.ValidString(extrasString) {
							fmt.Println(*body)
							fmt.Println(extrasString)
						}

						link := Link{
							Source:   *normalizedPageUrl,
							Date:     crawlingTime,
							Tag:      token.Data,
							Extras:   extrasString,
							Fragment: hrefObject.Fragment,
							Link:     normalizedHrefValue,
						}
						pageLinks.append(&link)
					} else {
						//LINK NORMALIZATION FAILED
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
					normalizedHrefValue, hrefObject := getAbsoluteNormalized(pageUrl, hrefValue)
					if hrefObject == nil {

					} else {
						link := Link{
							Source:   *normalizedPageUrl,
							Date:     crawlingTime,
							Tag:      token.Data,
							Extras:   extrasValue,
							Fragment: hrefObject.Fragment,
							Link:     normalizedHrefValue,
						}
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

func WriteParquet(destination string, writersChannel chan *LinksBuffer, failed *abool.AtomicBool, done chan bool) {

	fmt.Println("Write in", destination)
	fw, err := local.NewLocalFileWriter(destination)
	if err != nil {
		failed.Set()
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
			pw.PageSize = 1024 * 1024 * 32

			// Iterate until it is open
			for linksChunk := range writersChannel {
				fmt.Println("New write request:", linksChunk.length, "links")
				for node := linksChunk.head; node != nil; node = node.next {
					//if !utf8.ValidString(node.Link.Link) || !utf8.ValidString(node.Link.Source) ||
					//	!utf8.ValidString(node.Link.Fragment) || !utf8.ValidString(node.Link.Extras){
					//	//fmt.Println(node.Link)
					//}else {
					if err := pw.Write(node.Link); err != nil {
						failed.Set()
						//LOG ERROR IN WRITING
						break
						//}
					}
				}
			}

			if err := pw.WriteStop(); err != nil {
				failed.Set()
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
