package main

import (
	"github.com/PuerkitoBio/purell"
	"github.com/slyrz/warc"
	"golang.org/x/net/html"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

var locationRegex *regexp.Regexp = regexp.MustCompile(`\nLocation: ([^\n]*)\n`)

const CHUNK_SIZE = 100000

var readerID uint64

func readerWorker(jobs chan string, writersChannel chan *[]Link, readersWaitGroup *sync.WaitGroup, logger Logger) {
	//id := atomic.AddUint64(&writerID, 1)

	for path := range jobs {

		file, err := os.Open(path)

		reader, err := warc.NewReader(file)
		if err != nil {
			//panic(err)//NOOOO
		}

		tmp := make([]Link, 0)
		var linksBuffer *[]Link = &tmp

		//fmt.Println(path)
		logger.FileStartChannel <- path
		for {

			if len(*linksBuffer) >= CHUNK_SIZE {
				writersChannel <- linksBuffer
				tmp := make([]Link, 0)
				linksBuffer = &tmp
			}

			//Read a record from thw WARC
			record, err := reader.ReadRecord()
			if err != nil {
				if err != io.EOF {
					//LOG FILE FAILED DON'T ADD LINKS
				}
				break
			} else {
				contentType := record.Header.Get("content-type")
				recordType := record.Header.Get("warc-type")
				if recordType == "response" && strings.HasPrefix(contentType, "application/http") {

					recordDate, err := time.Parse(time.RFC3339, record.Header.Get("warc-date"))
					if err != nil {

					} else {
						originalUrl := record.Header.Get("WARC-Target-URI")

						pageUrl, err := url.Parse(originalUrl)

						//println(originalUrl)

						if err != nil {

						} else {

							normalizedPageUrl := purell.NormalizeURL(pageUrl, PURELL_FLAGS)

							buf := make([]byte, 12)
							_, err := record.Content.Read(buf)

							if nil == err {
								httpStatusCode := string(buf[9:12])
								//fmt.Println(httpStatusCode)
								if httpStatusCode == "200" {
									contentBytes, err := ioutil.ReadAll(record.Content)
									if err != nil {

									} else {
										content := string(contentBytes)
										pageLinks := getLinks(recordDate.Unix(), pageUrl, &normalizedPageUrl, &content)
										*linksBuffer = append(*linksBuffer, pageLinks...)
										//linksBuffer = &linksBuffer_
									}
								} else if strings.HasPrefix(httpStatusCode, "3") {
									//fmt.Println(string(buf))
									//allWarcLinks.a
									contentBytes, _ := ioutil.ReadAll(record.Content)
									responseBody := string(contentBytes)
									locationString := locationRegex.FindStringSubmatch(responseBody)

									if len(locationString) > 1 {
										redirectionUrl := strings.TrimSpace(locationString[1])

										absoluteUrl, _ := getAbsoluteNormalized(pageUrl, redirectionUrl)
										if len(absoluteUrl) > 0 {
											//if absoluteUrl=="http://acomp.stanford.edu/networkconnections/stepbystep/iOS" {
											//	fmt.Println(originalUrl, locationString, responseBody)
											//}
											*linksBuffer = append(*linksBuffer,
												Link{
													Date:   recordDate.Unix(),
													Source: normalizedPageUrl,
													Link:   absoluteUrl, Tag: httpStatusCode})
											//fmt.Println(Link{
											//	Date: recordDate.Unix(),
											//	Source:normalizedPageUrl,
											//	Link:absoluteUrl, Tag:
											//		httpStatusCode})
										}

									}
									//locationStartIndex := strings.Index(responseBody, "\r\nLocation:")
									//locationEndIndex := strings.Index(responseBody, "\r\nLocation:")

								}
							}
						}
					}

				}

			}

		}

		writersChannel <- linksBuffer

		reader.Close()

		logger.FileEndChannel <- path
	}
	readersWaitGroup.Done()
}

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

func getLinks(crawlingTime int64, pageUrl *url.URL, normalizedPageUrl *string, body *string) []Link {
	//Links in the current page
	var pageLinks []Link

	//Parse the URL of the current page
	//pageUrl, _ := url.Parse(originalUrl)

	//if pageUrl != nil {

	//Initialise tokenizer
	tokenizer := html.NewTokenizer(strings.NewReader(*body))

	pageExistsMarker := Link{Source: *normalizedPageUrl,
		Date: crawlingTime,
		Tag:  "exists",
	}

	pageLinks = append(pageLinks, pageExistsMarker)

	for {
		//get the next token type
		tokenType := tokenizer.Next()
		//fmt.Println(tokenizer.Token().Data)
		//fmt.Println(tokenType)
		if tokenType == html.StartTagToken || tokenType == html.SelfClosingTagToken {
			//Get token info
			token := tokenizer.Token()
			//fmt.Println(token.Data)
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
							//fmt.Println(tokenType)
							if tokenType == html.TextToken {
								extras.WriteString(strings.TrimSpace(html.UnescapeString(string(token.Data))))
							} else if tokenType == html.EndTagToken && token.Data == "a" {
								break
							} else if tokenType == html.ErrorToken {
								return pageLinks

							}
						}

						extrasString := extras.String()
						if len(extrasString) > 256 {
							extrasString = extrasString[:256]
						}

						link := Link{
							Source:   *normalizedPageUrl,
							Date:     crawlingTime,
							Tag:      token.Data,
							Extras:   extrasString,
							Fragment: hrefObject.Fragment,
							Link:     normalizedHrefValue,
						}

						pageLinks = append(pageLinks, link)
					} else {

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

				//fmt.Println(pageUrl, hrefValue, normalizedHrefValue)
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
						pageLinks = append(pageLinks, link)
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
	//} else {
	//	//Page URL cannot be parsed
	//	//LOG
	//}
	return pageLinks
}
