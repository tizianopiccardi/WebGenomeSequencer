package main

import (
	"fmt"
	"github.com/PuerkitoBio/purell"
	"github.com/slyrz/warc"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var TAB = []byte{9}
var NL = []byte{10}

func TestList(t *testing.T) {
	buffer := LinksBuffer{}
	for i := 0; i < 10; i++ {
		link := Link{Link: strconv.Itoa(i)}
		buffer.append(&link)
	}

	buffer2 := LinksBuffer{}
	for i := 10; i < 30; i++ {
		link := Link{Link: strconv.Itoa(i)}
		buffer2.append(&link)
	}

	buffer.appendBuffer(&buffer2)

	fmt.Println("List length:", buffer.length)

	for node := buffer.head; node != nil; node = node.next {
		fmt.Print(node.Link.Link, "|")
	}
	fmt.Println("")

}

func TestGetLinks(t *testing.T) {
	file, _ := os.Open("example.warc.gz")
	reader, _ := warc.NewReader(file)

	linksBuffer := LinksBuffer{}
	for {
		//Read a record from the WARC
		record, err := reader.ReadRecord()
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
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

					if err != nil {
						//NOT A VALID URL
					} else {

						normalizedPageUrl := purell.NormalizeURL(pageUrl, PURELL_FLAGS)

						buf := make([]byte, 12)
						_, err := record.Content.Read(buf)

						if nil == err {
							httpStatusCode := string(buf[9:12])
							if httpStatusCode == "200" {
								contentBytes, err := ioutil.ReadAll(record.Content)
								if err != nil {

								} else {
									content := string(contentBytes)
									pageLinks := getLinks(recordDate.Unix(), pageUrl, &normalizedPageUrl, &content)
									linksBuffer.appendBuffer(pageLinks)

								}
							} else if strings.HasPrefix(httpStatusCode, "3") {

								contentBytes, _ := ioutil.ReadAll(record.Content)
								responseBody := string(contentBytes)
								locationString := locationRegex.FindStringSubmatch(responseBody)

								if len(locationString) > 1 {
									redirectionUrl := strings.TrimSpace(locationString[1])

									absoluteUrl, _ := getAbsoluteNormalized(pageUrl, redirectionUrl)
									if len(absoluteUrl) > 0 {

										link := Link{
											Date:   recordDate.Unix(),
											Source: normalizedPageUrl,
											Link:   absoluteUrl,
											Tag:    httpStatusCode}
										linksBuffer.append(&link)

									}

								}

							} else if strings.HasPrefix(httpStatusCode, "4") {

								link := Link{
									Date:   recordDate.Unix(),
									Source: normalizedPageUrl,
									Tag:    httpStatusCode}
								linksBuffer.append(&link)

							}
						}
					}
				}

			}

		}

	}

	println(linksBuffer.length)

	start := time.Now()
	writeParquet(linksBuffer, 16*1024*1024, 16*1024*1024, 4)

	//f, _ := os.Create("file.gz")
	//
	//// Create gzip writer.
	//w := gzip.NewWriter(f)
	//
	//for node := linksBuffer.head; node != nil; node = node.next {
	//	w.Write([]byte(node.Link.Source))
	//	w.Write(TAB)
	//	w.Write([]byte(node.Link.Link))
	//	w.Write(TAB)
	//	w.Write([]byte(strconv.FormatInt(node.Link.Date, 10)))
	//	w.Write(TAB)
	//	w.Write([]byte(node.Link.Tag))
	//	w.Write(TAB)
	//	w.Write([]byte(node.Link.Extras))
	//	w.Write(TAB)
	//	w.Write([]byte(node.Link.Fragment))
	//	w.Write(NL)
	//}
	//// Write bytes in compressed form to the file.
	//
	//
	//// Close the file.
	//w.Close()
	//
	fmt.Println("Job completed in:", time.Now().Sub(start))

	///read
	fr, err := local.NewLocalFileReader("flat.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	for i := 0; i < num/10; i++ {
		if i%2 == 0 {
			pr.SkipRows(10) //skip 10 rows
			continue
		}
		stus := make([]Student, 10) //read 10 rows
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}

	pr.ReadStop()
	fr.Close()
	//start = time.Now()
	//writeParquet(linksBuffer, 16*1024*1024, 128*1024*1024, 1)
	//fmt.Println("Job completed in:", time.Now().Sub(start))
	//
	//start = time.Now()
	//writeParquet(linksBuffer, 16*1024*1024, 32*1024*1024, 1)
	//fmt.Println("Job completed in:", time.Now().Sub(start))
	//
	//start = time.Now()
	//writeParquet(linksBuffer, 16*1024*1024, 16*1024*1024, 1)
	//fmt.Println("Job completed in:", time.Now().Sub(start))
	//
	//start = time.Now()
	//writeParquet(linksBuffer, 128*1024*1024, 128*1024*1024, 8)
	//fmt.Println("Job completed in:", time.Now().Sub(start))

}

func writeParquet(buffer LinksBuffer, rgs int64, pageSize int64, writers int64) {
	//var err error
	//randomName, _ := uuid.NewUUID()
	fw, err := local.NewLocalFileWriter("links.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := writer.NewParquetWriter(fw, new(Link), writers)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = rgs //128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_GZIP
	pw.PageSize = pageSize //1024 * 1024 * 32

	for node := buffer.head; node != nil; node = node.next {
		if err := pw.Write(node.Link); err != nil {
			log.Println("Write error", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err) //LOG
		return
	}
	fw.Close()
}
