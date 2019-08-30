package main

import (
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"testing"
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

type Record struct {
	Value string `parquet:"name=value, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

func TestURL(t *testing.T) {

	//u, _:=url.Parse("http://schiederhof-mittersill.at/ciao#qwerty")
	//fmt.Println(u.Fragment)

	page := "http://schiederhof-mittersill.at"
	href := "Ein_herzliches#ciao"

	pageUrl, _ := url.Parse(page)

	hrefUrl, err := url.Parse(href)
	if err == nil {
		if hrefUrl.Scheme == "" {
			hrefUrl = pageUrl.ResolveReference(hrefUrl)
			//fmt.Println(hrefUrl.Fragment)

		}
		fmt.Println(hrefUrl.Fragment)
	}
	fmt.Println(hrefUrl.Fragment)

}

func TestFileReader(t *testing.T) {
	lines, err := readLines("2005-warcs.gz")
	if err != nil {
		log.Fatalf("readLines: %s", err)
	}

	for _, line := range lines {
		fmt.Println(line)
	}
}

func TestUrlParsing(t *testing.T) {
	//page := "http://www.gb.nrao.edu/~glangsto/rfi/600"
	relative := "http://www.aifb.uni-karlsruhe.de/Personen/"

	u, err := url.Parse(relative)
	if err != nil {
		println(err)
	}
	s := strings.Split(u.Host, ".")

	for i := 0; i < len(s)/2; i++ {
		j := len(s) - i - 1
		s[i], s[j] = s[j], s[i]
	}

	fmt.Println(strings.Join(s, "."))

}
