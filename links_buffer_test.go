package main

import (
	"fmt"
	"net/url"
	"strconv"
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
