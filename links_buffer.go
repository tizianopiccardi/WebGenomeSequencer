package main

import (
	"unicode/utf8"
)

type Link struct {
	Date       int64  `parquet:"name=date, type=INT64"`
	SourceHost string `parquet:"name=source, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Secure     bool   `parquet:"name=bool, type=BOOLEAN"`
	Source     string `parquet:"name=source, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Link       string `parquet:"name=link, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Fragment   string `parquet:"name=fragment, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Tag        string `parquet:"name=tag, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Extras     string `parquet:"name=extras, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

type ListNode struct {
	next *ListNode
	Link *Link
}

type LinksBuffer struct {
	head   *ListNode
	tail   *ListNode
	length int32
}

//func NewExistsLink(date int64, source string) Link {
//	return NewLink(date, source, "", "", "200", "")
//}

func NewExistsMarker(date int64, source, httpCode, extras string, secure bool, host string) Link {
	//fmt.Println("MARKER")
	return NewLink(date, source, "", "", httpCode, extras, secure, host)
}

func ToValidUTF8(text string) string {
	if utf8.ValidString(text) {
		return text
	}
	s := []byte(text)
	b := make([]byte, 0, len(s))
	invalid := false // previous byte was from an invalid UTF-8 sequence
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			i++
			invalid = false
			b = append(b, byte(c))
			continue
		}
		_, wid := utf8.DecodeRune(s[i:])
		if wid == 1 {
			i++
			if !invalid {
				invalid = true
				//b = append(b, replacement...)
			}
			continue
		}
		invalid = false
		b = append(b, s[i:i+wid]...)
		i += wid
	}
	return string(b)
}

func NewLink(date int64, source, link, fragment, tag, extras string, secure bool, host string) Link {
	return Link{Date: date,
		SourceHost: host,
		Secure:     secure,
		Source:     ToValidUTF8(source),
		Link:       ToValidUTF8(link),
		Fragment:   ToValidUTF8(fragment),
		Tag:        tag,
		Extras:     ToValidUTF8(extras),
	}
}

func (lb *LinksBuffer) append(link *Link) {
	node := ListNode{Link: link, next: nil}
	if lb.head == nil {
		lb.head = &node
		lb.tail = &node
	} else {
		lb.tail.next = &node
		lb.tail = &node
	}
	lb.length++
}

func (lb *LinksBuffer) appendBuffer(linksList *LinksBuffer) {
	if linksList.length > 0 {
		if lb.head == nil {
			lb.head = linksList.head
			lb.tail = linksList.tail
		} else {
			lb.tail.next = linksList.head
			lb.tail = linksList.tail
		}
		lb.length += linksList.length
	}
}

func (lb *LinksBuffer) copy() LinksBuffer {
	copied := LinksBuffer{}
	copied.head = lb.head
	copied.tail = lb.tail
	copied.length = lb.length
	return copied
}
