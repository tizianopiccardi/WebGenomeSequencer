package main

type Link struct {
	Date     int64  `parquet:"name=date, type=INT64"`
	Source   string `parquet:"name=source, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Link     string `parquet:"name=link, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Fragment string `parquet:"name=fragment, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Tag      string `parquet:"name=tag, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Extras   string `parquet:"name=extras, type=UTF8, encoding=PLAIN_DICTIONARY"`
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
	if lb.head == nil {
		lb.head = linksList.head
		lb.tail = linksList.tail
	} else {
		lb.tail.next = linksList.head
		lb.tail = linksList.tail
	}
	lb.length += linksList.length
}

func (lb *LinksBuffer) copy() LinksBuffer {
	copied := LinksBuffer{}
	copied.head = lb.head
	copied.tail = lb.tail
	copied.length = lb.length
	return copied
}
