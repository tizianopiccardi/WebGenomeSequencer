package main

// Simple linked list to collate efficiently all the markers extracted from a Web page

type MarkersList struct {
	head   *ListNode
	tail   *ListNode
	length int32
}

type ListNode struct {
	next *ListNode
	Marker *Marker
}

// Appends a Marker element to the referred MarkersList
func (ml *MarkersList) append(link *Marker) {
	node := ListNode{Marker: link, next: nil}
	if ml.head == nil {
		ml.head = &node
		ml.tail = &node
	} else {
		ml.tail.next = &node
		ml.tail = &node
	}
	ml.length++
}

// Appends a MarkersList to the referred MarkersList
func (ml *MarkersList) appendList(linksList *MarkersList) {
	if linksList.length > 0 {
		if ml.head == nil {
			ml.head = linksList.head
			ml.tail = linksList.tail
		} else {
			ml.tail.next = linksList.head
			ml.tail = linksList.tail
		}
		ml.length += linksList.length
	}
}

// Creates a copy of the referred MarkersList
func (ml *MarkersList) copy() MarkersList {
	copied := MarkersList{}
	copied.head = ml.head
	copied.tail = ml.tail
	copied.length = ml.length
	return copied
}
