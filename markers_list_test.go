package main

import (
	"fmt"
	"strconv"
	"testing"
)

// TODO: figure out how to run tests with symbols defined in external files

func TestList(t *testing.T) {
	list := MarkersList{}
	for i := 0; i < 10; i++ {
		link := Marker{Link: strconv.Itoa(i)}
		list.append(&link)
	}

	list2 := MarkersList{}
	for i := 10; i < 30; i++ {
		link := Marker{Link: strconv.Itoa(i)}
		list2.append(&link)
	}

	list.appendList(&list2)

	fmt.Println("List length:", list.length)

	for node := list.head; node != nil; node = node.next {
		fmt.Print(node.Marker.Link, "|")
	}
	fmt.Println("")

}