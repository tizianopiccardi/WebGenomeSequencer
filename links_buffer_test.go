package main

import (
	"fmt"
	"strconv"
	"testing"
	"unicode/utf8"
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

	s := "http://schiederhof-mittersill.at?Ein_herzliches_\x84Gr\xfc\xdf_Gott\x93&nbsp;auf_dem&nbsp;Schiederhof_in_Mittersill=&print="
	fmt.Println(s)
	fmt.Println(utf8.ValidString(s))
	fmt.Println(ToValidUTF8(s))

	//q, _ := url.QueryUnescape("http://schiederhof-mittersill.at/?Ein_herzliches_%84Gr%FC%DF_Gott%93%26nbsp%3Bauf_dem%26nbsp%3BSchiederhof_in_Mittersill")
	//fmt.Println(utf8.ValidString(q))
	//q,_= decoder.String("dsd")
	//
	//var err error
	//fw, err := local.NewLocalFileWriter("flat.parquet")
	//if err != nil {
	//	log.Println("Can't create local file", err)
	//	return
	//}
	//
	////write
	//pw, err := writer.NewParquetWriter(fw, new(Record), 1)
	//if err != nil {
	//	log.Println("Can't create parquet writer", err)
	//	return
	//}
	//
	//pw.RowGroupSize = 128 * 1024 * 1024 //128M
	//pw.CompressionType = parquet.CompressionCodec_GZIP
	//
	//record := Record{Value:q}
	//if err = pw.Write(record); err != nil {
	//	log.Println("Write error", err)
	//}
	//
	//if err = pw.WriteStop(); err != nil {
	//	log.Println("WriteStop error", err)
	//	return
	//}
	//log.Println("Write Finished")
	//fw.Close()

}
