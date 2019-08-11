package main

import (
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"sync"
	"sync/atomic"
)

//const MAX_LINK_PER_FILE = 10000000
var writerID uint64

type LinksWriter struct {
	id       uint64
	fileName string
	//RowsCount     int64
	parquetFile   source.ParquetFile
	parquetWriter *writer.ParquetWriter
	maxRows       int64
}

func NewLinksWriter(fileName string, maxLinkPerParquet int64, logger Logger) (LinksWriter, error) {
	workerID := atomic.AddUint64(&writerID, 1)
	obj := LinksWriter{fileName: fileName, maxRows: maxLinkPerParquet, id: workerID}
	//var err error
	fw, err := local.NewLocalFileWriter(fileName)
	if err != nil {
		log.Println("Can't create local file", err)
		return obj, err
	}

	//write
	pw, err := writer.NewParquetWriter(fw, new(Link), 1)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return obj, err
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_GZIP
	pw.PageSize = 1024 * 1024 * 32

	obj.parquetFile = fw
	obj.parquetWriter = pw

	return obj, nil
}

func (lw LinksWriter) write(link Link) {
	if err := lw.parquetWriter.Write(link); err != nil {
		log.Println("Write error", err)
	}
	//lw.RowsCount++
}

func (lw LinksWriter) close() {
	if err := lw.parquetWriter.WriteStop(); err != nil {
		log.Println("WriteStop error", err) //LOG
		return
	}

	lw.parquetFile.Close()
}

func writerWorker(warcLinks chan *[]Link, writerWaitingGroup *sync.WaitGroup, maxLinkPerParquet int64, logger Logger) {
	randomName, _ := uuid.NewUUID()
	lw, err := NewLinksWriter("links/"+randomName.String()+".parquet", maxLinkPerParquet, logger)
	var rowsCount int64
	if err != nil {

	} else {
		for links := range warcLinks {

			logger.CounterChannel <- WriteRequest{linksCount: int64(len(*links)), writerID: lw.id}
			//fmt.Println("GOT A LIST OF", len(*links), "ELEMENTS")
			for _, l := range *links {

				//fmt.Println(rowsCount)
				rowsCount++
				if rowsCount >= lw.maxRows {
					//fmt.Println("NEW FILE")
					lw.close()
					randomName, _ := uuid.NewUUID()
					lw, err = NewLinksWriter("links/"+randomName.String()+".parquet", maxLinkPerParquet, logger)
					rowsCount = 0
					if err != nil {

					}
				}

				lw.write(l)

			}
		}
	}

	lw.close()

	writerWaitingGroup.Done()
}
