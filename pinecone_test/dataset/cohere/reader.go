package cohere

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"

	"pinecone_test/dataset/cohere/msg"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

type Reader struct {
	files []string
	ch    chan *msg.Msg
}

func NewReader(dir string) (*Reader, error) {
	files, err := listFilesInFolder(dir)
	if err != nil {
		return nil, err
	}
	r := &Reader{
		files: files,
		ch:    make(chan *msg.Msg, 10000),
	}
	go r.readVectorFromDirectory(dir)
	return r, nil
}

func (r *Reader) Chan() <-chan *msg.Msg {
	return r.ch
}

func (r *Reader) readVectorFromDirectory(dir string) {
	defer close(r.ch)
	for _, file := range r.files {
		r.readVectorFromFile(file)
	}
}

func (r *Reader) readVectorFromFile(filename string) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		zap.L().Warn("readVectorFromFile failed, skip it", zap.Error(err))
		return
	}
	buffer := bytes.NewBuffer(data)
	for buffer.Len() > 0 {
		var length int32
		err := binary.Read(buffer, binary.BigEndian, &length)
		if err != nil {
			zap.L().Warn("binary.Read failed", zap.Error(err))
			return
		}

		blockData := buffer.Next(int(length))
		item := &msg.Msg{}
		err = proto.Unmarshal(blockData, item)
		if err != nil {
			zap.L().Warn("proto.Unmarshal failed", zap.Error(err))
			return
		}
		if item.Id < 10055960 {
			continue
		}
		r.ch <- item
	}
}

func listFilesInFolder(folderPath string) ([]string, error) {
	var files []string

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})

	return files, err
}
