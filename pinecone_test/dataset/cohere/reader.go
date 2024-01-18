package cohere

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"

	"pinecone_test/dataset/cohere/msg"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

func ReadAndDecodeProtoBlocks(filename string) (<-chan *msg.Msg, error) {
	ch := make(chan *msg.Msg, 1000)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(ch)
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
			ch <- item
		}
	}()
	return ch, nil
}
