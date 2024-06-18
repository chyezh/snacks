package main

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func main() {
	r := &milvuspb.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_Insert,
		},
		CollectionName: "test",
	}

	data, err := proto.Marshal(r)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v", data)
	var h commonpb.MsgHeader
	err = proto.Unmarshal(data, &h)
	h.XXX_unrecognized[0] = 0x01
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v", data)
}
