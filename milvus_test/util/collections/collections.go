package collections

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/remeh/sizedwaitgroup"
)

const (
	Book collectionEnum = iota
)

type collectionEnum int

type Collection struct {
	schema     *entity.Schema
	shard      int32
	index      entity.Index
	metricType entity.MetricType
	indexField string
	cli        client.Client
}

// NewCollection create a new collection info.
func NewCollection(e collectionEnum, cli client.Client, opts ...Opt) *Collection {
	newer := map[collectionEnum]func(...Opt) *Collection{
		Book: newBookCollection,
	}
	coll := newer[e](opts...)
	coll.cli = cli
	return coll
}

func (coll *Collection) DropCollection(ctx context.Context) error {
	return coll.cli.DropCollection(ctx, coll.Name())
}

func (coll *Collection) CreateCollection(ctx context.Context) error {
	return coll.cli.CreateCollection(ctx, coll.schema, coll.shard, client.WithConsistencyLevel(entity.ClBounded))
}

func (coll *Collection) ApplyRandomCase(ctx context.Context, count int) error {
	wg := sizedwaitgroup.New(5)
	batch := 1000
	offset := 0
	for count > 0 {
		colls := []entity.Column{}
		if batch > count {
			batch = count
		}
		for _, f := range coll.schema.Fields {
			if f.AutoID {
				continue
			}
			switch f.DataType {
			case entity.FieldTypeInt64:
				if f.PrimaryKey {
					colls = append(colls, entity.NewColumnInt64(f.Name, rangeInt64Slice(int64(offset), batch)))
				} else {
					// colls = append(colls, entity.NewColumnInt64(f.Name, rangeInt64Slice(0, 1e8, batch)))
					colls = append(colls, entity.NewColumnInt64(f.Name, rangeInt64Slice(int64(offset), batch)))
				}
			case entity.FieldTypeFloatVector:
				dim, err := strconv.ParseInt(f.TypeParams["dim"], 10, 64)
				if err != nil {
					panic(err)
				}
				colls = append(colls, entity.NewColumnFloatVector(f.Name, int(dim), randFloatVectorSlice(int(dim), batch)))
			default:
				panic("unsupported data type")
			}
		}
		wg.Add()
		go func() {
			defer wg.Done()
			if _, err := coll.cli.Insert(ctx, coll.Name(),
				"",
				colls...,
			); err != nil {
				fmt.Printf("insert failed, %s\n", err)
			}
			fmt.Print("insert success\n")
		}()
		count -= batch
		offset += batch
	}
	wg.Wait()
	return coll.cli.Flush(ctx, coll.Name(), false)
}

func (coll *Collection) ApplyIndex(ctx context.Context) error {
	if err := coll.cli.CreateIndex(ctx, coll.Name(), coll.indexField, coll.index, false); err != nil {
		panic(err)
	}
	for {
		state, err := coll.cli.GetIndexState(ctx, coll.Name(), coll.indexField)
		if err != nil {
			panic(err)
		}
		if state == 3 {
			break
		}
		fmt.Printf("index state: %d\n", state)
		time.Sleep(time.Second)
	}
	return nil
}

func (coll *Collection) Load(ctx context.Context) error {
	return coll.cli.LoadCollection(ctx, coll.Name(), false)
}

func (coll *Collection) Name() string {
	return coll.schema.CollectionName
}

func (coll *Collection) VecField() *entity.Field {
	for _, field := range coll.schema.Fields {
		if field.DataType == entity.FieldTypeFloatVector {
			return field
		}
	}
	return nil
}

func (coll *Collection) DIM() int {
	for _, field := range coll.schema.Fields {
		if field.DataType == entity.FieldTypeFloatVector {
			dim, err := strconv.ParseInt(field.TypeParams["dim"], 10, 64)
			if err != nil {
				panic(err)
			}
			return int(dim)
		}
	}
	return 0
}

func randRangeQueryExpr(keyName string) string {
	randPKCount := rand.Int31n(100000) + 1
	return fmt.Sprintf("%s > %d", keyName, 442164263772235776+int(randPKCount))
}

func randKeyInQueryExpr(keyName string) string {
	randPKCount := rand.Int31n(100) + 1
	return fmt.Sprintf("%s in [%s]", keyName, randInt64SliceString(442164263772235776, 500000, int(randPKCount)))
}

func randInt64SliceString(offset int64, limit int64, count int) string {
	if count == 0 {
		panic("randInt64SliceString's count must be greater than 0")
	}
	ints := randInt64Slice(offset, limit, count)
	intString := make([]string, 0, len(ints))
	for _, i := range ints {
		intString = append(intString, strconv.FormatInt(i, 10))
	}
	return strings.Join(intString, ",")
}

func rangeInt64Slice(offset int64, count int) []int64 {
	items := make([]int64, count)
	for i := 0; i < count; i++ {
		items[i] = offset + int64(i)
	}
	return items
}

func randInt64Slice(offset int64, limit int64, count int) []int64 {
	items := make([]int64, count)
	for i := range items {
		items[i] = offset + rand.Int63n(limit) // generates a random int64 between 0 and 99
	}
	return items
}

func randFloatVectorSlice(dim int, count int) [][]float32 {
	items := make([][]float32, count)
	for i := range items {
		items[i] = randFloatVector(dim)
	}
	return items
}

func randFloatVectorEntitySlice(dim int, count int) []entity.Vector {
	items := make([]entity.Vector, count)
	for i := range items {
		items[i] = entity.FloatVector(randFloatVector(dim))
	}
	return items
}

func randFloatVector(dim int) []float32 {
	v := make([]float32, dim)
	for j := range v {
		v[j] = rand.Float32()
	}
	return v
}
