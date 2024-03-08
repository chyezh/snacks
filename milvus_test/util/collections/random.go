package collections

import "github.com/milvus-io/milvus-sdk-go/v2/entity"

func newRandomCollection(opts ...Opt) *Collection {
	option := options{
		Name: "random",
		DIM:  "128",
	}
	option.apply(opts...)
	metricType := entity.L2

	idx, err := entity.NewIndexIvfFlat(
		metricType,
		1024,
	)
	if err != nil {
		panic(err)
	}

	return &Collection{
		schema: &entity.Schema{
			CollectionName: option.Name,
			Description:    "Test random search",
			Fields: []*entity.Field{
				{
					Name:       "book_id",
					DataType:   entity.FieldTypeInt64,
					PrimaryKey: true,
					AutoID:     option.AutoID,
				},
				{
					Name:       "word_count",
					DataType:   entity.FieldTypeInt64,
					PrimaryKey: false,
					AutoID:     false,
				},
				{
					Name:     "book_intro",
					DataType: entity.FieldTypeFloatVector,
					TypeParams: map[string]string{
						"dim": option.DIM,
					},
				},
			},
		},
		shard:      2,
		indexField: "book_intro",
		index:      idx,
		metricType: metricType,
	}
}
