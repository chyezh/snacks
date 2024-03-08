package collections

import "github.com/milvus-io/milvus-sdk-go/v2/entity"

func newBookCollection(opts ...Opt) *Collection {
	option := options{
		Name:   "book",
		DIM:    "768",
		AutoID: true,
	}
	option.apply(opts...)
	metricType := entity.L2

	idx, err := entity.NewIndexIvfFlat(
		metricType,
		128,
	)
	if err != nil {
		panic(err)
	}

	return &Collection{
		shard: 2,
		schema: &entity.Schema{
			CollectionName: option.Name,
			Description:    "Test book search",
			Fields: []*entity.Field{
				{
					Name:       "book_id",
					DataType:   entity.FieldTypeInt64,
					PrimaryKey: true,
					AutoID:     option.AutoID,
				},
				{
					Name:     "word_count",
					DataType: entity.FieldTypeInt64,
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
		indexField: "book_intro",
		index:      idx,
		metricType: metricType,
	}
}
