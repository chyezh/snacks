package collections

import "strconv"

type Opt = func(opt *options)

type options struct {
	Name   string
	DIM    string
	AutoID bool
}

func (o *options) apply(opts ...Opt) {
	for _, opt := range opts {
		opt(o)
	}
}

func OptName(name string) Opt {
	return func(opt *options) {
		opt.Name = name
	}
}

func OptDIM(dim int) Opt {
	return func(opt *options) {
		opt.DIM = strconv.FormatInt(int64(dim), 10)
	}
}

func OptAutoID(autoID bool) Opt {
	return func(opt *options) {
		opt.AutoID = autoID
	}
}
