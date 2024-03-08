package stat

import (
	"crypto/rand"
	"math/big"
)

const (
	fullCnt = 10000
)

// NewAutoSampler returns a sampler that samples indices with the given ratio.
func NewAutoSampler(expectedCnt int, totalCnt int) *Sampler {
	if expectedCnt <= 0 || totalCnt <= 0 {
		panic("expectedCnt and totalCnt must be positive")
	}
	if expectedCnt >= totalCnt {
		return NewFullSampler()
	}
	ratio := int64((float64(expectedCnt) / float64(totalCnt)) * 10000)
	return &Sampler{
		sampleRatio: ratio,
	}
}

// NewSampler returns a sampler that samples indices with the given ratio.
func NewSampler(sampleRatio int) *Sampler {
	if sampleRatio <= 0 || sampleRatio > 10000 {
		panic("sampleRatio must be in (0, 10000]")
	}
	return &Sampler{
		sampleRatio: int64(sampleRatio),
	}
}

// NewFullSampler returns a sampler that samples all indices.
func NewFullSampler() *Sampler {
	return &Sampler{
		sampleRatio: fullCnt,
	}
}

// Sampler samples indices.
type Sampler struct {
	sampleRatio int64
}

// Hit returns true if the index should be sampled.
func (s *Sampler) Hit() bool {
	if s.sampleRatio == fullCnt {
		return true
	}
	k, _ := rand.Int(rand.Reader, big.NewInt(fullCnt))
	return k.Int64() < s.sampleRatio
}
