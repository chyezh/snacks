package cohere

import (
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"pinecone_test/pinecone"
)

var batchSize = 500

func NewTestCase(r *Reader, namespace string) *TestCase {
	tc := &TestCase{
		Reader:    r,
		Namespace: namespace,
		deleteCh:  make(chan pinecone.DeleteRequest, 100),
		upsertCh:  make(chan pinecone.UpsertRequest, 100),
		queryCh:   make(chan pinecone.QueryRequest, 100),
		closed:    make(chan struct{}),
		deleted:   make(map[int32]struct{}),
	}
	tc.wg.Add(3)
	go tc.doUpsert()
	go tc.doDelete()
	go tc.doQuery()
	return tc
}

type TestCase struct {
	*Reader
	Namespace string
	wg        sync.WaitGroup
	deleteCh  chan pinecone.DeleteRequest
	upsertCh  chan pinecone.UpsertRequest
	queryCh   chan pinecone.QueryRequest
	maxID     atomic.Int32
	closed    chan struct{}
	deleted   map[int32]struct{}
}

func (tc *TestCase) doDelete() {
	defer tc.wg.Done()
	for {
		maxID := tc.maxID.Load()
		select {
		case <-tc.closed:
			return
		default:
		}
		if maxID == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		req := pinecone.DeleteRequest{
			Namespace: tc.Namespace,
			IDs:       make([]string, 0, batchSize),
		}
		for i := 0; i < batchSize; i++ {
			id := rand.Int31n(maxID)
			if _, ok := tc.deleted[id]; ok {
				continue
			}
			req.IDs = append(req.IDs, strconv.FormatInt(int64(id), 10))
		}
		if len(req.IDs) <= 100 {
			continue
		}

		select {
		case tc.deleteCh <- req:
			for _, id := range req.IDs {
				intID, _ := strconv.ParseInt(id, 10, 64)
				tc.deleted[int32(intID)] = struct{}{}
			}
		case <-tc.closed:
			return
		}
	}
}

func (tc *TestCase) doQuery() {
	defer tc.wg.Done()
	for msg := range tc.Chan() {
		select {
		case tc.queryCh <- pinecone.QueryRequest{
			ID:        strconv.FormatInt(int64(msg.Id), 10),
			Namespace: tc.Namespace,
			TopK:      100,
			Vector:    msg.Emb,
		}:
		case <-tc.closed:
			return
		}
	}
}

func (tc *TestCase) doUpsert() {
	defer tc.wg.Done()
	upsert := &pinecone.UpsertRequest{
		Namespace: tc.Namespace,
		Done:      make(chan struct{}),
	}
	maxID := int32(0)
	for msg := range tc.Chan() {
		upsert.Vectors = append(upsert.Vectors, pinecone.UpsertVector{
			ID:     strconv.FormatInt(int64(msg.Id), 10),
			Values: msg.Emb,
			Metadata: map[string]string{
				"title": msg.Title,
				"langs": strconv.FormatInt(int64(msg.Lang), 10),
			},
		})
		if maxID < msg.Id {
			maxID = msg.Id
		}

		if len(upsert.Vectors) >= batchSize {
			select {
			case <-tc.closed:
				return
			case tc.upsertCh <- *upsert:
			}
			tc.wg.Add(1)
			go tc.doneUpsert(maxID, upsert)
			upsert = &pinecone.UpsertRequest{
				Namespace: tc.Namespace,
				Done:      make(chan struct{}),
			}
		}
	}

	if len(upsert.Vectors) >= 0 {
		select {
		case <-tc.closed:
			return
		case tc.upsertCh <- *upsert:
		}
		tc.wg.Add(1)
		go tc.doneUpsert(maxID, upsert)
		upsert = &pinecone.UpsertRequest{
			Namespace: tc.Namespace,
			Done:      make(chan struct{}),
		}
	}
}

func (tc *TestCase) doneUpsert(maxID int32, req *pinecone.UpsertRequest) {
	defer tc.wg.Done()
	select {
	case <-req.Done:
	case <-tc.closed:
		return
	}

	select {
	case tc.queryCh <- pinecone.QueryRequest{
		ID:        req.Vectors[0].ID,
		Namespace: tc.Namespace,
		TopK:      100,
		Vector:    req.Vectors[0].Values,
	}:
	case <-tc.closed:
		return
	}

	for {
		idNow := tc.maxID.Load()
		if idNow < maxID {
			if ok := tc.maxID.CompareAndSwap(idNow, maxID); ok {
				return
			}
		} else {
			return
		}
	}
}

func (tc *TestCase) Close() {
	close(tc.closed)
	tc.wg.Wait()
	close(tc.deleteCh)
	close(tc.queryCh)
	close(tc.upsertCh)
}

func (tc *TestCase) DeleteChan(limit uint64) <-chan pinecone.DeleteRequest {
	ch := make(chan pinecone.DeleteRequest, 1)
	go func() {
		defer close(ch)
		count := 0
		for req := range tc.deleteCh {
			ch <- req
			count += len(req.IDs)
			if count >= int(limit) {
				break
			}
		}
	}()
	return ch
}

func (tc *TestCase) UpsertChan(limit int) <-chan pinecone.UpsertRequest {
	ch := make(chan pinecone.UpsertRequest, 1)
	go func() {
		defer close(ch)
		count := 0
		for req := range tc.upsertCh {
			ch <- req
			count += len(req.Vectors)
			if count >= int(limit) {
				break
			}
		}
	}()
	return ch
}

func (tc *TestCase) QueryChan(limit int) <-chan pinecone.QueryRequest {
	ch := make(chan pinecone.QueryRequest, 1)
	go func() {
		defer close(ch)
		count := 0
		for req := range tc.queryCh {
			ch <- req
			count++
			if count >= int(limit) {
				break
			}
		}
	}()
	return ch
}
