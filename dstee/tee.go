package dstee

import (
	"context"
	ds "github.com/ipfs/go-datastore"
)

type Operation string

const (
	Put    Operation = "put"
	Delete Operation = "del"
)

type BatchItem struct {
	Key   ds.Key
	Value []byte
}

type Tee interface {
	String() string
	OnPut(key ds.Key, value []byte)
	OnDelete(key ds.Key)
	OnBatch(puts []BatchItem, deletes []ds.Key)
}

type DSTee struct {
	ds.Batching
	Tee Tee
}

func (t *DSTee) Put(ctx context.Context, key ds.Key, value []byte) error {
	if err := t.Batching.Put(ctx, key, value); err != nil {
		return err
	}
	t.Tee.OnPut(key, value)
	return nil
}

func (t *DSTee) Delete(ctx context.Context, key ds.Key) error {
	if err := t.Batching.Delete(ctx, key); err != nil {
		return err
	}
	t.Tee.OnDelete(key)
	return nil
}

func (t *DSTee) Batch(ctx context.Context) (ds.Batch, error) {
	b, err := t.Batching.Batch(ctx)
	if err != nil {
		return nil, err
	}
	return &DSTeeBatch{Batch: b, Tee: t.Tee}, nil
}

type DSTeeBatch struct {
	Batch   ds.Batch
	Tee     Tee
	puts    []BatchItem
	deletes []ds.Key
}

func (b *DSTeeBatch) Put(ctx context.Context, key ds.Key, value []byte) error {
	if err := b.Batch.Put(ctx, key, value); err != nil {
		return err
	}
	b.puts = append(b.puts, BatchItem{key, value})
	return nil
}

func (b *DSTeeBatch) Delete(ctx context.Context, key ds.Key) error {
	if err := b.Batch.Delete(ctx, key); err != nil {
		return err
	}
	b.deletes = append(b.deletes, key)
	return nil
}

func (b *DSTeeBatch) Commit(ctx context.Context) error {
	if err := b.Batch.Commit(ctx); err != nil {
		return err
	}
	b.Tee.OnBatch(b.puts, b.deletes)
	return nil
}

func (b *DSTeeBatch) Reset() {
	b.puts = b.puts[:0]
	b.deletes = b.deletes[:0]
}
