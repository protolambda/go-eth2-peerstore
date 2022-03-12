package dstee

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	"strconv"
	"sync"
	"time"
)

type CSVTee struct {
	Name  string
	CSV   *csv.Writer
	OnErr func(op Operation, key ds.Key, value []byte, err error)
	sync.Mutex
}

func (t *CSVTee) String() string {
	return fmt.Sprintf("CSV Tee: %s", t.Name)
}

func (t *CSVTee) OnPut(key ds.Key, value []byte) {
	t.Lock()
	defer t.Unlock()
	if err := t.CSV.Write([]string{
		string(Put),
		strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10),
		key.String(),
		hex.EncodeToString(value), // TODO: maybe format some special sub paths, e.g. IPs, utf-8 values, etc.
	}); err != nil {
		if t.OnErr != nil {
			t.OnErr(Put, key, value, err)
		}
	} else {
		t.CSV.Flush()
	}
}

func (t *CSVTee) OnDelete(key ds.Key) {
	t.Lock()
	defer t.Unlock()
	if err := t.CSV.Write([]string{
		string(Delete),
		strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10),
		key.String(),
		"",
	}); err != nil {
		if t.OnErr != nil {
			t.OnErr(Delete, key, nil, err)
		}
	} else {
		t.CSV.Flush()
	}
}

func (t *CSVTee) OnBatch(puts []BatchItem, deletes []ds.Key) {
	t.Lock()
	defer t.Unlock()
	m := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	for i, p := range puts {
		if err := t.CSV.Write([]string{
			string(Put),
			m,
			p.Key.String(),
			hex.EncodeToString(p.Value),
		}); err != nil {
			if t.OnErr != nil {
				t.OnErr(Put, p.Key, p.Value, fmt.Errorf("failed to write batch put entry %d: %w", i, err))
			}
		}
	}
	for i, d := range deletes {
		if err := t.CSV.Write([]string{
			string(Delete),
			m,
			d.String(),
			"",
		}); err != nil {
			if t.OnErr != nil {
				t.OnErr(Delete, d, nil, fmt.Errorf("failed to write batch delete entry %d: %w", i, err))
			}
		}
	}
	t.CSV.Flush()
}
