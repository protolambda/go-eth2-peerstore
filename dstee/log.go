package dstee

import (
	"encoding/hex"
	"github.com/ethereum/go-ethereum/log"
	ds "github.com/ipfs/go-datastore"
)

type LogTee struct {
	Log log.Logger
	// TODO: maybe an option to customize used log level?
}

func (t *LogTee) String() string {
	return "Logger Tee"
}

func (t *LogTee) OnPut(key ds.Key, value []byte) {
	t.Log.Info("put",
		"op", "put",
		"key", key.String(),
		"value", hex.EncodeToString(value))
}

func (t *LogTee) OnDelete(key ds.Key) {
	t.Log.Info("delete",
		"op", "del",
		"key", key.String())
}

func (t *LogTee) OnBatch(puts []BatchItem, deletes []ds.Key) {
	for _, p := range puts {
		t.OnPut(p.Key, p.Value)
	}
	for _, d := range deletes {
		t.OnDelete(d)
	}
}
