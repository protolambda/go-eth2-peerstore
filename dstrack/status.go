package dstrack

import (
	"bytes"
	"context"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/protolambda/go-eth2-peerstore"
	"github.com/protolambda/zrnt/eth2/beacon/common"
	"github.com/protolambda/ztyp/codec"
	"sync"
)

var statusSuffix = ds.NewKey("/status")

type dsStatusBook struct {
	ds ds.Datastore
	// cache status objects to not load/store them all the time
	data sync.Map
}

var _ eth2peerstore.StatusBook = (*dsStatusBook)(nil)

func NewStatusBook(store ds.Datastore) (*dsStatusBook, error) {
	return &dsStatusBook{ds: store}, nil
}

func (sb *dsStatusBook) loadStatus(ctx context.Context, p peer.ID) (*common.Status, error) {
	key := peerIdToKey(eth2Base, p).Child(statusSuffix)
	value, err := sb.ds.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error while fetching status from datastore for peer %s: %s\n", p.Pretty(), err)
	}
	var status common.Status
	if err := status.Deserialize(codec.NewDecodingReader(bytes.NewReader(value), uint64(len(value)))); err != nil {
		return nil, fmt.Errorf("failed parse status bytes from datastore: %v", err)
	}
	// cache it
	sb.data.Store(p, &status)
	return &status, nil
}

func (sb *dsStatusBook) storeStatus(ctx context.Context, p peer.ID, st *common.Status) error {
	key := peerIdToKey(eth2Base, p).Child(statusSuffix)
	size := st.FixedLength()
	out := bytes.NewBuffer(make([]byte, 0, size))
	if err := st.Serialize(codec.NewEncodingWriter(out)); err != nil {
		return fmt.Errorf("failed encode status bytes for datastore: %v", err)
	}
	if err := sb.ds.Put(ctx, key, out.Bytes()); err != nil {
		return fmt.Errorf("failed to store status: %v", err)
	}
	return nil
}

func (sb *dsStatusBook) Status(ctx context.Context, id peer.ID) (*common.Status, error) {
	dat, loaded := sb.data.Load(id)
	if loaded {
		return dat.(*common.Status), nil
	} else {
		// lazy-load status into the db
		return sb.loadStatus(ctx, id)
	}
}

// TODO: option to remove Status from the DB?

// RegisterStatus updates latest peer status
func (sb *dsStatusBook) RegisterStatus(ctx context.Context, id peer.ID, st common.Status) error {
	sb.data.Store(id, &st)
	// Try persist it to the store
	return sb.storeStatus(ctx, id, &st)
}

func (sb *dsStatusBook) flush(ctx context.Context) error {
	var clErr error
	// store all statuses to datastore before exiting
	sb.data.Range(func(key, value interface{}) bool {
		id := key.(peer.ID)
		st := value.(*common.Status)
		if err := sb.storeStatus(ctx, id, st); err != nil {
			clErr = err
			return false
		}
		return true
	})
	return clErr
}

func (sb *dsStatusBook) Close() error {
	return sb.flush(context.Background())
}
