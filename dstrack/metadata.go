package dstrack

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/protolambda/go-eth2-peerstore"
	"github.com/protolambda/zrnt/eth2/beacon/common"
	"github.com/protolambda/ztyp/codec"
	"sync"
)

var (
	metadataSuffix = ds.NewKey("/metadata")
	claimSuffix    = ds.NewKey("/metadata_claim")
)

type dsMetadataBook struct {
	ds ds.Datastore
	// cache metadata objects to not load/store them all the time
	sync.RWMutex
	// Track metadata with highest sequence number
	metadatas map[peer.ID]common.MetaData
	// highest claimed seq nr, we may not have the actual corresponding metadata yet.
	claims map[peer.ID]common.SeqNr
	// Track how many times we have tried to ask them for metadata without getting an answer
	fetches map[peer.ID]uint64
}

var _ eth2peerstore.MetadataBook = (*dsMetadataBook)(nil)

func NewMetadataBook(store ds.Datastore) (*dsMetadataBook, error) {
	return &dsMetadataBook{
		ds:        store,
		metadatas: make(map[peer.ID]common.MetaData),
		claims:    make(map[peer.ID]common.SeqNr),
		fetches:   make(map[peer.ID]uint64),
	}, nil
}

func (mb *dsMetadataBook) loadMetadata(ctx context.Context, p peer.ID) (*common.MetaData, error) {
	key := peerIdToKey(eth2Base, p).Child(metadataSuffix)
	value, err := mb.ds.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error while fetching metadata from datastore for peer %s: %s\n", p.Pretty(), err)
	}
	var md common.MetaData
	if err := md.Deserialize(codec.NewDecodingReader(bytes.NewReader(value), uint64(len(value)))); err != nil {
		return nil, fmt.Errorf("failed parse metadata bytes from datastore: %v", err)
	}
	return &md, nil
}

func (mb *dsMetadataBook) storeMetadata(ctx context.Context, p peer.ID, md *common.MetaData) error {
	key := peerIdToKey(eth2Base, p).Child(metadataSuffix)
	size := md.FixedLength()
	out := bytes.NewBuffer(make([]byte, 0, size))
	if err := md.Serialize(codec.NewEncodingWriter(out)); err != nil {
		return fmt.Errorf("failed encode metadata bytes for datastore: %v", err)
	}
	if err := mb.ds.Put(ctx, key, out.Bytes()); err != nil {
		return fmt.Errorf("failed to store metadata: %v", err)
	}
	return nil
}

func (mb *dsMetadataBook) loadClaim(ctx context.Context, p peer.ID) (common.SeqNr, error) {
	key := peerIdToKey(eth2Base, p).Child(claimSuffix)
	value, err := mb.ds.Get(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("error while fetching claim seq nr from datastore for peer %s: %s\n", p.Pretty(), err)
	}
	claim := common.SeqNr(binary.LittleEndian.Uint64(value))
	return claim, nil
}

func (mb *dsMetadataBook) storeClaim(ctx context.Context, p peer.ID, claim common.SeqNr) error {
	key := peerIdToKey(eth2Base, p).Child(claimSuffix)
	var dat [8]byte
	binary.LittleEndian.PutUint64(dat[:], uint64(claim))
	if err := mb.ds.Put(ctx, key, dat[:]); err != nil {
		return fmt.Errorf("failed to store claim seq nr: %v", err)
	}
	return nil
}

func (mb *dsMetadataBook) Metadata(ctx context.Context, id peer.ID) (*common.MetaData, error) {
	mb.Lock()
	defer mb.Unlock()
	return mb.metadata(ctx, id)
}

func (mb *dsMetadataBook) metadata(ctx context.Context, id peer.ID) (*common.MetaData, error) {
	dat, ok := mb.metadatas[id]
	if !ok {
		md, err := mb.loadMetadata(ctx, id)
		if err != nil {
			return nil, err
		}
		mb.metadatas[id] = *md
		return md, nil
	}
	return &dat, nil
}

func (mb *dsMetadataBook) ClaimedSeq(ctx context.Context, id peer.ID) (seq common.SeqNr, err error) {
	mb.Lock()
	defer mb.Unlock()
	return mb.claimedSeq(ctx, id)
}

func (mb *dsMetadataBook) claimedSeq(ctx context.Context, id peer.ID) (seq common.SeqNr, err error) {
	dat, ok := mb.claims[id]
	if !ok {
		n, err := mb.loadClaim(ctx, id)
		if err != nil {
			return 0, err
		}
		mb.claims[id] = n
		return n, nil
	}
	return dat, nil
}

// RegisterSeqClaim updates the latest supposed seq nr of the peer
func (mb *dsMetadataBook) RegisterSeqClaim(ctx context.Context, id peer.ID, seq common.SeqNr) (newer bool, err error) {
	mb.Lock()
	defer mb.Unlock()
	dat, err := mb.claimedSeq(ctx, id)
	newer = err != nil || dat < seq
	if newer {
		mb.claims[id] = seq
		err = mb.storeClaim(ctx, id, seq)
	}
	return
}

// RegisterMetaFetch increments how many times we tried to get the peer metadata
// without satisfying answer, returning the counter.
func (mb *dsMetadataBook) RegisterMetaFetch(ctx context.Context, id peer.ID) (uint64, error) {
	mb.Lock()
	defer mb.Unlock()
	count, _ := mb.fetches[id]
	count += 1
	mb.fetches[id] = count
	return count, nil
}

// RegisterMetadata updates metadata, if newer than previous. Resetting ongoing fetch counter if it's new enough
func (mb *dsMetadataBook) RegisterMetadata(ctx context.Context, id peer.ID, md common.MetaData) (newer bool, err error) {
	mb.Lock()
	defer mb.Unlock()
	dat, err := mb.metadata(ctx, id)
	newer = dat == nil || err != nil || dat.SeqNumber < md.SeqNumber
	if newer {
		// will 0 if no claim
		claimed, _ := mb.claims[id]
		if md.SeqNumber >= claimed {
			// if it is newer or equal to best, we can reset the ongoing fetches
			mb.fetches[id] = 0
		}
		mb.metadatas[id] = md
		err := mb.storeMetadata(ctx, id, &md)
		if err != nil {
			return true, err
		}
		if md.SeqNumber > claimed {
			mb.claims[id] = md.SeqNumber
			err := mb.storeClaim(ctx, id, md.SeqNumber)
			return true, err
		}
	}
	return
}

func (mb *dsMetadataBook) flush(ctx context.Context) error {
	mb.RLock()
	defer mb.RUnlock()
	// store all claims to datastore before exiting
	for id, cl := range mb.claims {
		if err := mb.storeClaim(ctx, id, cl); err != nil {
			return err
		}
	}
	// store all metadatas to datastore before exiting
	for id, md := range mb.metadatas {
		if err := mb.storeMetadata(ctx, id, &md); err != nil {
			return err
		}
	}
	return nil
}

func (mb *dsMetadataBook) Close() error {
	return mb.flush(context.Background())
}
