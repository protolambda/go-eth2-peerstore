package dstrack

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/protolambda/go-eth2-peerstore"
	"github.com/protolambda/go-eth2-peerstore/addrutil"
)

// enrs are stored under the /eth2/<peer id>/enr path, and stored in string representation
var enrSuffix = ds.NewKey("/enr")

var validSchemesForDB = enr.SchemeMap{
	"v4":   enode.V4ID{},
	"null": enode.NullID{},
}

type dsENRBook struct {
	ds ds.Datastore
}

var _ eth2peerstore.ENRBook = (*dsENRBook)(nil)

func NewENRBook(store ds.Datastore) (*dsENRBook, error) {
	return &dsENRBook{ds: store}, nil
}

func (eb *dsENRBook) loadEnr(ctx context.Context, p peer.ID) (*enode.Node, error) {
	key := peerIdToKey(eth2Base, p).Child(enrSuffix)
	value, err := eb.ds.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error while fetching enr from datastore for peer %s: %s\n", p.Pretty(), err)
	}
	rec, err := addrutil.ParseEnr(string(value))
	if err != nil {
		return nil, fmt.Errorf("retrieved enr could not be parsed: %v", err)
	}
	return enode.New(validSchemesForDB, rec)
}

func (eb *dsENRBook) storeEnr(ctx context.Context, p peer.ID, n *enode.Node) error {
	key := peerIdToKey(eth2Base, p).Child(enrSuffix)
	if err := eb.ds.Put(ctx, key, []byte(n.String())); err != nil {
		return fmt.Errorf("failed to store enr: %v", err)
	}
	return nil
}

// Update the record tracking of the peer,
// return updated=true if the node is new, or it overrides a previously seen node (by higher seq nr).
// and return eth2 and attnet data, if any.
func (eb *dsENRBook) UpdateENRMaybe(ctx context.Context, id peer.ID, n *enode.Node) (updated bool, err error) {
	old, err := eb.loadEnr(ctx, id)
	if err != nil || old.Seq() < n.Seq() {
		if err := eb.storeEnr(ctx, id, n); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (eb *dsENRBook) LatestENR(ctx context.Context, id peer.ID) (n *enode.Node, err error) {
	return eb.loadEnr(ctx, id)
}
