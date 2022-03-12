package eth2peerstore

import (
	"context"
	"encoding/json"
	"github.com/ethereum/go-ethereum/p2p/enode"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/protolambda/go-eth2-peerstore/dstee"
	"github.com/protolambda/zrnt/eth2/beacon/common"
	"time"
)

// IdentifyBook exposes the peer libp2p-identify info.
// Libp2p stores this data in the misc-data book
type IdentifyBook interface {
	ProtocolVersion(ctx context.Context, id peer.ID) (string, error)
	UserAgent(ctx context.Context, id peer.ID) (string, error)
}

type ENRBook interface {
	// Updates the ENR, if it is has a higher sequence number
	UpdateENRMaybe(ctx context.Context, id peer.ID, n *enode.Node) (updated bool, err error)

	// find the latest enr for the given peer.
	LatestENR(ctx context.Context, id peer.ID) (n *enode.Node, err error)
}

type StatusBook interface {
	// Status retrieves the peer status, and may be nil if there is no status
	Status(context.Context, peer.ID) (*common.Status, error)
	// RegisterStatus updates the status of the peer
	RegisterStatus(context.Context, peer.ID, common.Status) error
}

type MetadataBook interface {
	Metadata(context.Context, peer.ID) (*common.MetaData, error)
	ClaimedSeq(context.Context, peer.ID) (seq common.SeqNr, err error)
	RegisterSeqClaim(ctx context.Context, id peer.ID, seq common.SeqNr) (newer bool, err error)
	RegisterMetaFetch(context.Context, peer.ID) (uint64, error)
	RegisterMetadata(ctx context.Context, id peer.ID, md common.MetaData) (newer bool, err error)
}

type PeerAllData struct {
	PeerID peer.ID  `json:"peer_id"`
	NodeID enode.ID `json:"node_id"`
	Pubkey string   `json:"pubkey"`

	Addrs     []string `json:"addrs,omitempty"`
	Protocols []string `json:"protocols,omitempty"`

	Latency time.Duration `json:"latency,omitempty"`

	UserAgent       string `json:"user_agent,omitempty"`
	ProtocolVersion string `json:"protocol_version,omitempty"`

	ForkDigest      *common.ForkDigest `json:"enr_fork_digest,omitempty"`
	NextForkVersion *common.Version    `json:"enr_next_fork_version,omitempty"`
	NextForkEpoch   *common.Epoch      `json:"enr_next_fork_epoch,omitempty"`

	Attnets *common.AttnetBits `json:"enr_attnets,omitempty"`

	// Metadata with highest sequence number
	MetaData *common.MetaData `json:"metadata,omitempty"`
	// Highest claimed seq nr, we may not have the actual corresponding metadata yet.
	ClaimedSeq common.SeqNr `json:"claimed_seq,omitempty"`
	// Latest status
	Status *common.Status `json:"status,omitempty"`
	// Latest ENR
	ENR *enode.Node `json:"enr,omitempty"`
}

func (p *PeerAllData) String() string {
	if p == nil {
		return "no data available"
	} else {
		dat, err := json.MarshalIndent(p, "", "  ")
		if err != nil {
			return "failed to format peer data"
		}
		return string(dat)
	}
}

type AllDataGetter interface {
	GetAllData(ctx context.Context, id peer.ID) (*PeerAllData, error)
}

type TeedDatastore interface {
	// AddTee registers a tee, and returns true if it was already registered
	AddTee(tee dstee.Tee) (exists bool)
	// RmTee registers a tee, and returns true if it was registered before unregistering it
	RmTee(tee dstee.Tee) (exists bool)
	// ListTees lists all the tees
	ListTees() (out []dstee.Tee)
}

type ExtendedPeerstore interface {
	TeedDatastore
	Datastore() ds.Batching
	peerstore.Peerstore
	StatusBook
	MetadataBook
	ENRBook
	AllDataGetter
	// TODO: maybe track when we've last been connected to a peer?
}
