# `go-eth2-peerstore`

```shell
go get github.com/protolambda/go-eth2-peerstore
```

This extends the [Libp2p peerstore](https://github.com/libp2p/go-libp2p-peerstore), adding:
- Address conversion utils in `addrutil`
- Eth2 `Status`, `Metadata` (with seqnr handling) support, building on [ZRNT](https://github.com/protolambda/zrnt/) types
- Eth2 ENR support
- Interface to access the libp2p Identify info (default libp2p does not expose it)
- List function to get a collection of detailed info of a peer, to not have to query all separate peerstore components.
- Everything can be persisted, with the same datastore abstraction as the native libp2p peerstore uses.
- Peerstore tee: sync any changes made to the libp2p keystore with an external source. Logging and CSV tee types included as examples.

## Getting started

```go
import (
    "fmt"
    ds "github.com/ipfs/go-datastore"
    "github.com/ipfs/go-datastore/sync"
    "github.com/libp2p/go-libp2p-peerstore/pstoreds"
    "github.com/protolambda/go-eth2-peerstore/dstrack"
)

func main() {
    store := sync.MutexWrap(ds.NewMapDatastore())  // or your favorite persisted libp2p datastore (Leveldb, badger, etc.)
    peerstore, err := dstrack.NewExtendedPeerstore(context.Background(), store, pstoreds.DefaultOpts())
    if err != nil {
    	panic(err)
    }
    fmt.Println(peerstore.GetAllData("peerid....."))
}
```

## License

MIT. See `LICENSE` file.

