module github.com/ayachain/go-aya-alvm

require (
	github.com/chzyer/logex v1.1.10 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/chzyer/test v0.0.0-20180213035817-a1ea475d72b1 // indirect
	github.com/ipfs/go-cid v0.0.2
	github.com/ipfs/go-datastore v0.0.5
	github.com/ipfs/go-ipfs v0.0.0-00010101000000-000000000000
	github.com/ipfs/go-ipfs-api v0.0.1
	github.com/ipfs/go-ipfs-cmds v0.0.8
	github.com/ipfs/go-ipfs-config v0.0.4
	github.com/ipfs/go-ipfs-files v0.0.3
	github.com/ipfs/go-ipfs-flags v0.0.1 // indirect
	github.com/ipfs/go-ipfs-util v0.0.1
	github.com/ipfs/go-ipld-format v0.0.2
	github.com/ipfs/go-log v0.0.1
	github.com/ipfs/go-merkledag v0.0.3
	github.com/ipfs/go-metrics-prometheus v0.0.2
	github.com/ipfs/go-mfs v0.0.7
	github.com/ipfs/go-unixfs v0.0.6
	github.com/ipfs/interface-go-ipfs-core v0.0.8
	google.golang.org/appengine v1.4.0 // indirect
)

replace github.com/ipfs/go-ipfs => ../../ipfs/go-ipfs

go 1.12
