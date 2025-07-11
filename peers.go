package Orbit_Cache

import "context"

// PeerPicker 的 PickPeer() 方法用于根据传入的 key 选择相应节点 Peer
type PeerPicker interface {
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error
}

// Peer Get方法用于从对应 group 查找缓存值 PeerGetter 对应HTTP 客户端
type Peer interface {
	Get(group string, key string) ([]byte, error)
	set(ctx context.Context, group string, key string, value []byte) error
	Delete(group string, key string) (bool, error)
	Close() error
}
