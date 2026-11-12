package cache

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ProtoCodec 用 proto 的二进制编码序列化消息。M 是消息的指针类型，例如 *orderv1.Order。
type ProtoCodec[M proto.Message] struct{}

// Marshal 实现 Codec。
func (ProtoCodec[M]) Marshal(m M) ([]byte, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("cache: marshal proto: %w", err)
	}
	return b, nil
}

// Unmarshal 实现 Codec。
func (ProtoCodec[M]) Unmarshal(data []byte) (M, error) {
	var zero M
	// M 是指针类型，零值是 nil。通过反射拿到消息类型再新建一个空消息。
	m, ok := zero.ProtoReflect().New().Interface().(M)
	if !ok {
		return zero, fmt.Errorf("cache: %T is not a concrete proto message", zero)
	}
	if err := proto.Unmarshal(data, m); err != nil {
		return zero, fmt.Errorf("cache: unmarshal proto: %w", err)
	}
	return m, nil
}
