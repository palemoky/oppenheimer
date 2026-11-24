//go:build !faults

package service

import (
	"context"

	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

// listRows 从索引表读一页。
func (s *Service) listRows(ctx context.Context, q store.ListQuery) ([]store.Row, error) {
	return s.store.List(ctx, q)
}
