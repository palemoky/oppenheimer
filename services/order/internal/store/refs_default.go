//go:build !faults

package store

import (
	"context"
	"fmt"
)

// listRefs 在 slot 上执行 query，返回 (order_id, user_id) 的列表。src 的含义同 ListExpired。
func (s *Store) listRefs(ctx context.Context, slot int, src Source, query string, args ...any) ([]OrderRef, error) {
	rows, err := s.sourceOf(slot, src).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list orders in slot %d: %w", slot, err)
	}
	defer rows.Close()
	var refs []OrderRef
	for rows.Next() {
		var r OrderRef
		if err := rows.Scan(&r.OrderID, &r.UserID); err != nil {
			return nil, fmt.Errorf("scan order ref: %w", err)
		}
		refs = append(refs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate order refs: %w", err)
	}
	return refs, nil
}
