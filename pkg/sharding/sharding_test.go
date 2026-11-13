package sharding

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/palemoky/Oppenheimer/pkg/idgen"
)

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	for _, cfg := range []Config{
		{Slots: 0, DBs: 1},
		{Slots: 6, DBs: 2},
		{Slots: 128, DBs: 2},
		{Slots: 8, DBs: 0},
		{Slots: 8, DBs: 3},
		{Slots: 8, DBs: 16},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("New(%+v) succeeded, want error", cfg)
		}
	}
}

// 基因法的核心性质：订单号和它的用户落在同一个 slot、同一个库。
func TestOrderAndUserShareSlot(t *testing.T) {
	t.Parallel()
	g, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []Config{{8, 2}, {8, 1}, {64, 8}, {1, 1}} {
		r, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for range 2000 {
			userID := rand.Int64N(1 << 50)
			orderID, err := g.Next(userID)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := r.ForOrder(orderID), r.ForUser(userID); got != want {
				t.Fatalf("%+v: ForOrder(%d) = %d, ForUser(%d) = %d", cfg, orderID, got, userID, want)
			}
		}
	}
}

func TestSlotsOfPartitionsAllSlots(t *testing.T) {
	t.Parallel()
	r, err := New(Config{Slots: 8, DBs: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.SlotsOf(0); !slices.Equal(got, []int{0, 2, 4, 6}) {
		t.Errorf("SlotsOf(0) = %v", got)
	}
	if got := r.SlotsOf(1); !slices.Equal(got, []int{1, 3, 5, 7}) {
		t.Errorf("SlotsOf(1) = %v", got)
	}
	seen := make(map[int]int)
	for db := range r.DBs() {
		for _, s := range r.SlotsOf(db) {
			if r.DB(s) != db {
				t.Errorf("slot %d listed under db %d but DB() = %d", s, db, r.DB(s))
			}
			seen[s]++
		}
	}
	for s := range r.Slots() {
		if seen[s] != 1 {
			t.Errorf("slot %d appears %d times", s, seen[s])
		}
	}
}

// 连续的用户号均匀地落在各个 slot 上。
func TestSequentialUsersSpreadEvenly(t *testing.T) {
	t.Parallel()
	r, err := New(Config{Slots: 8, DBs: 2})
	if err != nil {
		t.Fatal(err)
	}
	counts := make([]int, r.Slots())
	for u := range int64(800) {
		counts[r.ForUser(u+1000)]++
	}
	for s, n := range counts {
		if n != 100 {
			t.Errorf("slot %d got %d users, want 100", s, n)
		}
	}
}

func TestTable(t *testing.T) {
	t.Parallel()
	if got := Table("orders", 3); got != "orders_3" {
		t.Errorf("Table = %q", got)
	}
}
