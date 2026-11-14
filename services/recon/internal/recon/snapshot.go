package recon

import (
	"encoding/json/v2"
	"fmt"
	"io"
	"time"

	"github.com/palemoky/oppenheimer/pkg/merkle"
)

// snapshotVersion 是快照文件格式的版本。
const snapshotVersion = 1

// SnapshotFile 是 archive 的快照：归档之前存下的“热库加冷库”的树，归档之后拿来比。
//
// 格式是一个 JSON 对象：创建时间、水位线、每个比对的名字、根摘要和树（merkle.Tree.MarshalBinary，
// JSON 里是 base64）。比对时用这里记的水位线，不用当时的时间重新算：水位线变了，两次覆盖的订单就不同，
// 新落进来的订单会被报成差异。
type SnapshotFile struct {
	Version   int             `json:"version"`
	CreatedAt time.Time       `json:"created_at"`
	Bound     int64           `json:"bound"`
	Trees     []SnapshotEntry `json:"trees"`
}

// SnapshotEntry 是一个比对的树。
type SnapshotEntry struct {
	Name string `json:"name"`
	Root string `json:"root"`
	Rows uint64 `json:"rows"`
	Tree []byte `json:"tree"`
}

// Add 加入一个比对的树。
func (f *SnapshotFile) Add(name string, t *merkle.Tree) error {
	b, err := t.MarshalBinary()
	if err != nil {
		return fmt.Errorf("recon: marshal tree %s: %w", name, err)
	}
	r := t.Root()
	f.Trees = append(f.Trees, SnapshotEntry{Name: name, Root: r.String(), Rows: r.Count, Tree: b})
	return nil
}

// Write 写出快照。
func (f *SnapshotFile) Write(w io.Writer) error {
	f.Version = snapshotVersion
	if err := json.MarshalWrite(w, f); err != nil {
		return fmt.Errorf("recon: write snapshot: %w", err)
	}
	return nil
}

// ReadSnapshot 读回快照，核对每棵树的根摘要与记录的相同，返回按比对名字索引的树。
func ReadSnapshot(r io.Reader) (*SnapshotFile, map[string]*merkle.Tree, error) {
	var f SnapshotFile
	if err := json.UnmarshalRead(r, &f); err != nil {
		return nil, nil, fmt.Errorf("recon: read snapshot: %w", err)
	}
	if f.Version != snapshotVersion {
		return nil, nil, fmt.Errorf("recon: snapshot version %d, want %d", f.Version, snapshotVersion)
	}
	trees := map[string]*merkle.Tree{}
	for _, e := range f.Trees {
		var t merkle.Tree
		if err := t.UnmarshalBinary(e.Tree); err != nil {
			return nil, nil, fmt.Errorf("recon: snapshot tree %s: %w", e.Name, err)
		}
		if got := t.Root().String(); got != e.Root {
			return nil, nil, fmt.Errorf("recon: snapshot tree %s has root %s, recorded %s", e.Name, got, e.Root)
		}
		if _, dup := trees[e.Name]; dup {
			return nil, nil, fmt.Errorf("recon: snapshot has two trees named %s", e.Name)
		}
		trees[e.Name] = &t
	}
	return &f, trees, nil
}
