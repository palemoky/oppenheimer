package kafkax

import "testing"

// Kafka 自己的单元测试（UtilsTest.testMurmur2）里的几组值：这份 murmur2 和 Java 客户端的是同一个函数。
func TestMurmur2MatchesKafka(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]int32{
		"21":                         -973932308,
		"foobar":                     -790332482,
		"a-little-bit-long-string":   -985981536,
		"a-little-bit-longer-string": -1486304829,
		"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
		"abc": 479470107,
	} {
		if got := int32(murmur2([]byte(in))); got != want { //nolint:gosec // 按位重新解释成有符号数，和 Java 的 int 对齐
			t.Errorf("murmur2(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestPartitionOfBounds(t *testing.T) {
	t.Parallel()
	if got := PartitionOf([]byte("k"), 0); got != 0 {
		t.Fatalf("PartitionOf with 0 partitions = %d, want 0", got)
	}
	for i := range 1000 {
		key := []byte{byte(i), byte(i >> 8), 'x'}
		if p := PartitionOf(key, 7); p < 0 || p >= 7 {
			t.Fatalf("PartitionOf(%v, 7) = %d, out of range", key, p)
		}
	}
}
