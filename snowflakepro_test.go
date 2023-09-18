package snowflakepro

import (
	"math/rand"
	"testing"
)

func Test_Next(t *testing.T) {
	var s, _ = NewSnowflakePro(100, uint64(rand.Int63n(int64(MaxNonce))))
	for i := 0; i < 1000; i++ {
		id := s.Next()
		if id.Time() != s.tms {
			t.Fatal("time error")
		}

		if id.Nonce() != s.nonce {
			t.Fatal("nonce error", id.Nonce(), s.nonce)
		}

		if id.Node() != s.node {
			t.Fatal("node error")
		}

		if id.SN() != s.sn {
			t.Fatal("sn error")
		}
		t.Log(id)
	}
}

func Benchmark_next(b *testing.B) {
	var s, _ = NewSnowflakePro(65000, uint64(rand.Uint32()))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Next()
	}
}
