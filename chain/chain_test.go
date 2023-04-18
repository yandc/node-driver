package chain

import "testing"

func BenchmarkArrayRace(b *testing.B) {
	arr := make([]uint64, 10)
	s := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			for range s {
				v := arr[idx]
				v++
				arr[idx] = v
			}
		}(i)
	}
	for i := 0; i < b.N; i++ {
		s <- true
	}
}
