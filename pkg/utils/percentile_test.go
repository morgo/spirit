package utils

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPercentileNearestRank(t *testing.T) {
	require.Equal(t, time.Duration(0), Percentile([]time.Duration(nil), 50))
	require.Equal(t, time.Duration(0), Percentile([]time.Duration{}, 90))

	one := []time.Duration{7 * time.Millisecond}
	require.Equal(t, 7*time.Millisecond, Percentile(one, 50))
	require.Equal(t, 7*time.Millisecond, Percentile(one, 90))

	two := []time.Duration{1 * time.Millisecond, 2 * time.Millisecond}
	require.Equal(t, 1*time.Millisecond, Percentile(two, 50))
	require.Equal(t, 2*time.Millisecond, Percentile(two, 90))

	// 1..100ms: nearest-rank p50 = 50ms, p90 = 90ms
	hundred := make([]time.Duration, 100)
	for i := range hundred {
		hundred[i] = time.Duration(i+1) * time.Millisecond
	}
	require.Equal(t, 50*time.Millisecond, Percentile(hundred, 50))
	require.Equal(t, 90*time.Millisecond, Percentile(hundred, 90))
}

func TestPercentileRankIndex(t *testing.T) {
	// Nearest-rank means index = ceil(n*p/100) - 1. Small n is where
	// percentile definitions disagree most, and a checksum pass over a small
	// table has small n. Values equal their index so the result names it.
	for _, c := range []struct {
		n, p, want int
	}{
		{1, 50, 0}, {1, 90, 0}, // a single sample is every percentile
		{2, 50, 0}, {2, 90, 1},
		{3, 50, 1}, {3, 90, 2},
		{10, 50, 4}, {10, 90, 8},
		{100, 50, 49}, {100, 90, 89},
		{7, 50, 3}, {7, 90, 6},
		// n*p/100 below 1 must still name a real sample rather than underflowing.
		{5, 1, 0},
		{5, 100, 4},
	} {
		sorted := make([]uint64, c.n)
		for i := range sorted {
			sorted[i] = uint64(i)
		}
		require.Equal(t, uint64(c.want), Percentile(sorted, c.p), "Percentile(n=%d, p=%d)", c.n, c.p)
	}
}

func TestPercentileP90MatchesSecondFromTop(t *testing.T) {
	// The dynamic chunker used to take "the value len/10 from the top" of its
	// chunk history as a p90. That index, n-1-n/10, is exactly the
	// nearest-rank p90 for every n, so replacing it changed no chunk sizing.
	for n := 1; n <= 1000; n++ {
		sorted := make([]int, n)
		for i := range sorted {
			sorted[i] = i
		}
		require.Equal(t, n-1-n/10, Percentile(sorted, 90), "n=%d", n)
	}
}

func TestPercentileUnsortedHistory(t *testing.T) {
	times := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		1 * time.Second,
		3 * time.Second,
		10 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
	}
	slices.Sort(times)
	require.Equal(t, 3*time.Second, Percentile(times, 90))
}
