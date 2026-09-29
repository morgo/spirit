package utils

import (
	"cmp"
	"slices"
)

// Percentile returns the p-th percentile (0 < p <= 100) of values, which may
// be in any order. It uses the nearest-rank method: the smallest value with at
// least p% of the samples at or below it, i.e. sorted[ceil(n*p/100) - 1]. It
// returns the zero value for an empty slice. A p outside (0, 100] is clamped:
// p <= 0 returns the smallest value and p > 100 the largest, so a bad p
// cannot panic.
//
// values is not modified: Percentile sorts a copy, so callers can pass a live
// history (such as a ring buffer) without it being reordered. The rank is
// computed in integer arithmetic so float rounding of n*p can never move it
// by one.
func Percentile[T cmp.Ordered](values []T, p int) T {
	if len(values) == 0 {
		var zero T
		return zero
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	rank := max((len(sorted)*p+99)/100, 1)
	return sorted[min(rank, len(sorted))-1]
}
