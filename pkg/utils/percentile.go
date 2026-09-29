package utils

import "cmp"

// Percentile returns the p-th percentile (0 < p <= 100) of sorted, which must
// already be in ascending order. It uses the nearest-rank method: the smallest
// value with at least p% of the samples at or below it, i.e.
// sorted[ceil(n*p/100) - 1]. It returns the zero value for an empty slice.
//
// The rank is computed in integer arithmetic so float rounding of n*p can
// never move it by one.
func Percentile[T cmp.Ordered](sorted []T, p int) T {
	if len(sorted) == 0 {
		var zero T
		return zero
	}
	rank := max((len(sorted)*p+99)/100, 1)
	return sorted[min(rank, len(sorted))-1]
}
