package change

import "sync"

// subscriptionRegistry owns the table-keyed Subscription map and the lock
// that protects it. Encapsulation matters: every map access on Client used
// to spell out a snapshot dance under c.Lock, and one forgotten release
// caused the data race fixed in this package. With the registry, the
// underlying map is unreachable except through these methods, each of
// which takes the right lock for the operation.
//
// The lock is an RWMutex because reads (Get / Snapshot) dominate writes
// (Add only fires once per table at startup). Subscription implementations
// have their own internal synchronization, so callers may operate on a
// snapshot without holding the registry lock.
type subscriptionRegistry struct {
	mu   sync.RWMutex
	subs map[string]Subscription
}

func newSubscriptionRegistry() *subscriptionRegistry {
	return &subscriptionRegistry{subs: make(map[string]Subscription)}
}

// Add inserts sub under key. Returns false if a subscription with this key
// already exists; the existing entry is left untouched and sub is discarded
// by the caller.
//
// sub must be fully constructed before it is added — in particular its
// internal sync.Cond must already be wired (NewBufferedSubscription does
// this). The registry only stores the value, so it is agnostic to the
// concrete Subscription implementation.
func (r *subscriptionRegistry) Add(key string, sub Subscription) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.subs[key]; exists {
		return false
	}
	r.subs[key] = sub
	return true
}

// Get returns the subscription for key, if any.
func (r *subscriptionRegistry) Get(key string) (Subscription, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sub, ok := r.subs[key]
	return sub, ok
}

// Snapshot returns a slice of all current subscriptions. The returned slice
// is safe to iterate without holding the registry lock — Subscription
// implementations have their own synchronization. A subscription Added
// after the snapshot is taken will not appear in the result; that race is
// intentional and acceptable for every caller in this package.
func (r *subscriptionRegistry) Snapshot() []Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Subscription, 0, len(r.subs))
	for _, s := range r.subs {
		out = append(out, s)
	}
	return out
}
