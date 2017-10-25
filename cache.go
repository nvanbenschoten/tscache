/*
 * Copyright (C) 2017 Andy Kimball
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tscache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/andy-kimball/arenaskl"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// RangeOptions are passed to AddRange to indicate the bounds of the range. By
// default, the "from" and "to" keys are inclusive. Setting these bit flags
// indicates that one or both is exclusive instead.
type rangeOptions int

const (
	// ExcludeFrom indicates that the range does not include the starting key.
	ExcludeFrom = 0x1

	// ExcludeTo indicates that the range does not include the ending key.
	ExcludeTo = 0x2
)

// NodeOptions are meta tags on skiplist nodes that indicate the status and role
// of that node in the cache. The options are bit flags that can be independently
// added and removed.
//
// Each key in the cache is associated with the latest read timestamp for that
// key. In addition, the cache maintains the latest read timestamp for the range
// of keys between itself and the next key that is present in the cache. This
// space between keys is called the "gap", and the timestamp for that range is
// called the "gap timestamp". Here is a simplified representation that would
// result after these ranges were added to an empty cache:
//   ["apple", "orange") = 200
//   ["kiwi", "raspberry"] = 100
//
//   "apple"    "orange"   "raspberry"
//   keyts=200  keyts=100  keyts=100
//   gapts=200  gapts=100  gapts=0
//
// That is, the range from apple (inclusive) to orange (exclusive) has a read
// timestamp of 200. The range from orange (inclusive) to raspberry (inclusive)
// has a read timestamp of 100. All other keys have a read timestamp of 0.
type nodeOptions int

const (
	// Initializing indicates that the node has been created, but is still in a
	// provisional state. Any key and gap timestamps are not final values, and
	// should not be used until initialization is complete.
	initializing = 0x1

	// HasKey indicates the node has an associated key timestamp. If this is not
	// set, then the key timestamp is assumed to be zero.
	hasKey = 0x2

	// HasGap indicates the node has an associated gap timestamp. If this is not
	// set, then the gap timestamp is assumed to be zero.
	hasGap = 0x4

	// UseMaxTs indicates that the cache's max value should be used in place of
	// the node's gap and key timestamps. This is used when the cache has filled
	// up and the value can no longer be set (but meta can).
	useMaxTs = 0x8
)

const (
	encodedTsSize    = 12
	encodedTxnIDSize = 16
	encodedValSize   = encodedTsSize + encodedTxnIDSize
)

// FixedCache maintains a skiplist based on a fixed-size arena. When the arena
// has filled up, it returns an error. At that point, a new fixed cache must be
// allocated and used instead.
type fixedCache struct {
	list        *arenaskl.Skiplist
	maxWallTime int64
	isFull      int32
}

// Cache efficiently tracks the latest logical time at which any key or range
// of keys has been accessed. Keys are binary values of any length, and times
// are represented as hybrid logical timestamps (see hlc package). The cache
// guarantees that the read timestamp of any given key or range will never
// decrease. In other words, if a lookup returns timestamp A and repeating the
// same lookup returns timestamp B, then B >= A.
//
// Add and lookup operations do not block or interfere with one another, which
// enables predictable operation latencies. Also, the impact of the cache on the
// GC is virtually nothing, even when the cache is very large. These properties
// are enabled by employing a lock-free skiplist implementation that uses an
// arena allocator. Skiplist nodes refer to one another by offset into the arena
// rather than by pointer, so the GC has very few objects to track.
type Cache struct {
	// Maximum size of the cache in bytes. When the cache fills, older entries
	// are discarded.
	size uint32

	// RotMutex synchronizes cache rotation with all other operations. The read
	// lock is acquired by the Add and Lookup operations. The write lock is
	// acquired only when the caches are rotated. Since that is very rare, the
	// vast majority of operations can proceed without blocking.
	rotMutex sync.RWMutex

	// The cache maintains two fixed-size skiplist caches. When the later cache
	// fills, it becomes the earlier cache, and the previous earlier cache is
	// discarded. In order to ensure that timestamps never decrease, the cache
	// maintains a floor timestamp, which is the minimum read timestamp that can
	// be returned by the lookup operations. When the earlier cache is discarded,
	// its current maximum read timestamp becomes the new floor timestamp for the
	// overall cache.
	later   *fixedCache
	earlier *fixedCache
	floorTs hlc.Timestamp
}

// Used when a TimestampValue has no corresponding TxnID.
var noTxnID uuid.UUID

// TimestampValue combines a timestamp with an optional txn ID.
type TimestampValue struct {
	ts    hlc.Timestamp
	txnID uuid.UUID
}

func NewTimestampValue(ts hlc.Timestamp, txnID uuid.UUID) TimestampValue {
	return TimestampValue{ts: ts, txnID: txnID}
}

func (tv TimestampValue) Ts() hlc.Timestamp { return tv.ts }
func (tv TimestampValue) TxnID() uuid.UUID  { return tv.txnID }

// New creates a new timestamp cache with the given maximum size.
func New(size uint32) *Cache {
	return NewWithFloor(size, hlc.Timestamp{})
}

// NewWithFloor creates a new timestamp cache with the given maximum size and
// the specified floor timestamp.
func NewWithFloor(size uint32, floor hlc.Timestamp) *Cache {
	// The earlier and later fixed caches are each 1/2 the size of the larger
	// cache.
	return &Cache{
		size:    size,
		later:   newFixedCache(size / 2),
		floorTs: floor,
	}
}

// Clear clears the cache and sets a new floor timestamp.
func (c *Cache) Clear(floor hlc.Timestamp) {
	c.rotMutex.Lock()
	defer c.rotMutex.Unlock()
	c.floorTs = floor
	c.later = newFixedCache(c.size / 2)
	c.earlier = nil
}

// FloorTS returns the floor timestamp.
func (c *Cache) FloorTS() hlc.Timestamp {
	c.rotMutex.RLock()
	defer c.rotMutex.RUnlock()
	return c.floorTs
}

// Add marks the a single key as having been read at the given timestamp. Once
// Add completes, future lookups of this key are guaranteed to return an equal
// or greater timestamp.
func (c *Cache) Add(key []byte, val TimestampValue) {
	c.AddRange(key, key, 0, val)
}

// AddRange marks the given range of keys (from, to) as having been read at the
// given timestamp. If some or all of the range was previously read at a higher
// timestamp, then the range is split into sub-ranges that are each marked with
// the maximum read timestamp for that sub-range. The starting and ending points
// of the range are inclusive by default, but can be excluded by passing the
// applicable range options. Once AddRange completes, future lookups at any point
// in the range are guaranteed to return an equal or greater timestamp.
func (c *Cache) AddRange(from, to []byte, opt rangeOptions, val TimestampValue) {
	if from == nil {
		panic("from key cannot be nil")
	}

	if to != nil {
		cmp := bytes.Compare(from, to)
		if cmp > 0 {
			// Starting key is after ending key, so range is zero length.
			return
		}

		if cmp == 0 {
			// Starting key is same as ending key, so just add single node.
			if opt == (ExcludeFrom | ExcludeTo) {
				// Both from and to keys are excluded, so range is zero length.
				return
			}

			// Just add the ending key.
			from = nil
			opt = 0
		}
	}

	for {
		// Try to add the range to the later cache.
		filledCache := c.addRange(from, to, opt, val)
		if filledCache == nil {
			break
		}

		// The cache was filled up, so rotate the caches and then try again.
		c.rotateCaches(filledCache)
	}
}

// LookupTimestamp returns the latest timestamp at which the given key was read.
// If this operation is repeated with the same key, it will always result in an
// equal or greater timestamp.
func (c *Cache) LookupTimestamp(key []byte) TimestampValue {
	return c.LookupTimestampRange(key, key, 0)
}

// LookupTimestampRange returns the latest timestamp of any key within the
// specified range. If this operation is repeated with the same range, it will
// always result in an equal or greater timestamp.
func (c *Cache) LookupTimestampRange(from, to []byte, opt rangeOptions) TimestampValue {
	val, _ := c.LookupTimestampRangeWithFlag(from, to, opt)
	return val
}

// LookupTimestampRangeWithFlag behaves like LookupTimestampRange, but also returns
// whether the lookup is returning the floor value.
func (c *Cache) LookupTimestampRangeWithFlag(from, to []byte, opt rangeOptions) (val TimestampValue, floor bool) {
	// Acquire the rotation mutex read lock so that the cache will not be rotated
	// while add or lookup operations are in progress.
	c.rotMutex.RLock()
	defer c.rotMutex.RUnlock()

	// First perform lookup on the later cache.
	val = c.later.lookupTimestampRange(from, to, opt)

	// Now perform same lookup on the earlier cache.
	if c.earlier != nil {
		// If later cache timestamp is greater than the max timestamp in the
		// earlier cache, then no need to do lookup at all.
		maxTs := hlc.Timestamp{WallTime: atomic.LoadInt64(&c.earlier.maxWallTime)}
		if val.ts.Less(maxTs) {
			val2 := c.earlier.lookupTimestampRange(from, to, opt)
			val, _, _ = ratchetTimestampValue(val, val2)
		}
	}

	// Return the higher timestamp from the two lookups.
	if !c.floorTs.Less(val.ts) {
		val = TimestampValue{ts: c.floorTs, txnID: noTxnID}
		floor = true
	}

	return val, floor
}

func (c *Cache) addRange(from, to []byte, opt rangeOptions, val TimestampValue) *fixedCache {
	// Acquire the rotation mutex read lock so that the cache will not be rotated
	// while add or lookup operations are in progress.
	c.rotMutex.RLock()
	defer c.rotMutex.RUnlock()

	// If floor ts is >= requested timestamp, then no need to perform a search
	// or add any records.
	if !c.floorTs.Less(val.ts) {
		return nil
	}

	var it arenaskl.Iterator
	it.Init(c.later.list)

	// Start by ensuring that the ending node has been created (unless "to" is
	// nil, in which case the range extends indefinitely). Do this before creating
	// the start node, so that the range won't extend past the end point during
	// the period between creating the two endpoints.
	var err error
	if to != nil {
		if (opt & ExcludeTo) == 0 {
			err = c.later.addNode(&it, to, val, hasKey)
		} else {
			err = c.later.addNode(&it, to, val, 0)
		}

		if err == arenaskl.ErrArenaFull {
			return c.later
		}
	}

	// If from is nil, then the "range" is just a single key.
	if from == nil {
		return nil
	}

	// Ensure that the starting node has been created.
	if (opt & ExcludeFrom) == 0 {
		err = c.later.addNode(&it, from, val, hasKey|hasGap)
	} else {
		err = c.later.addNode(&it, from, val, hasGap)
	}

	if err == arenaskl.ErrArenaFull {
		return c.later
	}

	// Seek to the node immediately after the "from" node.
	if !it.Valid() || bytes.Compare(it.Key(), from) != 0 {
		if it.Seek(from) {
			it.Next()
		}
	} else {
		it.Next()
	}

	// Now iterate forwards and ensure that all nodes between the start and
	// end (exclusive) have timestamps that are >= the range timestamp.
	if !c.later.ensureFloorTs(&it, to, val) {
		// Cache is filled up, so rotate caches and try again.
		return c.later
	}

	return nil
}

// RotateCaches makes the later cache the earlier cache, and then discards the
// earlier cache. The max timestamp of the earlier cache becomes the new floor
// timestamp, in order to guarantee that timestamp lookups never return decreasing
// values.
func (c *Cache) rotateCaches(filledCache *fixedCache) {
	c.rotMutex.Lock()
	defer c.rotMutex.Unlock()

	if filledCache != c.later {
		// Another thread already rotated the caches, so don't do anything more.
		return
	}

	// Max timestamp of the earlier cache becomes the new floor timestamp.
	if c.earlier != nil {
		newFloorTs := hlc.Timestamp{WallTime: atomic.LoadInt64(&c.earlier.maxWallTime)}
		if c.floorTs.Less(newFloorTs) {
			c.floorTs = newFloorTs
		}
	}

	// Make the later cache the earlier cache.
	c.earlier = c.later
	c.later = newFixedCache(c.size / 2)
}

func newFixedCache(size uint32) *fixedCache {
	return &fixedCache{list: arenaskl.NewSkiplist(arenaskl.NewArena(size))}
}

func (c *fixedCache) lookupTimestampRange(from, to []byte, opt rangeOptions) TimestampValue {
	if to != nil {
		cmp := bytes.Compare(from, to)
		if cmp > 0 {
			// Starting key is after ending key, so range is zero length.
			return TimestampValue{}
		}

		if cmp == 0 {
			// Starting key is same as ending key.
			if opt == (ExcludeFrom | ExcludeTo) {
				// Both from and to keys are excluded, so range is zero length.
				return TimestampValue{}
			}

			opt = 0
		}
	}

	var it arenaskl.Iterator
	it.Init(c.list)

	onKey := it.SeekForPrev(from)
	return c.scanForTimestamp(&it, from, to, opt, onKey)
}

func (c *fixedCache) addNode(it *arenaskl.Iterator, key []byte, val TimestampValue, opt nodeOptions) error {
	var arr [encodedValSize * 2]byte
	var keyVal, gapVal TimestampValue

	if (opt & hasKey) != 0 {
		keyVal = val
	}

	if (opt & hasGap) != 0 {
		gapVal = val
	}

	if !it.SeekForPrev(key) {
		// If the previous node has a gap timestamp that is >= than the new
		// timestamp, then there is no need to add another node, since its
		// timestamp would be the same as the gap timestamp.
		prevVal := c.scanForTimestamp(it, key, key, 0, false)
		if !prevVal.ts.Less(val.ts) {
			return nil
		}

		// Ratchet max timestamp before adding the node.
		c.ratchetMaxTimestamp(val.ts)

		// Ensure that a new node is created. It needs to stay in the
		// initializing state until the gap timestamp of its preceding node
		// has been found and used to ratchet this node's timestamps. During
		// the search for the gap timestamp, this node acts as a sentinel
		// for other ongoing operations - when they see this node they're
		// forced to stop and help complete its initialization before they
		// can continue.
		b, meta := c.encodeTimestampSet(arr[:0], keyVal, gapVal)
		err := it.Add(key, b, meta|initializing)
		if err == arenaskl.ErrArenaFull {
			atomic.StoreInt32(&c.isFull, 1)
			return err
		}

		if err == nil {
			// Add was successful, so finish initialization by scanning for
			// gap timestamp and using it to ratchet the new nodes' timestamps.
			c.scanForTimestamp(it, key, key, 0, true)
			return nil
		}

		// Another thread raced and added the node, so just ratchet its
		// timestamps instead.
	} else {
		if opt == 0 {
			// Don't need to set either key or gap ts, so done.
			return nil
		}
	}

	// Ratchet up the timestamps on the existing node, but don't clear the
	// initializing bit, since we don't have the gap timestamp from the previous
	// node. Leave finishing initialization to the thread that added the node, or
	// to a lookup thread that requires it.
	c.ratchetTimestampSet(it, keyVal, gapVal, false)

	return nil
}

func (c *fixedCache) ensureFloorTs(it *arenaskl.Iterator, to []byte, val TimestampValue) bool {
	for it.Valid() {
		if to != nil && bytes.Compare(it.Key(), to) >= 0 {
			break
		}

		if atomic.LoadInt32(&c.isFull) == 1 {
			// Cache is full, so stop iterating. The caller will then be able to
			// release the read lock and rotate the caches. Not doing this could
			// result in forcing all other operations to wait for this thread to
			// completely finish iteration. That could take a long time if this
			// range is very large.
			return false
		}

		// Don't clear the initialization bit, since we don't have the gap
		// timestamp from the previous node, and don't need an initialized node
		// for this operation anyway.
		c.ratchetTimestampSet(it, val, val, false)

		it.Next()
	}

	return true
}

// Cheat and just use the max wall time portion of the timestamp, since it's fine
// for the max timestamp to be a bit too large.
func (c *fixedCache) ratchetMaxTimestamp(ts hlc.Timestamp) {
	new := ts.WallTime
	if ts.Logical > 0 {
		new++
	}

	for {
		old := atomic.LoadInt64(&c.maxWallTime)
		if new <= old {
			break
		}

		if atomic.CompareAndSwapInt64(&c.maxWallTime, old, new) {
			break
		}
	}
}

// RatchetTimestampSet will update the current node's key and gap timestamps to
// the maximum of their current values or the given values. If clearInit is true,
// then the initializing bit will be cleared, indicating that the node is now
// fully initialized and its timestamps can now be relied upon.
func (c *fixedCache) ratchetTimestampSet(it *arenaskl.Iterator, keyVal, gapVal TimestampValue, clearInit bool) {
	var arr [encodedValSize * 2]byte

	for {
		meta := it.Meta()
		oldKeyVal, oldGapVal := c.decodeTimestampSet(it.Value(), meta)

		keyUpdateVal, keyUpdateTs := false, false
		keyVal, keyUpdateVal, keyUpdateTs = ratchetTimestampValue(oldKeyVal, keyVal)

		gapUpdateVal, gapUpdateTs := false, false
		gapVal, gapUpdateVal, gapUpdateTs = ratchetTimestampValue(oldGapVal, gapVal)

		updateVal := keyUpdateVal || gapUpdateVal
		updateTs := keyUpdateTs || gapUpdateTs

		var initMeta uint16
		if clearInit {
			initMeta = 0
		} else {
			initMeta = meta & initializing
		}

		// Check whether it's necessary to make an update.
		var err error
		if !updateVal {
			if !clearInit || (meta&initializing) == 0 {
				// No update necessary because the init bit doesn't need to be
				// cleared or it's already cleared.
				return
			}

			// Clear the initializing bit, but no need to update the timestamps.
			err = it.SetMeta(meta & ^uint16(initializing))
		} else {
			// Ratchet the max timestamp.
			if updateTs {
				keyTs, gapTs := keyVal.ts, gapVal.ts
				if gapTs.Less(keyTs) {
					c.ratchetMaxTimestamp(keyTs)
				} else {
					c.ratchetMaxTimestamp(gapTs)
				}
			}

			// Update the timestamps, possibly preserving the init bit.
			b, newMeta := c.encodeTimestampSet(arr[:0], keyVal, gapVal)
			err = it.Set(b, newMeta|initMeta)
		}

		switch err {
		case nil:
			return

		case arenaskl.ErrArenaFull:
			atomic.StoreInt32(&c.isFull, 1)

			// Arena full, so ratchet the timestamps to the max timestamp.
			err = it.SetMeta(uint16(useMaxTs) | initMeta)
			if err == arenaskl.ErrRecordUpdated {
				continue
			}

			return

		case arenaskl.ErrRecordUpdated:
			// Record was updated by another thread, so restart ratchet attempt.
			continue

		default:
			panic(fmt.Sprintf("unexpected error: %v", err))
		}
	}
}

// ratchetTimestampValue returns the TimestampValue that results from ratchetting
// the provided old and new TimestampValues. It also returns flags describing
// whether the value was updated and whether the timestamp within the value
// was updated.
func ratchetTimestampValue(old, new TimestampValue) (res TimestampValue, updateVal, updateTs bool) {
	if old.ts.Less(new.ts) {
		return new, true, true
	} else if new.ts.Less(old.ts) {
		return old, false, false
	} else if new.txnID != old.txnID && old.txnID != noTxnID {
		// The two values have different transaction IDs but the same timestamp.
		// We need to remove the transaction ID from the value.
		new.txnID = noTxnID
		return new, true, false
	}
	return new, false, false
}

// ScanForTimestamp scans backwards for the first initialized node and uses its
// gap timestamp as the initial candidate. It then scans forwards until it
// reaches the termination key, ratcheting any uninitialized nodes it encounters,
// and updating the candidate gap timestamp as it goes. The timestamp of the
// termination key is returned.
//
// Iterating backwards and then forwards solves potential race conditions with
// other threads. During iteration backwards, other nodes can be inserting new
// nodes between the previous node and the lookup node, which could change the
// correct value of the gap timestamp. The solution is two-fold:
//
// 1. Add new nodes in two phases - initializing and then initialized. Nodes in
//    the initializing state act as a synchronization point between goroutines
//    that are adding a particular node and goroutines that are scanning for gap
//    timestamps. Scanning goroutines encounter the initializing nodes and are
//    forced to deal with them before continuing.
//
// 2. After the gap timestamp of the previous node has been found, the scanning
//    goroutine will scan forwards until it reaches the original key. It will
//    complete initialization of any nodes along the way and inherit the gap
//    timestamp of initialized nodes as it goes. By the time it reaches the
//    original key, it has a valid gap timestamp value.
//
// During forward iteration, if another goroutine inserts a new gap node in the
// interval between the previous node and the original key, then either:
//
// 1. The forward iteration finds it and looks up its gap timestamp. That node
//    now becomes the new "previous node", and iteration continues.
//
// 2. The new node is created after the iterator has move past its position. As
//    part of node creation, the creator had to scan backwards to find the gap
//    timestamp of the previous node. It is guaranteed to find a gap timestamp
//    that is >= the gap timestamp found by the original goroutine.
//
// This means that no matter what gets inserted, or when it gets inserted, the
// scanning goroutine is guaranteed to end up with a timestamp value that will
// never decrease on future lookups, which is the critical invariant.
func (c *fixedCache) scanForTimestamp(it *arenaskl.Iterator, from, to []byte, opt rangeOptions, onKey bool) TimestampValue {
	clone := *it

	if onKey {
		// The iterator is currently positioned on the key node, so need to
		// iterate backwards from there in order to find the gap timestamp.
		clone.Prev()
	}

	// First iterate backwards, looking for an already initialized node which
	// will supply the initial candidate gap timestamp.
	var gapVal TimestampValue
	for {
		if !clone.Valid() {
			// No more previous nodes, so use the zero timestamp and begin
			// forward iteration from the first node.
			clone.SeekToFirst()
			break
		}

		meta := clone.Meta()
		if (meta & initializing) == 0 {
			// Found the gap timestamp for an initialized node.
			_, gapVal = c.decodeTimestampSet(clone.Value(), meta)
			clone.Next()
			break
		}

		clone.Prev()
	}

	// Now iterate forwards until "key" is reached, update any uninitialized
	// nodes along the way, and update the gap timestamp.
	var maxVal TimestampValue
	for {
		if !clone.Valid() {
			maxVal, _, _ = ratchetTimestampValue(maxVal, gapVal)
			return maxVal
		}

		if (clone.Meta() & initializing) != 0 {
			// Finish initializing the node with the gap timestamp.
			c.ratchetTimestampSet(&clone, gapVal, gapVal, true)
		}

		toCmp := -1
		if to != nil {
			toCmp = bytes.Compare(clone.Key(), to)
		}

		if toCmp > 0 || (toCmp == 0 && (opt&ExcludeTo) != 0) {
			// Past the lookup key, so use the gap timestamp.
			maxVal, _, _ = ratchetTimestampValue(maxVal, gapVal)
			return maxVal
		}

		fromCmp := bytes.Compare(clone.Key(), from)
		gapStartAllow := fromCmp > 0
		gapEndAllow := toCmp < 0
		if gapStartAllow && gapEndAllow {
			maxVal, _, _ = ratchetTimestampValue(maxVal, gapVal)
		}

		var keyVal TimestampValue
		keyVal, gapVal = c.decodeTimestampSet(clone.Value(), clone.Meta())

		keyStartAllow := fromCmp > 0 || fromCmp == 0 && (opt&ExcludeFrom) == 0
		keyEndAllow := toCmp <= 0 // && (opt&ExcludeTo) == 0

		if keyStartAllow && keyEndAllow {
			maxVal, _, _ = ratchetTimestampValue(maxVal, keyVal)
		}

		if toCmp == 0 {
			// On the lookup key, so use the key timestamp.
			return maxVal
		}

		// Haven't yet reached the lookup key, so keep iterating.
		clone.Next()
	}
}

func (c *fixedCache) decodeTimestampSet(b []byte, meta uint16) (keyVal, gapVal TimestampValue) {
	if (meta & useMaxTs) != 0 {
		ts := hlc.Timestamp{WallTime: atomic.LoadInt64(&c.maxWallTime)}
		maxVal := TimestampValue{ts: ts, txnID: noTxnID}
		return maxVal, maxVal
	}

	if (meta & hasKey) != 0 {
		b, keyVal = decodeTimestamp(b)
	}

	if (meta & hasGap) != 0 {
		b, gapVal = decodeTimestamp(b)
	}

	return
}

func (c *fixedCache) encodeTimestampSet(b []byte, keyVal, gapVal TimestampValue) (ret []byte, meta uint16) {
	if keyTs := keyVal.ts; keyTs.WallTime != 0 || keyTs.Logical != 0 {
		b = encodeTimestamp(b, keyVal)
		meta |= hasKey
	}

	if gapTs := gapVal.ts; gapTs.WallTime != 0 || gapTs.Logical != 0 {
		b = encodeTimestamp(b, gapVal)
		meta |= hasGap
	}

	ret = b
	return
}

func decodeTimestamp(b []byte) (ret []byte, val TimestampValue) {
	wallTime := binary.BigEndian.Uint64(b)
	logical := binary.BigEndian.Uint32(b[8:])
	val.ts = hlc.Timestamp{WallTime: int64(wallTime), Logical: int32(logical)}
	if err := val.txnID.Unmarshal(b[encodedTsSize:encodedValSize]); err != nil {
		panic(err)
	}
	ret = b[encodedValSize:]
	return
}

func encodeTimestamp(b []byte, val TimestampValue) []byte {
	l := len(b)
	b = b[:l+encodedValSize]
	binary.BigEndian.PutUint64(b[l:], uint64(val.ts.WallTime))
	binary.BigEndian.PutUint32(b[l+8:], uint32(val.ts.Logical))
	if _, err := val.txnID.MarshalTo(b[l+encodedTsSize:]); err != nil {
		panic(err)
	}
	return b
}
