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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	arenaSize = 64 * 1024 * 1024
)

var (
	emptyTsVal = TimestampValue{}
	floorTs    = hlc.Timestamp{100, 0}
	floorTsVal = makeTsValNoTxnID(floorTs)
)

func makeTsValNoTxnID(ts hlc.Timestamp) TimestampValue {
	return TimestampValue{ts: ts, txnID: noTxnID}
}

func makeTsVal(ts hlc.Timestamp, txnIDStr string) TimestampValue {
	txnIDBytes := []byte(txnIDStr)
	if len(txnIDBytes) < 16 {
		oldTxnIDBytes := txnIDBytes
		txnIDBytes = make([]byte, 16)
		copy(txnIDBytes[16-len(oldTxnIDBytes):], oldTxnIDBytes)
	}
	txnID, err := uuid.FromBytes(txnIDBytes)
	if err != nil {
		panic(err)
	}
	return TimestampValue{ts: ts, txnID: txnID}
}

func TestCacheAdd(t *testing.T) {
	tsVal := makeTsVal(hlc.Timestamp{100, 100}, "1")
	tsVal2 := makeTsVal(hlc.Timestamp{200, 201}, "2")

	c := New(arenaSize)
	c.Add([]byte("apricot"), tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("banana")))

	c.Add([]byte("banana"), tsVal2)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("cherry")))
}

func TestCacheSingleRange(t *testing.T) {
	tsVal := makeTsVal(hlc.Timestamp{100, 100}, "1")
	tsVal2 := makeTsVal(hlc.Timestamp{200, 50}, "2")

	c := New(arenaSize)

	c.AddRange([]byte("apricot"), []byte("orange"), 0, tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("raspberry")))

	// Try again and make sure it's a no-op.
	c.AddRange([]byte("apricot"), []byte("orange"), 0, tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("raspberry")))

	// Ratchet up the timestamps.
	c.AddRange([]byte("apricot"), []byte("orange"), 0, tsVal2)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("raspberry")))

	// Add disjoint range.
	c.AddRange([]byte("pear"), []byte("tomato"), ExcludeFrom|ExcludeTo, tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("watermelon")))

	// Try again and make sure it's a no-op.
	c.AddRange([]byte("pear"), []byte("tomato"), ExcludeFrom|ExcludeTo, tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the timestamps.
	c.AddRange([]byte("pear"), []byte("tomato"), ExcludeFrom|ExcludeTo, tsVal2)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("peach")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("watermelon")))
}

func TestCacheOpenRanges(t *testing.T) {
	tsVal := makeTsVal(hlc.Timestamp{200, 200}, "1")
	tsVal2 := makeTsVal(hlc.Timestamp{200, 201}, "2")

	c := New(arenaSize)
	c.floorTs = floorTs

	c.AddRange([]byte("banana"), nil, ExcludeFrom, tsVal)
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("orange")))

	c.AddRange([]byte(""), []byte("kiwi"), 0, tsVal2)
	require.Equal(t, tsVal2, c.LookupTimestamp(nil))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("orange")))
}

func TestCacheSupersetRange(t *testing.T) {
	tsVal := makeTsVal(hlc.Timestamp{200, 1}, "1")
	tsVal2 := makeTsVal(hlc.Timestamp{201, 0}, "2")
	tsVal3 := makeTsVal(hlc.Timestamp{300, 0}, "3")

	c := New(arenaSize)
	c.floorTs = floorTs

	// Same range.
	c.AddRange([]byte("kiwi"), []byte("orange"), 0, tsVal)
	c.AddRange([]byte("kiwi"), []byte("orange"), 0, tsVal2)
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("mango")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("raspberry")))

	// Superset range, but with lower timestamp.
	c.AddRange([]byte("grape"), []byte("pear"), 0, tsVal)
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("watermelon")))

	// Superset range, but with higher timestamp.
	c.AddRange([]byte("banana"), []byte("raspberry"), 0, tsVal3)
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("watermelon")))
}

func TestCacheContiguousRanges(t *testing.T) {
	tsVal := makeTsVal(hlc.Timestamp{200, 1}, "1")
	tsVal2 := makeTsVal(hlc.Timestamp{201, 0}, "2")

	c := New(arenaSize)
	c.floorTs = floorTs

	c.AddRange([]byte("banana"), []byte("kiwi"), ExcludeTo, tsVal)
	c.AddRange([]byte("kiwi"), []byte("orange"), ExcludeTo, tsVal2)
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("orange")))
}

func TestCacheOverlappingRanges(t *testing.T) {
	tsVal := makeTsVal(hlc.Timestamp{200, 1}, "1")
	tsVal2 := makeTsVal(hlc.Timestamp{201, 0}, "2")
	tsVal3 := makeTsVal(hlc.Timestamp{300, 0}, "3")
	tsVal4 := makeTsVal(hlc.Timestamp{400, 0}, "4")

	c := New(arenaSize)
	c.floorTs = floorTs

	c.AddRange([]byte("banana"), []byte("kiwi"), 0, tsVal)
	c.AddRange([]byte("grape"), []byte("raspberry"), ExcludeTo, tsVal2)
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("raspberry")))

	c.AddRange([]byte("apricot"), []byte("orange"), 0, tsVal3)
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("raspberry")))

	c.AddRange([]byte("kiwi"), []byte(nil), ExcludeFrom, tsVal4)
	require.Equal(t, floorTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("grape")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, tsVal4, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, tsVal4, c.LookupTimestamp([]byte("pear")))
	require.Equal(t, tsVal4, c.LookupTimestamp([]byte("raspberry")))
}

func TestCacheBoundaryRange(t *testing.T) {
	tsVal := makeTsVal(hlc.Timestamp{100, 100}, "1")

	c := New(arenaSize)

	// Don't allow nil from key.
	require.Panics(t, func() { c.AddRange([]byte(nil), []byte(nil), ExcludeFrom, tsVal) })

	// If from key is greater than to key, then range is zero-length.
	c.AddRange([]byte("kiwi"), []byte("apple"), 0, tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("raspberry")))

	// If from key is same as to key, and both are excluded, then range is
	// zero-length.
	c.AddRange([]byte("banana"), []byte("banana"), ExcludeFrom|ExcludeTo, tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("kiwi")))

	// If from key is same as to key, then range has length one.
	c.AddRange([]byte("mango"), []byte("mango"), 0, tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("kiwi")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("mango")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("orange")))

	// If from key is same as to key, then range has length one.
	c.AddRange([]byte("banana"), []byte("banana"), ExcludeTo, tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("cherry")))
}

func TestCacheTxnIDs(t *testing.T) {
	tsVal := makeTsVal(hlc.Timestamp{100, 100}, "1")
	tsVal2 := makeTsVal(hlc.Timestamp{100, 100}, "2") // same ts as tsVal
	tsVal2NoTxnID := makeTsValNoTxnID(hlc.Timestamp{100, 100})
	tsVal3 := makeTsVal(hlc.Timestamp{250, 50}, "2") // same txn ID as tsVal2
	tsVal4 := makeTsVal(hlc.Timestamp{250, 50}, "3") // same ts as tsVal3
	tsVal4NoTxnID := makeTsValNoTxnID(hlc.Timestamp{250, 50})

	c := New(arenaSize)

	c.AddRange([]byte("apricot"), []byte("raspberry"), 0, tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("tomato")))

	// Ratchet up the txnID with the same timestamp. txnID should be removed.
	c.AddRange([]byte("apricot"), []byte("tomato"), 0, tsVal2)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal2NoTxnID, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal2NoTxnID, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal2NoTxnID, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the timestamp with the same txnID.
	c.AddRange([]byte("apricot"), []byte("orange"), 0, tsVal3)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, tsVal2NoTxnID, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("watermelon")))

	// Ratchet up the txnID with the same timestamp. txnID should be removed.
	c.AddRange([]byte("apricot"), []byte("banana"), 0, tsVal4)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal4NoTxnID, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal4NoTxnID, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("orange")))
	require.Equal(t, tsVal2NoTxnID, c.LookupTimestamp([]byte("raspberry")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("tomato")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("watermelon")))
}

func TestCacheLookupRange(t *testing.T) {
	tsVal := makeTsVal(hlc.Timestamp{100, 100}, "1")
	tsVal2 := makeTsVal(hlc.Timestamp{200, 201}, "2")
	tsVal3 := makeTsVal(hlc.Timestamp{200, 201}, "3") // same ts as tsVal2
	tsVal3NoTxnID := makeTsValNoTxnID(hlc.Timestamp{200, 201})
	tsVal4 := makeTsVal(hlc.Timestamp{300, 201}, "4")
	tsVal5 := makeTsVal(hlc.Timestamp{300, 201}, "5") // same ts as tsVal2
	tsVal5NoTxnID := makeTsValNoTxnID(hlc.Timestamp{300, 201})

	c := New(arenaSize)

	c.Add([]byte("apricot"), tsVal)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("banana")))

	c.Add([]byte("banana"), tsVal2)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("cherry")))

	c.Add([]byte("cherry"), tsVal3)
	require.Equal(t, emptyTsVal, c.LookupTimestamp([]byte("apple")))
	require.Equal(t, tsVal, c.LookupTimestamp([]byte("apricot")))
	require.Equal(t, tsVal2, c.LookupTimestamp([]byte("banana")))
	require.Equal(t, tsVal3, c.LookupTimestamp([]byte("cherry")))

	require.Equal(t, emptyTsVal, c.LookupTimestampRange([]byte("apple"), []byte("apple"), 0))
	require.Equal(t, tsVal, c.LookupTimestampRange([]byte("apple"), []byte("apricot"), 0))
	require.Equal(t, tsVal, c.LookupTimestampRange([]byte("apple"), []byte("apricot"), ExcludeFrom))
	require.Equal(t, emptyTsVal, c.LookupTimestampRange([]byte("apple"), []byte("apricot"), ExcludeTo))

	require.Equal(t, tsVal, c.LookupTimestampRange([]byte("apricot"), []byte("apricot"), 0))
	require.Equal(t, tsVal, c.LookupTimestampRange([]byte("apricot"), []byte("apricot"), ExcludeFrom))
	require.Equal(t, tsVal, c.LookupTimestampRange([]byte("apricot"), []byte("apricot"), ExcludeTo))
	require.Equal(t, emptyTsVal, c.LookupTimestampRange([]byte("apricot"), []byte("apricot"), (ExcludeFrom|ExcludeTo)))

	require.Equal(t, tsVal2, c.LookupTimestampRange([]byte("apricot"), []byte("banana"), 0))
	require.Equal(t, tsVal2, c.LookupTimestampRange([]byte("apricot"), []byte("banana"), ExcludeFrom))
	require.Equal(t, tsVal, c.LookupTimestampRange([]byte("apricot"), []byte("banana"), ExcludeTo))
	require.Equal(t, emptyTsVal, c.LookupTimestampRange([]byte("apricot"), []byte("banana"), (ExcludeFrom|ExcludeTo)))

	require.Equal(t, tsVal3NoTxnID, c.LookupTimestampRange([]byte("banana"), []byte("cherry"), 0))
	require.Equal(t, tsVal3, c.LookupTimestampRange([]byte("banana"), []byte("cherry"), ExcludeFrom))
	require.Equal(t, tsVal2, c.LookupTimestampRange([]byte("banana"), []byte("cherry"), ExcludeTo))
	require.Equal(t, emptyTsVal, c.LookupTimestampRange([]byte("banana"), []byte("cherry"), (ExcludeFrom|ExcludeTo)))

	require.Equal(t, emptyTsVal, c.LookupTimestampRange([]byte("tomato"), []byte(nil), 0))
	require.Equal(t, tsVal3, c.LookupTimestampRange([]byte("cherry"), []byte(nil), 0))
	require.Equal(t, tsVal3NoTxnID, c.LookupTimestampRange([]byte("banana"), []byte(nil), 0))
	require.Equal(t, tsVal3NoTxnID, c.LookupTimestampRange([]byte("apricot"), []byte(nil), 0))

	c.AddRange([]byte("apple"), []byte("kiwi"), ExcludeTo, tsVal4)
	c.AddRange([]byte("kiwi"), []byte("tomato"), ExcludeTo, tsVal5)
	require.Equal(t, tsVal4, c.LookupTimestampRange([]byte("apple"), []byte("kiwi"), ExcludeTo))
	require.Equal(t, tsVal5, c.LookupTimestampRange([]byte("kiwi"), []byte("tomato"), ExcludeTo))
	require.Equal(t, tsVal5NoTxnID, c.LookupTimestampRange([]byte("apple"), []byte("tomato"), ExcludeTo))
}

func TestCacheFillCache(t *testing.T) {
	const n = 200
	const txnID = "12"

	// Use constant seed so that skiplist towers will be of predictable size.
	rand.Seed(0)

	c := New(3000)

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		tsVal := makeTsVal(hlc.Timestamp{WallTime: int64(100 + i), Logical: int32(i)}, txnID)
		c.AddRange(key, key, 0, tsVal)
	}

	floorTs := c.floorTs
	require.True(t, hlc.Timestamp{WallTime: 100}.Less(floorTs))

	lastKey := []byte(fmt.Sprintf("%05d", n-1))
	expTsVal := makeTsVal(hlc.Timestamp{WallTime: int64(100 + n - 1), Logical: int32(n - 1)}, txnID)
	require.Equal(t, expTsVal, c.LookupTimestamp(lastKey))

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		require.False(t, c.LookupTimestamp(key).ts.Less(floorTs))
	}
}

// Repeatedly fill cache and make sure timestamp lookups always increase.
func TestCacheFillCache2(t *testing.T) {
	const n = 10000
	const txnID = "12"

	c := New(997)
	key := []byte("some key")

	for i := 0; i < n; i++ {
		tsVal := makeTsVal(hlc.Timestamp{WallTime: int64(i), Logical: int32(i)}, txnID)
		c.Add(key, tsVal)
		require.True(t, !c.LookupTimestamp(key).ts.Less(tsVal.ts))
	}
}

// Test concurrency with a small cache size in order to force lots of cache
// rotations.
func TestCacheConcurrencyRotates(t *testing.T) {
	testConcurrency(t, 2048)
}

// Test concurrency with a larger cache size in order to test slot concurrency
// without the added complication of cache rotations.
func TestCacheConcurrencySlots(t *testing.T) {
	testConcurrency(t, arenaSize)
}

func BenchmarkAdd(b *testing.B) {
	const max = 500000000
	const txnID = "12"

	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond)
	cache := New(64 * 1024 * 1024)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	size := 1
	for i := 0; i < 9; i++ {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				rnd := int64(rng.Int31n(max))
				from := []byte(fmt.Sprintf("%020d", rnd))
				to := []byte(fmt.Sprintf("%020d", rnd+int64(size-1)))
				cache.AddRange(from, to, 0, makeTsVal(clock.Now(), txnID))
			}
		})

		size *= 10
	}
}

func BenchmarkReadstamp(b *testing.B) {
	const parallel = 1
	const max = 1000000000
	const data = 500000
	const txnID = "12"

	cache := New(arenaSize)
	clock := hlc.NewClock(hlc.UnixNano, time.Millisecond)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < data; i++ {
		from, to := makeRange(rng.Int31n(max))
		nowTsVal := makeTsVal(clock.Now(), txnID)
		cache.AddRange(from, to, ExcludeFrom|ExcludeTo, nowTsVal)
	}

	for i := 0; i <= 10; i++ {
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			var wg sync.WaitGroup

			for p := 0; p < parallel; p++ {
				wg.Add(1)

				go func(i int) {
					defer wg.Done()

					rng := rand.New(rand.NewSource(time.Now().UnixNano()))

					for n := 0; n < b.N/parallel; n++ {
						readFrac := rng.Int31()
						keyNum := rng.Int31n(max)

						if (readFrac % 10) < int32(i) {
							key := []byte(fmt.Sprintf("%020d", keyNum))
							cache.LookupTimestamp(key)
						} else {
							from, to := makeRange(keyNum)
							nowTsVal := makeTsVal(clock.Now(), txnID)
							cache.AddRange(from, to, ExcludeFrom|ExcludeTo, nowTsVal)
						}
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

func testConcurrency(t *testing.T, cacheSize uint32) {
	const n = 10000
	const slots = 20

	var wg sync.WaitGroup
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	cache := New(cacheSize)

	for i := 0; i < slots; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			key := []byte(fmt.Sprintf("%05d", i))
			maxTsVal := emptyTsVal

			for j := 0; j < n; j++ {
				fromNum := rng.Intn(slots)
				toNum := rng.Intn(slots)

				if fromNum > toNum {
					fromNum, toNum = toNum, fromNum
				} else if fromNum == toNum {
					toNum++
				}

				from := []byte(fmt.Sprintf("%05d", fromNum))
				to := []byte(fmt.Sprintf("%05d", toNum))

				now := clock.Now()
				nowTsVal := makeTsValNoTxnID(now)
				cache.AddRange(from, to, 0, nowTsVal)

				tsVal := cache.LookupTimestamp(from)
				require.False(t, tsVal.ts.Less(now))

				tsVal = cache.LookupTimestamp(to)
				require.False(t, tsVal.ts.Less(now))

				tsVal = cache.LookupTimestamp(key)
				require.False(t, tsVal.ts.Less(maxTsVal.ts))
				maxTsVal = tsVal
			}
		}(i)
	}

	wg.Wait()
}

func makeRange(start int32) (from, to []byte) {
	var end int32

	rem := start % 100
	if rem < 80 {
		end = start + 0
	} else if rem < 90 {
		end = start + 100
	} else if rem < 95 {
		end = start + 10000
	} else if rem < 99 {
		end = start + 1000000
	} else {
		end = start + 100000000
	}

	from = []byte(fmt.Sprintf("%020d", start))
	to = []byte(fmt.Sprintf("%020d", end))
	return
}
