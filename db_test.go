package db

import (
	"fmt"
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// From: https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func randString(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func TestPutGetCount(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	k := Key{"IDX_TEST", []byte("AAA")}

	// Get from empty db
	require.NotPanics(func() {
		_, err := Get(&k)
		require.Error(err)
	})

	// Put value
	v := []byte("This is a test value")
	Put([]*Key{&k}, v)

	// Get value and ensure same as put value
	nv, err := Get(&k)
	require.NoError(err)
	assert.Equal(v, nv)

	// Get unknown value
	k.Key = []byte("UNKNOWN")
	_, err = Get(&k)
	require.Error(err)
	assert.Equal(ErrNotFound, err)

	// Get value from unknown index
	k.Index = "IDX_UNKNOWN"
	_, err = Get(&k)
	require.Error(err)
	assert.Equal(ErrInvalidIndex, err)

	// Count from unknown index
	_, err = Count("IDX_UNKNOWN")
	require.Error(err)
	assert.Equal(ErrInvalidIndex, err)

	// Count from known index
	c, err := Count("IDX_TEST")
	require.NoError(err)
	assert.Equal(1, c)
}

func TestGetRange(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// Test error cases
	testChannel := make(chan *KeyValuePair, 1)
	err := GetRange(&Key{"IDX_NOT_EXIST", []byte(" ")}, &Key{"IDX_NOT_EXIST", []byte(" ")}, testChannel)
	require.Error(err)
	assert.Equal(ErrInvalidIndex, err)

	testChannel = make(chan *KeyValuePair, 1)
	err = GetRange(&Key{"IDX_NOT_EXIST", []byte(" ")}, &Key{"IDX_NOT_EXIST_2", []byte(" ")}, testChannel)
	require.Error(err)
	assert.Equal(ErrIndexNotMatch, err)

	// Number of items to insert and retrieve to test
	// range ordering.
	const numItems = 10000

	var firstKey []byte
	var lastKey []byte
	for i := 0; i < numItems; i++ {
		keyVal := []byte(randString(128))

		if firstKey == nil || bytes.Compare(keyVal, firstKey) == -1 {
			firstKey = keyVal
		}
		if lastKey == nil || bytes.Compare(lastKey, keyVal) == -1 {
			lastKey = keyVal
		}
		k := Key{"IDX_GET_RANGE_TEST", keyVal}
		Put([]*Key{&k}, []byte(randString(256)))
	}

	readChannel := make(chan *KeyValuePair, 50)
	lastKey[len(lastKey)-1]++
	err = GetRange(&Key{"IDX_GET_RANGE_TEST", firstKey}, &Key{"IDX_GET_RANGE_TEST", lastKey}, readChannel)
	require.NoError(err)

	var prev []byte
	count := 0
	for v := range readChannel {
		count++

		if prev == nil {
			prev = v.Key
			continue
		}

		assert.Condition(func() bool {
			return bytes.Compare(prev, v.Key) == -1
		})
	}
	require.Equal(numItems, count, "Records processed")
}

// Benchmarks

var bmkDBKeys [][]byte

func benchmarkDBPut(count int) {
	for i := 0; i < count; i++ {
		k := Key{"BMK_INDEX", []byte(randString(256))}
		v := []byte(randString(256))
		Put([]*Key{&k}, v)
		bmkDBKeys = append(bmkDBKeys, k.Key)
	}
}

func benchmarkDBGet(count int) {
	for i := 0; i < count; i++ {
		k := Key{"BMK_INDEX", bmkDBKeys[i]}
		_, err := Get(&k)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkDBPutThenGet(b *testing.B) {
	benchmarkDBPut(b.N)
	benchmarkDBGet(b.N)
}

var bmkMap = make(map[string][]byte, 1000)
var bmkMapKeys []string

func benchmarkMapPut(count int) {
	for i := 0; i < count; i++ {
		k := randString(256)
		v := []byte(randString(256))
		v2 := make([]byte, len(v), len(v))
		copy(v, v2)
		bmkMap[k] = v
		bmkMapKeys = append(bmkMapKeys, k)
	}
}

func benchmarkMapGet(count int) {
	for i := 0; i < count; i++ {
		_, ok := bmkMap[bmkMapKeys[i]]
		if !ok {
			panic("Key not found")
		}
	}
}

func BenchmarkMapPutThenGet(b *testing.B) {
	benchmarkMapPut(b.N)
	benchmarkMapGet(b.N)
}
