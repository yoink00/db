package db

import (
	"bytes"
	"errors"

	"github.com/petar/GoLLRB/llrb"
)

var indices map[string]*llrb.LLRB

var ErrNotFound = errors.New("Key not found")
var ErrInvalidIndex = errors.New("Invalid index")
var ErrIndexNotMatch = errors.New("Indices do not match")

type Key struct {
	Index string
	Key []byte
}

type KeyValuePair struct {
	item llrb.Item

	Key   []byte
	Value []byte
}

func (lhs *KeyValuePair) Less(rhs llrb.Item) bool {
	t := rhs.(*KeyValuePair)
	return bytes.Compare(lhs.Key, t.Key) == -1
}

func Put(keys []*Key, item []byte) {
	if indices == nil {
		indices = make(map[string]*llrb.LLRB, 10)
	}

	for _, k := range keys {
		if _, ok := indices[k.Index]; !ok {
			indices[k.Index] = llrb.New()
		}

		i := new(KeyValuePair)
		i.Key = make([]byte, len(k.Key), len(k.Key))
		copy(i.Key, k.Key)
		i.Value = make([]byte, len(item), len(item))
		copy(i.Value, item)

		indices[k.Index].ReplaceOrInsert(i)
	}
}

func Get(key *Key) ([]byte, error) {
	if indices == nil {
		return nil, ErrInvalidIndex
	}

	if _, ok := indices[key.Index]; !ok {
		return nil, ErrInvalidIndex
	}

	i := new(KeyValuePair)
	i.Key = key.Key
	v := indices[key.Index].Get(i)

	if v == nil {
		return nil, ErrNotFound
	}

	return v.(*KeyValuePair).Value, nil
}

func GetRange(start *Key, end *Key, values chan<- *KeyValuePair) error {
	if start.Index != end.Index {
		close(values)
		return ErrIndexNotMatch
	}

	if _, ok := indices[start.Index]; !ok {
		close(values)
		return ErrInvalidIndex
	}

	s := new(KeyValuePair)
	e := new(KeyValuePair)
	s.Key = start.Key
	e.Key = end.Key

	go func() {
		defer close(values)

		indices[start.Index].AscendRange(s, e, func(i llrb.Item) bool {
		    bi := i.(*KeyValuePair)
		    values <- bi

		    return true
		})
	}()

	return nil
}

func Count(index string) (int, error) {
	if i, ok := indices[index]; ok {
		return i.Len(), nil
	}

	return 0, ErrInvalidIndex
}
