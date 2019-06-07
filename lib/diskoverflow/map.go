// Copyright (C) 2018 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package diskoverflow

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type SortedMap interface {
	Set(k []byte, v Value)
	Get(k []byte, v Value) bool
	Pop(k []byte, v Value) bool
	PopFirst(v Value) bool
	PopLast(v Value) bool
	Delete(k []byte)
	NewIterator() MapIterator
	NewReverseIterator() MapIterator
	Bytes() int
	Items() int
	SetOverflowBytes(bytes int)
	Close()
}

type sortedMap struct {
	base
	commonMap
}

type commonMap interface {
	common
	set(k []byte, v Value)
	Get(k []byte, v Value) bool
	Pop(k []byte, v Value) bool
	PopFirst(v Value) bool
	PopLast(v Value) bool
	Delete(k []byte)
	newIterator(reverse bool) MapIterator
}

// NewSortedMap returns an implementation of Map, spilling to disk at location.
func NewSortedMap(location string) SortedMap {
	o := &sortedMap{base: newBase(location)}
	o.commonMap = &memMap{
		values: make(map[string]Value),
	}
	return o
}

func (o *sortedMap) Set(k []byte, v Value) {
	if o.startSpilling(o.Bytes() + v.ProtoSize()) {
		d, err := v.Marshal()
		errPanic(err)
		newMap := newDiskMap(o.location)
		it := o.newIterator(false)
		for it.Next() {
			v.Reset()
			it.Value(v)
			newMap.set(it.Key(), v)
		}
		it.Release()
		o.commonMap.Close()
		o.commonMap = newMap
		o.spilling = true
		v.Reset()
		errPanic(v.Unmarshal(d))
	}
	o.set(k, v)
}

func (o *sortedMap) String() string {
	return fmt.Sprintf("SortedMap@%p", o)
}

// Close is just here to catch deferred calls to Close, such that the correct
// method is called in case spilling happened.
func (o *sortedMap) Close() {
	o.commonMap.Close()
}

type MapIterator interface {
	Iterator
	Key() []byte
}

func (o *sortedMap) NewIterator() MapIterator {
	return o.newIterator(false)
}

func (o *sortedMap) NewReverseIterator() MapIterator {
	return o.newIterator(true)
}

type memMap struct {
	needsSorting bool
	lastIndex    int
	keys         []string
	values       map[string]Value
	bytes        int
}

func (o *memMap) set(k []byte, v Value) {
	s := string(k)
	if ov, ok := o.values[s]; ok {
		o.bytes -= ov.ProtoSize()
	} else {
		o.needsSorting = true
		o.keys = append(o.keys, s)
	}
	o.values[s] = v
	o.bytes += v.ProtoSize()
}

func (o *memMap) Bytes() int {
	return o.bytes
}

func (o *memMap) Close() {
	o.values = nil
}

func (o *memMap) Get(k []byte, v Value) bool {
	nv, ok := o.values[string(k)]
	if !ok {
		return false
	}
	copyValue(v, nv)
	return true
}

func (o *memMap) Items() int {
	return len(o.values)
}

func (o *memMap) Pop(k []byte, v Value) bool {
	ok := o.Get(k, v)
	if !ok {
		return false
	}
	delete(o.values, string(k))
	o.bytes -= v.ProtoSize()
	return true
}

func (o *memMap) PopFirst(v Value) bool {
	if o.Items() == 0 {
		return false
	}
	if o.needsSorting {
		sort.Strings(o.keys)
		o.needsSorting = false
	}
	for _, ok := o.values[o.keys[0]]; !ok; _, ok = o.values[o.keys[0]] {
		o.keys = o.keys[1:]
	}
	return o.Pop([]byte(o.keys[0]), v)
}

func (o *memMap) PopLast(v Value) bool {
	if o.Items() == 0 {
		return false
	}
	if o.needsSorting {
		sort.Strings(o.keys)
		o.needsSorting = false
	}
	for _, ok := o.values[o.keys[len(o.keys)-1]]; !ok; _, ok = o.values[o.keys[len(o.keys)-1]] {
		o.keys = o.keys[:len(o.keys)-1]
	}
	return o.Pop([]byte(o.keys[len(o.keys)-1]), v)
}

func (o *memMap) Delete(k []byte) {
	s := string(k)
	v, ok := o.values[s]
	if !ok {
		return
	}
	delete(o.values, s)
	o.bytes -= v.ProtoSize()
}

type memMapIterator struct {
	*posIterator
	keys   []string
	values map[string]Value
}

func (o *memMap) newIterator(reverse bool) MapIterator {
	if o.needsSorting {
		sort.Strings(o.keys)
		o.needsSorting = false
	}
	return &memMapIterator{
		posIterator: newPosIterator(len(o.keys), reverse),
		keys:        o.keys,
		values:      o.values,
	}
}

func (si *memMapIterator) Next() bool {
	if !si.posIterator.Next() {
		return false
	}
	// If items were removed from the map, their keys remained.
	for _, ok := si.values[si.keys[si.pos()]]; !ok; _, ok = si.values[si.keys[si.pos()]] {
		if si.offset == si.len-1 {
			return false
		}
		si.keys = append(si.keys[:si.pos()], si.keys[si.pos()+1:]...)
		si.len--
	}
	return true
}

func (si *memMapIterator) Value(v Value) {
	if si.offset >= 0 && si.offset < si.len {
		copyValue(v, si.values[si.keys[si.pos()]])
	}
}

func (si *memMapIterator) Key() []byte {
	return []byte(si.keys[si.pos()])
}

type diskMap struct {
	db    *leveldb.DB
	bytes int
	dir   string
	len   int
}

func newDiskMap(location string) *diskMap {
	// Use a temporary database directory.
	tmp, err := ioutil.TempDir(location, "overflow-")
	if err != nil {
		panic("creating temporary directory: " + err.Error())
	}
	db, err := leveldb.OpenFile(tmp, &opt.Options{
		OpenFilesCacheCapacity: 10,
		WriteBuffer:            512 << 10,
	})
	if err != nil {
		panic("creating temporary database: " + err.Error())
	}
	return &diskMap{
		db:  db,
		dir: tmp,
	}
}

func (o *diskMap) set(k []byte, v Value) {
	old, oldErr := o.db.Get([]byte(k), nil)
	d, err := v.Marshal()
	errPanic(err)
	errPanic(o.db.Put(k, d, nil))
	o.len++
	o.bytes += v.ProtoSize()
	if oldErr == nil {
		errPanic(v.Unmarshal(old))
		o.bytes -= v.ProtoSize()
	}
}

func (o *diskMap) Close() {
	o.db.Close()
	os.RemoveAll(o.dir)
}

func (o *diskMap) Bytes() int {
	return o.bytes
}

func (o *diskMap) Get(k []byte, v Value) bool {
	d, err := o.db.Get([]byte(k), nil)
	if err != nil {
		return false
	}
	errPanic(v.Unmarshal(d))
	return true
}

func (o *diskMap) Items() int {
	return o.len
}

func (o *diskMap) Pop(k []byte, v Value) bool {
	ok := o.Get(k, v)
	if ok {
		errPanic(o.db.Delete([]byte(k), nil))
		o.len--
	}
	return ok
}

func (o *diskMap) PopFirst(v Value) bool {
	return o.pop(v, true)
}

func (o *diskMap) PopLast(v Value) bool {
	return o.pop(v, false)
}

func (o *diskMap) pop(v Value, first bool) bool {
	it := o.db.NewIterator(nil, nil)
	defer it.Release()
	var ok bool
	if first {
		ok = it.First()
	} else {
		ok = it.Last()
	}
	if !ok {
		return false
	}
	errPanic(v.Unmarshal(it.Value()))
	errPanic(o.db.Delete(it.Key(), nil))
	o.bytes -= v.ProtoSize()
	o.len--
	return true
}

func (o *diskMap) Delete(k []byte) {
	errPanic(o.db.Delete([]byte(k), nil))
	o.len--
}

func (o *diskMap) newIterator(reverse bool) MapIterator {
	di := &diskIterator{o.db.NewIterator(nil, nil)}
	if !reverse {
		return di
	}
	ri := &diskReverseIterator{diskIterator: di}
	ri.next = func(i *diskReverseIterator) bool {
		i.next = func(j *diskReverseIterator) bool {
			return j.Prev()
		}
		return i.Last()
	}
	return ri
}

type diskIterator struct {
	iterator.Iterator
}

func (i *diskIterator) Value(v Value) {
	errPanic(v.Unmarshal(i.Iterator.Value()))
}

type diskReverseIterator struct {
	*diskIterator
	next func(*diskReverseIterator) bool
}

func (i *diskReverseIterator) Next() bool {
	return i.next(i)
}
