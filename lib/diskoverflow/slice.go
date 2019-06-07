// Copyright (C) 2018 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package diskoverflow

import (
	"encoding/binary"
	"fmt"
)

type Slice interface {
	Append(v Value)
	NewIterator() Iterator
	NewReverseIterator() Iterator
	Bytes() int
	Items() int
	SetOverflowBytes(bytes int)
	Close()
}

type slice struct {
	commonSlice
	base
}

type commonSlice interface {
	common
	append(v Value)
	newIterator(p iteratorParent, reverse bool) Iterator
}

// NewSorted creates a slice like container, spilling to disk at location.
// All items added to this instance must be of the same type as v.
func NewSlice(location string) Slice {
	o := &slice{base: newBase(location)}
	o.commonSlice = &memorySlice{}
	return o
}

func (o *slice) Append(v Value) {
	if o.iterating {
		panic(concurrencyMsg)
	}
	if o.startSpilling(o.Bytes() + v.ProtoSize()) {
		d, err := v.Marshal()
		errPanic(err)
		ds := &diskSlice{newDiskMap(o.location)}
		it := o.NewIterator()
		for it.Next() {
			v.Reset()
			it.Value(v)
			ds.append(v)
		}
		it.Release()
		o.commonSlice.Close()
		o.commonSlice = ds
		o.spilling = true
		v.Reset()
		errPanic(v.Unmarshal(d))
	}
	o.append(v)
}

func (o *slice) released() {
	o.iterating = false
}

func (o *slice) NewIterator() Iterator {
	return o.newIterator(false)
}

func (o *slice) NewReverseIterator() Iterator {
	return o.newIterator(true)
}

func (o *slice) newIterator(reverse bool) Iterator {
	if o.iterating {
		panic(concurrencyMsg)
	}
	o.iterating = true
	return o.commonSlice.newIterator(o, reverse)
}

// Close is just here to catch deferred calls to Close, such that the correct
// method is called in case spilling happened.
func (o *slice) Close() {
	o.commonSlice.Close()
}

func (o *slice) String() string {
	return fmt.Sprintf("Slice@%p", o)
}

type memorySlice struct {
	values []Value
	bytes  int
}

func (o *memorySlice) append(v Value) {
	o.values = append(o.values, v)
	o.bytes += v.ProtoSize()
}

func (o *memorySlice) Bytes() int {
	return o.bytes
}

func (o *memorySlice) Close() {
	o.values = nil
}

type sliceIterator struct {
	*posIterator
	values []Value
}

func (o *memorySlice) newIterator(p iteratorParent, reverse bool) Iterator {
	return &sliceIterator{
		posIterator: newPosIterator(p, len(o.values), reverse),
		values:      o.values,
	}
}

func (si *sliceIterator) Value(v Value) {
	if si.offset < si.len {
		copyValue(v, si.values[si.pos()])
	}
}

func (o *memorySlice) Items() int {
	return len(o.values)
}

type diskSlice struct {
	*diskMap
}

const indexLength = 8

func (o *diskSlice) append(v Value) {
	index := make([]byte, indexLength)
	binary.BigEndian.PutUint64(index, uint64(o.Items()))
	o.diskMap.set(index, v)
}

func (o *diskSlice) newIterator(p iteratorParent, reverse bool) Iterator {
	return o.diskMap.newIterator(p, reverse)
}
