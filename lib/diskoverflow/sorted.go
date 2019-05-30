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

const suffixLength = 8

// Sorted is a disk-spilling container, that sorts added values by the
// accompanying keys.
// You must not add new values after calling PopFirst/-Last.
type Sorted interface {
	Common
	Add(k, v []byte)
	PopFirst() ([]byte, bool)
	PopLast() ([]byte, bool)
}

type sorted struct {
	Map
}

// NewSorted returns an implementation of Sorted, spilling to disk at location.
func NewSorted(location string, overflowBytes int) Sorted {
	return &sorted{Map: NewMap(location, overflowBytes)}
}

func (o *sorted) String() string {
	return fmt.Sprintf("Sorted@%p", o)
}

func (o *sorted) Add(k, v []byte) {
	suffix := make([]byte, suffixLength)
	binary.BigEndian.PutUint64(suffix, uint64(o.Items()))
	o.Set(append(k, suffix...), v)
}

func (o *sorted) PopFirst() ([]byte, bool) {
	return o.pop(true)
}

func (o *sorted) PopLast() ([]byte, bool) {
	return o.pop(false)
}

func (o *sorted) pop(first bool) ([]byte, bool) {
	it := o.NewIterator()
	defer it.Release()
	var ok bool
	if first {
		ok = it.First()
	} else {
		ok = it.Last()
	}
	if ok {
		o.Delete(it.Key())
	}
	return it.Value(), ok
}
