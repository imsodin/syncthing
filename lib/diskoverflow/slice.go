// Copyright (C) 2018 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package diskoverflow

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type Slice interface {
	Append(v []byte)
	NewIterator() iterator.Iterator
	Items() int
	Close()
}

type slice struct {
	Sorted
}

// NewSlice creates a slice like container, spilling to disk at location.
// All items added to this instance must be of the same type as v.
func NewSlice(location string, overflowBytes int) Slice {
	return &slice{Sorted: NewSorted(location, overflowBytes)}
}

func (o *slice) String() string {
	return fmt.Sprintf("Slice@%p", o)
}

func (o *slice) Append(v []byte) {
	o.Add(nil, v)
}
